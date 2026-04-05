"""Claude API wrapper for LLM-powered radiology report formatting.

Takes raw Deepgram transcript + clinical metadata and returns a properly
structured, contextually corrected radiology report.  Follows the same
single-responsibility pattern as deepgram_client.py.
"""

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import anthropic

from crowdtrans.config_store import get_config_store

logger = logging.getLogger(__name__)

# Lazy singleton
_client: anthropic.Anthropic | None = None


@dataclass
class LLMFormatResult:
    formatted_text: str
    model: str
    input_tokens: int
    output_tokens: int
    duration_ms: int


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        store = get_config_store()
        api_key = store.get_global("anthropic_api_key")
        if not api_key:
            raise RuntimeError("Anthropic API key not configured")
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def reset_client():
    """Force re-creation of client (e.g. after API key change)."""
    global _client
    _client = None


# ---------------------------------------------------------------------------
# Doctor profile helpers
# ---------------------------------------------------------------------------

_DOCTOR_PROFILES: dict | None = None
_PROFILES_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "doctor_profiles.json"
_PROFILES_PATH_ALT = Path("/opt/crowdtrans/data/doctor_profiles.json")


def _load_profiles() -> dict:
    global _DOCTOR_PROFILES
    if _DOCTOR_PROFILES is not None:
        return _DOCTOR_PROFILES
    for path in (_PROFILES_PATH, _PROFILES_PATH_ALT):
        if path.exists():
            try:
                _DOCTOR_PROFILES = json.loads(path.read_text(encoding="utf-8"))
                return _DOCTOR_PROFILES
            except Exception:
                pass
    _DOCTOR_PROFILES = {}
    return _DOCTOR_PROFILES


def _get_doctor_context(doctor_id: str | None, modality_code: str | None) -> str:
    """Build doctor-specific prompt context from their profile."""
    if not doctor_id:
        return ""
    profiles = _load_profiles()
    profile = profiles.get(str(doctor_id))
    if not profile:
        return ""

    name = profile.get("doctor_name", "Unknown")
    mod_data = profile.get("modalities", {}).get(modality_code or "", {})
    if not mod_data or mod_data.get("count", 0) < 5:
        return ""

    parts = [f"\n## Doctor Profile: Dr {name}"]

    # Preferred section structure
    structures = mod_data.get("section_structure", {})
    if structures:
        best = max(structures, key=structures.get)
        parts.append(f"Preferred section structure for {modality_code}: {best}")

    # Heading renames
    for struct_key in structures:
        if "REPORT" in struct_key and "FINDINGS" not in struct_key:
            parts.append("This doctor uses 'REPORT' instead of 'FINDINGS' as a heading.")
            break

    # Section presence
    presence = mod_data.get("section_presence_pct", {})
    if presence:
        conclusion_pct = presence.get("CONCLUSION", 0)
        if conclusion_pct < 30:
            parts.append(f"This doctor rarely uses a CONCLUSION section ({conclusion_pct}% of reports).")
        elif conclusion_pct > 70:
            parts.append(f"This doctor almost always includes a CONCLUSION section ({conclusion_pct}% of reports).")

    # Word corrections (doctor-specific patterns)
    corrections = mod_data.get("word_corrections", [])
    if corrections:
        top = corrections[:10]
        correction_strs = [f'  "{c[0]}" -> "{c[1]}" ({c[2]}x)' for c in top if len(c) >= 3]
        if correction_strs:
            parts.append("Known word corrections for this doctor:")
            parts.extend(correction_strs)

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Modality section structure defaults
# ---------------------------------------------------------------------------

_MODALITY_STRUCTURES = {
    "CR": "CLINICAL HISTORY > FINDINGS (no CONCLUSION unless explicitly dictated)",
    "CT": "CLINICAL HISTORY > PROCEDURE > FINDINGS > CONCLUSION",
    "US": "CLINICAL HISTORY > FINDINGS > CONCLUSION",
    "MR": "CLINICAL HISTORY > PROCEDURE > FINDINGS > CONCLUSION",
    "MG": "CLINICAL HISTORY > FINDINGS > CONCLUSION",
    "NM": "CLINICAL HISTORY > PROCEDURE > FINDINGS > CONCLUSION",
    "BMD": "CLINICAL HISTORY > FINDINGS > CONCLUSION",
    "DSA": "CLINICAL HISTORY > PROCEDURE > FINDINGS > CONCLUSION",
}


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are a radiology report formatter for Australian medical dictation transcription.

Your task is to take a raw speech-to-text transcript of a radiologist's dictation and format it into a properly structured radiology report. The transcript comes from Deepgram Nova-3 Medical (Australian English).

## Rules

1. **Structure**: Organise the report into standard sections with UPPERCASE headings on their own line. The section structure depends on the imaging modality.

2. **Preserve clinical content exactly**: Do NOT add, remove, or change clinical findings, diagnoses, or measurements. Your job is formatting and correcting speech recognition errors only.

3. **Correct known speech recognition errors**: Deepgram commonly mishears:
   - "fusion" when the radiologist said "effusion" (joint/pleural effusion)
   - "retrotter" / "retrotiga" -> "rotator" (rotator cuff)
   - "bugling" -> "bulging" (disc bulging)
   - "angular" -> "annular" (annular tear/fissure in spine context)
   - "inclusion" -> "conclusion" (section heading)
   - "sun and nasal" -> "sinonasal"
   - "in plate" -> "endplate"
   - "near fusion" -> "knee effusion"
   - "generation" / "generations" -> "degeneration"
   - "fracturing" -> "fracture" (reports use noun form)
   - "cell stone" -> "Celestone" (steroid injection)
   - "sign of it" -> "synovitis"
   Use medical context to identify and correct similar mishears not listed above.

4. **Australian English spelling**: Use AU/UK spelling throughout:
   - oedema (not edema), haemorrhage (not hemorrhage), anaemia (not anemia)
   - ischaemia/ischaemic, oesophagus/oesophageal, foetus/foetal
   - tumour, fibre, grey, paediatric, orthopaedic, anaesthetic
   - -isation/-ised (not -ization/-ized): visualised, characterised, localised
   - osteopaenia (not osteopenia), haematoma (not hematoma)
   - lignocaine (not lidocaine — Australian drug name)
   - faecal (not fecal)

5. **Measurement formatting**: "5 millimeters" -> "5mm", "3 centimeters" -> "3cm", "5 by 3" -> "5 x 3"

6. **Remove dictation artifacts**: Strip filler words and dictation commands:
   - "sorry", "good", "okay", "yeah", "yep" as standalone fillers
   - "signing off", "send report", "thank you", "template" — end-of-dictation commands
   - "stop", "stopped", "stopping" — sentence-ending commands (replace with period)
   - "full stop" -> period

7. **Contractions**: Expand contractions: "don't" -> "do not", "can't" -> "cannot", etc.

8. **Hyphenation**: Use hyphens for: ground-glass, non-contrast, non-specific, non-tender, cross-sectional, intra-articular, post-menopausal

9. **Procedure title**: If a procedure description is provided, include it as an UPPERCASE title at the top of the report. Do not repeat it in the body text.

10. **Clinical history**: If clinical history is provided separately, include it under the CLINICAL HISTORY heading. Do not duplicate it from the dictation.

11. **Spine level sub-headings**: Preserve sub-headings like "L4/5:", "C5/6:" within the FINDINGS section — these are NOT new top-level sections.

12. **Output format**: Return ONLY the formatted report text. No markdown, no explanations, no preamble. Section headings in UPPERCASE on their own line, with a blank line before each heading.
"""


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def llm_format(
    raw_text: str,
    *,
    modality_code: str | None = None,
    procedure_description: str | None = None,
    clinical_history: str | None = None,
    doctor_id: str | None = None,
) -> LLMFormatResult:
    """Format a raw Deepgram transcript using Claude.

    Args:
        raw_text: Raw transcript text from Deepgram.
        modality_code: Imaging modality (CR, CT, US, MR, etc.).
        procedure_description: Procedure name from the RIS order.
        clinical_history: Clinical notes / complaint from the referral.
        doctor_id: Doctor ID for profile-based customisation.

    Returns:
        LLMFormatResult with the formatted report and usage metrics.
    """
    client = _get_client()
    store = get_config_store()
    model = store.get_global("llm_model") or "claude-sonnet-4-20250514"

    # Build system prompt with modality and doctor context
    system = _SYSTEM_PROMPT

    structure = _MODALITY_STRUCTURES.get(modality_code or "", "CLINICAL HISTORY > FINDINGS > CONCLUSION")
    system += f"\n\n## Modality: {modality_code or 'Unknown'}\nDefault section structure: {structure}\n"

    doctor_ctx = _get_doctor_context(doctor_id, modality_code)
    if doctor_ctx:
        system += doctor_ctx

    # Build user message
    parts = []
    if procedure_description:
        parts.append(f"Procedure: {procedure_description}")
    if clinical_history:
        parts.append(f"Clinical history: {clinical_history}")
    if modality_code:
        parts.append(f"Modality: {modality_code}")
    parts.append(f"\nRaw transcript:\n{raw_text}")

    user_message = "\n".join(parts)

    logger.info("Sending transcript to Claude (%s, %d chars)", model, len(raw_text))
    start = time.monotonic()

    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )

    elapsed_ms = int((time.monotonic() - start) * 1000)

    formatted = response.content[0].text.strip()
    usage = response.usage

    logger.info(
        "Claude formatting complete — %dms, %d input / %d output tokens",
        elapsed_ms, usage.input_tokens, usage.output_tokens,
    )

    return LLMFormatResult(
        formatted_text=formatted,
        model=model,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        duration_ms=elapsed_ms,
    )
