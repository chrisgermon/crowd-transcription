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
# Example report fetching (few-shot learning from Karisma)
# ---------------------------------------------------------------------------

_EXAMPLE_CACHE: dict[str, list[str]] = {}


def _get_example_reports(doctor_id: str | None, modality_code: str | None, limit: int = 3) -> list[str]:
    """Fetch recent typed Karisma reports for the same doctor+modality.

    Used as few-shot examples so the LLM matches the doctor's style.
    Results are cached in memory to avoid repeated DB queries.
    """
    cache_key = f"{doctor_id}:{modality_code}"
    if cache_key in _EXAMPLE_CACHE:
        return _EXAMPLE_CACHE[cache_key]

    examples = []
    try:
        from crowdtrans.config_store import get_config_store
        from crowdtrans.database import SessionLocal
        from crowdtrans.models import Transcription

        store = get_config_store()
        sites = store.get_enabled_site_configs()
        karisma = next((s for s in sites if s.ris_type == "karisma"), None)
        if not karisma:
            return []

        # Get recent completed transcriptions for this doctor+modality
        with SessionLocal() as session:
            query = (
                session.query(Transcription.source_dictation_id)
                .filter(
                    Transcription.status == "complete",
                    Transcription.site_id == "karisma",
                )
            )
            if doctor_id:
                query = query.filter(Transcription.doctor_id == str(doctor_id))
            if modality_code:
                query = query.filter(Transcription.modality_code == modality_code)

            txn_ids = [
                r[0] for r in query.order_by(Transcription.dictation_date.desc())
                .limit(limit * 3)  # fetch more in case some don't have reports
                .all()
            ]

        if not txn_ids:
            return []

        # Fetch the typed reports from Karisma
        from crowdtrans.karisma import fetch_reports
        reports = fetch_reports(karisma, txn_ids)

        # Take up to `limit` reports, prefer shorter ones (more typical)
        for tk in txn_ids:
            if tk in reports and len(reports[tk]) > 50:
                examples.append(reports[tk])
                if len(examples) >= limit:
                    break

    except Exception as e:
        logger.warning("Failed to fetch example reports: %s", e)

    _EXAMPLE_CACHE[cache_key] = examples
    return examples


def clear_example_cache():
    """Clear the example report cache (e.g. after learning runs)."""
    _EXAMPLE_CACHE.clear()


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

1. **Structure**: Organise the report into standard sections with headings on their own line. If example reports from this doctor are provided, match their exact heading format (e.g. "Clinical Details:" vs "CLINICAL HISTORY", "Findings:" vs "FINDINGS"). Otherwise default to UPPERCASE headings. The section structure depends on the imaging modality.

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
   - "calcification" -> "opacification" (when describing sinus/nasal soft tissue density)
   - "anterocoanal" -> "antrochoanal" (polyp)
   - "mucus" -> "mucous" (when used as adjective, e.g. "mucous retention cyst")
   - "would be" -> "appear" (e.g. "joints would be normal" -> "joints appear normal")
   - "Comic underline" / "underline" -> dictation command artifacts, remove entirely
   - "Unloud" / "See sinuses" -> dictation artifacts at start, remove
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

12. **Output format**: Return ONLY the formatted report text. No markdown, no explanations, no preamble. Match the heading style from the doctor's example reports if provided.

13. **CRITICAL SPACING RULES** (match the RIS system exactly):
   - NO blank lines anywhere in the report. Use single newlines only.
   - Section headings go on their own line, with content starting on the NEXT line (no blank line between heading and content).
   - Procedure title is the first line, heading immediately on the next line.
   - Each paragraph/sentence continues on the next line with no blank lines between them.
   - Heading format: "Heading:" with a colon, then content on the next line.
   - Example of correct format:
     ```
     ULTRASOUND RIGHT SHOULDER
     Clinical Notes:
     Right shoulder pain, restricted movement.
     Findings:
     The supraspinatus tendon is intact. Normal biceps tendon.
     CONCLUSION:
     No rotator cuff tear. Mild subacromial bursitis.
     ```
   - WRONG (do NOT do this):
     ```
     ULTRASOUND RIGHT SHOULDER

     Clinical Notes:

     Right shoulder pain.

     Findings:

     The supraspinatus tendon is intact.
     ```
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
    existing_report_text: str | None = None,
) -> LLMFormatResult:
    """Format a raw Deepgram transcript using Claude.

    Args:
        raw_text: Raw transcript text from Deepgram.
        modality_code: Imaging modality (CR, CT, US, MR, etc.).
        procedure_description: Procedure name from the RIS order.
        clinical_history: Clinical notes / complaint from the referral.
        doctor_id: Doctor ID for profile-based customisation.
        existing_report_text: Pre-populated report content from the RIS
            (e.g. sonographer template with measurements). When present,
            the dictation contains editing instructions rather than a
            full report dictation.

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

    # Add example reports from this doctor for style matching
    examples = _get_example_reports(doctor_id, modality_code, limit=3)
    if examples:
        system += "\n\n## Style Reference: Example Reports from This Doctor\n"
        system += "Match the formatting style, heading names, paragraph structure, "
        system += "and terminology preferences shown in these example reports:\n"
        for i, ex in enumerate(examples, 1):
            # Truncate long examples to keep token usage reasonable
            truncated = ex[:1500] if len(ex) > 1500 else ex
            system += f"\n--- Example {i} ---\n{truncated}\n"
        system += "\n--- End of examples ---\n"
        system += (
            "\nIMPORTANT: Use the same heading names (e.g. 'Clinical Details:' vs "
            "'Clinical History:', 'Report:' vs 'Findings:') and the same style of "
            "phrasing as these examples. The doctor's preferred style takes priority "
            "over the default structure. Note that RIS reports NEVER have blank lines "
            "— every line flows directly to the next with single newlines only.\n"
        )

    # When a pre-populated report template exists, add merge instructions
    if existing_report_text:
        system += """

## IMPORTANT: Pre-populated Report Template

A report template has already been partially filled in (e.g. by the sonographer with measurements and preliminary findings) BEFORE the radiologist dictated. The radiologist's dictation contains EDITING INSTRUCTIONS for this template, NOT a complete standalone report.

You must MERGE the dictation instructions with the existing template. Common patterns:
- "reports already there" / "report is already there" — the template content is the base; the dictation only adds/modifies specific parts
- "use my standard template" / "use my template" — apply the radiologist's standard template, incorporating the dictated changes
- "please add [text]" — add the specified text to the appropriate section
- "please change [X] to [Y]" / "change the [X] put in [Y]" — replace X with Y in the template
- "copy the clinical notes" — the clinical history from the referral should be placed under CLINICAL HISTORY
- "conclusion is [text]" / "in conclusion [text]" — set or update the CONCLUSION section
- "for the [section], please add [text]" — add to a specific section
- "delete the [section]" / "remove [text]" — remove content from the template
- "[patient name] [procedure]" at the start — strip the patient name and procedure echo

Start with the existing template as the base and apply the dictation instructions to produce the final report. Strip all meta-commands (the instructions themselves) from the output — only include the resulting clinical content.
"""

    # Build user message
    parts = []
    if procedure_description:
        parts.append(f"Procedure: {procedure_description}")
    if clinical_history:
        parts.append(f"Clinical history: {clinical_history}")
    if modality_code:
        parts.append(f"Modality: {modality_code}")
    if existing_report_text:
        parts.append(f"\nExisting report template (pre-populated before dictation):\n{existing_report_text}")
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
