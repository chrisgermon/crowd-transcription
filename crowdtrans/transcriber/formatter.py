"""Post-process Deepgram transcripts for radiology report formatting.

Patterns learned from analysis of 15,000+ Visage clinical_document reports
and comparison of 3,247 transcript-report pairs.

Handles:
- Australian English dictation commands ("stop" -> ".", "full stop" -> ".")
- Spoken formatting commands that Deepgram's dictation mode may miss
- Medical term corrections learned from transcript/report comparison
- Deepgram pronunciation mishear corrections (e.g. "retrotter" -> "rotator")
- Measurement formatting (millimeters -> mm, centimeters -> cm)
- Ordinal number formatting (fourth -> 4th)
- Australian English spelling normalisation
- Content-based section classification learned from Visage report patterns
- Automatic section headings based on modality and procedure type
- Procedure description deduplication (strip from body when used as title)
- Cleanup of residual dictation artifacts and filler words
- Filler word removal (so, again, signing off, send report)
- Spine level sub-heading preservation within FINDINGS
"""

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standard section headings per modality (from 15,000+ Visage report analysis)
# ---------------------------------------------------------------------------
# CR: 91.8% CLINICAL HISTORY > FINDINGS (no conclusion)
#      5.9% CLINICAL HISTORY > FINDINGS > CONCLUSION
# CT: 67.7% CLINICAL HISTORY > PROCEDURE > FINDINGS > CONCLUSION
#     ~15%  CLINICAL HISTORY > FINDINGS > CONCLUSION
# US: 74.2% CLINICAL HISTORY > FINDINGS > CONCLUSION
#     ~14%  CLINICAL HISTORY > FINDINGS
# BMD: 94.5% CLINICAL HISTORY > FINDINGS > CONCLUSION

# ---------------------------------------------------------------------------
# Doctor profiles (learned from retrospective analysis of Visage reports)
# ---------------------------------------------------------------------------

_DOCTOR_PROFILES: dict | None = None
_PROFILES_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "doctor_profiles.json"
# Fallback for deployed location
_PROFILES_PATH_ALT = Path("/opt/crowdtrans/data/doctor_profiles.json")


def _load_doctor_profiles() -> dict:
    """Load doctor formatting profiles from JSON. Cached after first load."""
    global _DOCTOR_PROFILES
    if _DOCTOR_PROFILES is not None:
        return _DOCTOR_PROFILES
    for path in (_PROFILES_PATH, _PROFILES_PATH_ALT):
        if path.exists():
            try:
                _DOCTOR_PROFILES = json.loads(path.read_text(encoding="utf-8"))
                logger.info("Loaded doctor profiles from %s (%d doctors)", path, len(_DOCTOR_PROFILES))
                return _DOCTOR_PROFILES
            except Exception as e:
                logger.warning("Failed to load doctor profiles from %s: %s", path, e)
    _DOCTOR_PROFILES = {}
    return _DOCTOR_PROFILES


def _get_doctor_mod_data(doctor_id: str | None, modality_code: str | None) -> dict | None:
    """Get the modality-specific profile data for a doctor. Returns None if unavailable."""
    if not doctor_id:
        return None
    profiles = _load_doctor_profiles()
    profile = profiles.get(str(doctor_id))
    if not profile:
        return None
    modalities = profile.get("modalities", {})
    return modalities.get(modality_code or "", None)


def _get_doctor_heading_map(doctor_id: str | None, modality_code: str | None) -> dict[str, str] | None:
    """Get per-doctor heading renames (e.g. FINDINGS -> REPORT for Dr. Ng).

    Returns a dict mapping canonical heading names to doctor-preferred names,
    or None if no overrides are needed.
    """
    mod_data = _get_doctor_mod_data(doctor_id, modality_code)
    if not mod_data:
        return None
    structures = mod_data.get("section_structure", mod_data.get("section_structures", {}))
    heading_map = {}
    total = sum(structures.values())
    report_count = sum(v for k, v in structures.items() if "REPORT" in k and "FINDINGS" not in k)
    findings_count = sum(v for k, v in structures.items() if "FINDINGS" in k)
    if total > 0 and report_count > findings_count:
        heading_map["FINDINGS"] = "REPORT"
    return heading_map if heading_map else None


def _get_doctor_headings(doctor_id: str | None, modality_code: str | None) -> list[str] | None:
    """Get the doctor's preferred section heading list for a modality.

    Derived from the most common section_structure pattern in their profile.
    Returns None to fall back to global modality defaults.
    """
    mod_data = _get_doctor_mod_data(doctor_id, modality_code)
    if not mod_data or mod_data.get("count", 0) < 5:
        return None  # Not enough data
    structures = mod_data.get("section_structure", mod_data.get("section_structures", {}))
    if not structures:
        return None
    # Find the most common structure
    best_seq = max(structures, key=structures.get)
    # Parse "CLINICAL HISTORY > FINDINGS > CONCLUSION" into list
    headings = [h.strip() for h in best_seq.split(">")]
    # Deduplicate (some structures have "CONCLUSION > CONCLUSION")
    seen = set()
    unique = []
    for h in headings:
        if h not in seen:
            seen.add(h)
            unique.append(h)
    return unique


def _doctor_uses_conclusion(doctor_id: str | None, modality_code: str | None) -> bool | None:
    """Check if this doctor typically includes a CONCLUSION section.

    Returns True/False based on their section_presence_pct, or None if unknown.
    Uses 30% threshold: below 30% means the doctor rarely uses CONCLUSION.
    """
    mod_data = _get_doctor_mod_data(doctor_id, modality_code)
    if not mod_data or mod_data.get("count", 0) < 5:
        return None
    presence = mod_data.get("section_presence_pct", {})
    conclusion_pct = presence.get("CONCLUSION", None)
    if conclusion_pct is None:
        return None
    return conclusion_pct >= 30.0


# Word corrections that are noise (common function words, not real corrections)
_CORRECTION_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "in", "at", "of", "for",
    "to", "and", "or", "on", "with", "as", "by", "it", "this", "that",
}


def _get_doctor_word_corrections(doctor_id: str | None, modality_code: str | None) -> list[tuple[re.Pattern, str]]:
    """Get doctor-specific word corrections from their profile.

    Only returns high-confidence corrections (2+ occurrences) that aren't
    already handled by global corrections or are common function words.
    """
    mod_data = _get_doctor_mod_data(doctor_id, modality_code)
    if not mod_data:
        return []
    corrections = mod_data.get("word_corrections", [])
    result = []
    for item in corrections:
        if len(item) < 3:
            continue
        wrong, right, count = item[0], item[1], item[2]
        if count < 2:
            continue
        # Skip function word noise
        if wrong.lower() in _CORRECTION_STOPWORDS or right.lower() in _CORRECTION_STOPWORDS:
            continue
        # Skip if wrong == right (case-only differences)
        if wrong.lower() == right.lower():
            continue
        # Skip single-character corrections
        if len(wrong) <= 1 or len(right) <= 1:
            continue
        # Build a word-boundary regex
        pattern = re.compile(r'\b' + re.escape(wrong) + r'\b', re.IGNORECASE)
        result.append((pattern, right))
    return result


_MODALITY_HEADINGS = {
    "CR": ["CLINICAL HISTORY", "FINDINGS", "CONCLUSION"],
    "CT": ["CLINICAL HISTORY", "PROCEDURE", "FINDINGS", "CONCLUSION"],
    "US": ["CLINICAL HISTORY", "FINDINGS", "CONCLUSION"],
    "MR": ["CLINICAL HISTORY", "PROCEDURE", "FINDINGS", "CONCLUSION"],
    "MG": ["CLINICAL HISTORY", "FINDINGS", "CONCLUSION"],
    "NM": ["CLINICAL HISTORY", "PROCEDURE", "FINDINGS", "CONCLUSION"],
    "BMD": ["CLINICAL HISTORY", "FINDINGS", "CONCLUSION"],
    "DSA": ["CLINICAL HISTORY", "PROCEDURE", "FINDINGS", "CONCLUSION"],
}

_DEFAULT_HEADINGS = ["CLINICAL HISTORY", "FINDINGS", "CONCLUSION"]

# ---------------------------------------------------------------------------
# Medical term corrections (learned from 3,247 transcript-report comparisons)
# ---------------------------------------------------------------------------

_MEDICAL_CORRECTIONS = [
    # Measurement formatting (17+ occurrences in comparison data)
    (re.compile(r'\b(\d+)\s+millimeters?\b', re.IGNORECASE), r'\1mm'),
    (re.compile(r'\b(\d+)\s+centimeters?\b', re.IGNORECASE), r'\1cm'),
    (re.compile(r'\b(\d+)\s+millimetres?\b', re.IGNORECASE), r'\1mm'),
    (re.compile(r'\b(\d+)\s+centimetres?\b', re.IGNORECASE), r'\1cm'),
    (re.compile(r'\b(\d+)\s+milliliters?\b', re.IGNORECASE), r'\1ml'),
    (re.compile(r'\b(\d+)\s+millilitres?\b', re.IGNORECASE), r'\1ml'),
    # "by" -> "x" for dimensions (13x in comparison): "5 by 3" -> "5 x 3"
    (re.compile(r'(\d+)\s*(?:mm|cm)?\s+by\s+(\d+)', re.IGNORECASE),
     lambda m: f"{m.group(1)} x {m.group(2)}"),
    # "cc's" / "ccs" -> "cc" (unit cleanup, 8x in transcripts)
    (re.compile(r"\bcc['']?s\b", re.IGNORECASE), 'cc'),

    # "beats per minute" -> "bpm" (2x in 177-pair comparison)
    (re.compile(r'\bbeats\s+per\s+minute\b', re.IGNORECASE), 'bpm'),

    # Ordinal number formatting (4x+ in comparisons: "fourth" -> "4th")
    (re.compile(r'\bfirst\b', re.IGNORECASE), '1st'),
    (re.compile(r'\bsecond\b', re.IGNORECASE), '2nd'),
    (re.compile(r'\bthird\b', re.IGNORECASE), '3rd'),
    (re.compile(r'\bfourth\b', re.IGNORECASE), '4th'),
    (re.compile(r'\bfifth\b', re.IGNORECASE), '5th'),

    # Vertebral level formatting (L 5 -> L5, C 5 -> C5) (4x+ in comparisons)
    (re.compile(r'\b([LCST])\s+(\d)\b'), r'\1\2'),
    (re.compile(r'\b([LCST])(\d)\s*/\s*(\d)\b'), r'\1\2/\3'),
    (re.compile(r'\b([LCST])(\d)\s*/\s*([LCST])(\d)\b'), r'\1\2/\3\4'),

    # Hyphenated compound terms (from report analysis)
    (re.compile(r'\bground\s+glass\b', re.IGNORECASE), 'ground-glass'),
    (re.compile(r'\bx\s+rays?\b', re.IGNORECASE),
     lambda m: m.group(0).replace(' ', '-')),
    (re.compile(r'\bcross\s+sectional\b', re.IGNORECASE), 'cross-sectional'),
    # "postmenopausal" / "post menopausal" -> "post-menopausal" (2x in 177 pairs)
    (re.compile(r'\bpostmenopausal\b', re.IGNORECASE), 'post-menopausal'),
    (re.compile(r'\bpost\s+menopausal\b', re.IGNORECASE), 'post-menopausal'),
    # "non contrast" -> "non-contrast" (16x in transcripts)
    (re.compile(r'\bnon\s+contrast\b', re.IGNORECASE), 'non-contrast'),

    # --- Deepgram pronunciation mishears (learned from 177 transcript-report
    # comparisons; multi-word patterns first, then single-word) ---

    # Multi-word mishears (2x+ in comparisons)
    (re.compile(r'\bnear fusion\b', re.IGNORECASE), 'knee effusion'),
    # "small/moderate/large/no/joint/pleural fusion" -> effusion (from 3,247-pair analysis)
    (re.compile(r'\b(small|moderate|large|mild|minimal|trace)\s+fusion\b', re.IGNORECASE),
     lambda m: m.group(1) + ' effusion'),
    (re.compile(r'\b(joint|knee|pleural|pericardial|hip|shoulder|ankle|elbow|glenohumeral)\s+fusion\b', re.IGNORECASE),
     lambda m: m.group(1) + ' effusion'),
    (re.compile(r'\bno\s+fusion\b(?=\s+(?:is|on|seen|noted|identified))', re.IGNORECASE), 'no effusion'),
    (re.compile(r'\bsun and nasal\b', re.IGNORECASE), 'sinonasal'),
    (re.compile(r'\bbunch of bone\b', re.IGNORECASE), 'bunching on'),
    (re.compile(r'\bin plate\b', re.IGNORECASE), 'endplate'),
    (re.compile(r'\bcollateral lymph\b', re.IGNORECASE), 'collateral ligament'),
    (re.compile(r'\bannular plate\b', re.IGNORECASE), 'volar plate'),
    (re.compile(r'\bballoon effusion\b', re.IGNORECASE), 'glenohumeral effusion'),
    (re.compile(r'\bincompetent subunit\b', re.IGNORECASE), 'incompetence'),
    (re.compile(r'\bcardio mediastinum\b', re.IGNORECASE), 'cardiomediastinum'),
    (re.compile(r'\bnormal stomach on\b', re.IGNORECASE), 'normal'),
    # New mishears from 15K-report deep analysis
    (re.compile(r'\bsingle\s+live\s+intruder\s+on\b', re.IGNORECASE), 'single live intrauterine'),
    (re.compile(r'\bcell\s*stone\b', re.IGNORECASE), 'Celestone'),
    (re.compile(r'\bcommon\s+sense\b(?=\s+(?:origin|tendon))', re.IGNORECASE), 'common extensor'),
    (re.compile(r'\bsemi\s+common\b', re.IGNORECASE), 'common'),
    (re.compile(r'\bsign\s+of\s+it\b', re.IGNORECASE), 'synovitis'),
    (re.compile(r'\bby\s+by\s+millimeters?\b', re.IGNORECASE), 'mm'),
    (re.compile(r'\bslightly\s+thick\b', re.IGNORECASE), 'slightly thickened'),

    # "endocrine" when dictating pelvic US (should be "endometrial/endometrium")
    (re.compile(r'\bendocrine\b(?=\s+(?:is|thickness|measures|mm|cm|\d))', re.IGNORECASE), 'endometrium'),

    # Single-word mishears -- anatomical terms
    (re.compile(r'\bretrotter\b', re.IGNORECASE), 'rotator'),
    (re.compile(r'\bretrotiga\b', re.IGNORECASE), 'rotator'),
    (re.compile(r'\bsubgranial\b', re.IGNORECASE), 'subacromial'),
    (re.compile(r'\bserogranular\b', re.IGNORECASE), 'subacromial'),
    (re.compile(r'\bsubcontour\b', re.IGNORECASE), 'contour'),
    (re.compile(r'\bcontrary\b(?=\s+(?:is|are|smooth))', re.IGNORECASE), 'contour'),
    (re.compile(r'\bgeneration\b', re.IGNORECASE), 'degeneration'),
    (re.compile(r'\bgenerations\b', re.IGNORECASE), 'degeneration'),
    (re.compile(r'\btriscaphy\b', re.IGNORECASE), 'triscaphe'),
    (re.compile(r'\bantralsthesis\b', re.IGNORECASE), 'anterolisthesis'),
    (re.compile(r'\bthorogolumbar\b', re.IGNORECASE), 'thoracolumbar'),
    (re.compile(r'\bsubchronic\b', re.IGNORECASE), 'subchorionic'),
    (re.compile(r'\bicosus\b', re.IGNORECASE), 'echoes'),
    (re.compile(r'\bintegrate\b(?=\s+(?:flow|from))', re.IGNORECASE), 'antegrade'),
    (re.compile(r'\binflamum\b', re.IGNORECASE), 'infraspinatus'),
    (re.compile(r'\bpterygoid\b(?=\s+ganglion)', re.IGNORECASE), 'paralabral'),
    (re.compile(r'\bpropria\b', re.IGNORECASE), 'omental'),
    (re.compile(r'\bglenium\b', re.IGNORECASE), 'glenohumeral'),
    (re.compile(r'\bbarotral\b', re.IGNORECASE), 'bilateral'),
    (re.compile(r'\bphony\b', re.IGNORECASE), 'bony'),
    # New mishears from doctor style analysis (208 report pairs)
    (re.compile(r'\bperjury\b(?=\s+(?:is|at|of|and|facet))', re.IGNORECASE), 'hypertrophy'),
    (re.compile(r'\bactually\b(?=\s+(?:normal|symmetric|thickened|enlarged|seen))', re.IGNORECASE), 'bilaterally'),
    (re.compile(r'\b(?<!\w)fusion\b(?=\s+(?:is|on|of|in|at|seen|noted))', re.IGNORECASE), 'effusion'),
    # "inclusion" -> "conclusion" (175x in transcripts, 0x in reports)
    # Deepgram hears "conclusion" as "inclusion" — match as standalone or "in inclusion"
    (re.compile(r'\bin\s+inclusion\b', re.IGNORECASE), 'in conclusion'),
    (re.compile(r'\binclusion\b(?=\s*[,.:;]|\s+(?:is|are|was|being))', re.IGNORECASE), 'conclusion'),
    # "angular" -> "annular" (30x) in spine/disc context
    (re.compile(r'\bangular\b(?=\s+(?:tear|fissure|bulge|disc|protrusion|rupture))', re.IGNORECASE), 'annular'),
    # "foramen" -> "foraminal" (13x) when used as adjective before stenosis/narrowing
    (re.compile(r'\bforamen\b(?=\s+(?:stenosis|narrowing|encroachment|compromise))', re.IGNORECASE), 'foraminal'),
    # "basilar" -> "vertebrobasilar" (13x) in specific vascular context
    (re.compile(r'\bbasilar\b(?=\s+(?:insufficiency|circulation))', re.IGNORECASE), 'vertebrobasilar'),
    # "bugling" -> "bulging" (129x — Deepgram mishear, never correct in radiology)
    (re.compile(r'\bbugling\b', re.IGNORECASE), 'bulging'),
    # "fracturing" -> "fracture" (444x — reports use noun form, not gerund)
    (re.compile(r'\bfracturing\b', re.IGNORECASE), 'fracture'),
    # "impingements" -> "impingement" (153x — singular preferred)
    (re.compile(r'(?<!\d\s)(?<!\d)\bimpingements\b', re.IGNORECASE), 'impingement'),
    # "mils" -> measurement unit (context-dependent: injection = ml, dimension = mm)
    (re.compile(r'\b(\d+)\s*mils?\b(?=\s+(?:of\s+)?(?:celestone|lignocaine|lidocaine|cortisone|marcaine|xylocaine|saline|contrast|local))', re.IGNORECASE), r'\1ml'),
    (re.compile(r'\b(\d+)\s*mils?\b', re.IGNORECASE), r'\1mm'),

    # Australian English spelling (from 15,000+ report analysis)
    # -emia -> -aemia (2114x in reports)
    (re.compile(r'\bhyperemia\b', re.IGNORECASE), 'hyperaemia'),
    (re.compile(r'\banemia\b', re.IGNORECASE), 'anaemia'),
    (re.compile(r'\bischemia\b', re.IGNORECASE), 'ischaemia'),
    (re.compile(r'\bischemic\b', re.IGNORECASE), 'ischaemic'),
    (re.compile(r'\bleukaemia\b', re.IGNORECASE), 'leukaemia'),
    # -edema -> oedema (222x in reports)
    (re.compile(r'\bedema\b', re.IGNORECASE), 'oedema'),
    (re.compile(r'\bledema\b', re.IGNORECASE), 'oedema'),
    # -esophag -> oesophag (47x in reports)
    (re.compile(r'\besophagus\b', re.IGNORECASE), 'oesophagus'),
    (re.compile(r'\besophageal\b', re.IGNORECASE), 'oesophageal'),
    (re.compile(r'\besophagitis\b', re.IGNORECASE), 'oesophagitis'),
    # -hemorrhag -> haemorrhag (297x in reports)
    (re.compile(r'\bhemorrhage\b', re.IGNORECASE), 'haemorrhage'),
    (re.compile(r'\bhemorrhagic\b', re.IGNORECASE), 'haemorrhagic'),
    # -hemoglobin -> haemoglobin
    (re.compile(r'\bhemoglobin\b', re.IGNORECASE), 'haemoglobin'),
    (re.compile(r'\bhemodynamic\w*\b', re.IGNORECASE),
     lambda m: 'haemo' + m.group(0)[4:].replace('hemo', 'haemo')),
    # foetal (575x in reports)
    (re.compile(r'\bfetus\b', re.IGNORECASE), 'foetus'),
    (re.compile(r'\bfetal\b', re.IGNORECASE), 'foetal'),
    # Other AU spellings
    (re.compile(r'\bpediatric\b', re.IGNORECASE), 'paediatric'),
    (re.compile(r'\bcecum\b', re.IGNORECASE), 'caecum'),
    (re.compile(r'\bmaneuver\b', re.IGNORECASE), 'manoeuvre'),
    (re.compile(r'\bgynecolog', re.IGNORECASE), 'gynaecolog'),
    (re.compile(r'\borthopedic\b', re.IGNORECASE), 'orthopaedic'),
    (re.compile(r'\banesthetic\b', re.IGNORECASE), 'anaesthetic'),
    (re.compile(r'\banesthesia\b', re.IGNORECASE), 'anaesthesia'),
    # -ization -> -isation (AU spelling, 51x characterised, 25x localised)
    (re.compile(r'\bdemineralization\b', re.IGNORECASE), 'demineralisation'),
    (re.compile(r'\bmineralization\b', re.IGNORECASE), 'mineralisation'),
    (re.compile(r'\bcharacterization\b', re.IGNORECASE), 'characterisation'),
    # -ized -> -ised
    (re.compile(r'\bvisualized\b', re.IGNORECASE), 'visualised'),
    (re.compile(r'\bcharacterized\b', re.IGNORECASE), 'characterised'),
    (re.compile(r'\blocalized\b', re.IGNORECASE), 'localised'),
    (re.compile(r'\brecognized\b', re.IGNORECASE), 'recognised'),
    (re.compile(r'\borganized\b', re.IGNORECASE), 'organised'),
    # gray -> grey (AU English)
    (re.compile(r'\bgray\b', re.IGNORECASE), 'grey'),
    (re.compile(r'\bgray-matter\b', re.IGNORECASE), 'grey-matter'),
    # -or -> -our (35x tumour in reports)
    (re.compile(r'\btumor\b', re.IGNORECASE), 'tumour'),
    (re.compile(r'\btumors\b', re.IGNORECASE), 'tumours'),
    # -er -> -re
    (re.compile(r'\bfiber\b', re.IGNORECASE), 'fibre'),
    (re.compile(r'\bfibers\b', re.IGNORECASE), 'fibres'),
    # fecal -> faecal (22x in 3,247-pair analysis)
    (re.compile(r'\bfecal\b', re.IGNORECASE), 'faecal'),
    # hematoma -> haematoma (22x)
    (re.compile(r'\bhematoma\b', re.IGNORECASE), 'haematoma'),
    (re.compile(r'\bhematomas\b', re.IGNORECASE), 'haematomas'),
    # osteopenia -> osteopaenia (11x)
    (re.compile(r'\bosteopenia\b', re.IGNORECASE), 'osteopaenia'),
    (re.compile(r'\bosteopenic\b', re.IGNORECASE), 'osteopaenic'),
    # lidocaine -> lignocaine (12x, AU drug name)
    (re.compile(r'\blidocaine\b', re.IGNORECASE), 'lignocaine'),

    # Hyphenation corrections (from 3,247-pair analysis)
    # nonspecific -> non-specific (27x)
    (re.compile(r'\bnonspecific\b', re.IGNORECASE), 'non-specific'),
    # nontender -> non-tender (26x)
    (re.compile(r'\bnontender\b', re.IGNORECASE), 'non-tender'),
    # intraarticular -> intra-articular (10x)
    (re.compile(r'\bintraarticular\b', re.IGNORECASE), 'intra-articular'),
    # periarticular -> peri-articular (AU preference)
    (re.compile(r'\bperiarticular\b', re.IGNORECASE), 'peri-articular'),

    # Contraction expansion (7x+ in comparisons)
    (re.compile(r"\bdon\s*'?\s*t\b", re.IGNORECASE), 'do not'),
    (re.compile(r"\bcan\s*'?\s*t\b", re.IGNORECASE), 'cannot'),
    (re.compile(r"\bwon\s*'?\s*t\b", re.IGNORECASE), 'will not'),
    (re.compile(r"\bisn\s*'?\s*t\b", re.IGNORECASE), 'is not'),
    (re.compile(r"\baren\s*'?\s*t\b", re.IGNORECASE), 'are not'),
    (re.compile(r"\bwasn\s*'?\s*t\b", re.IGNORECASE), 'was not'),
    (re.compile(r"\bweren\s*'?\s*t\b", re.IGNORECASE), 'were not'),
    (re.compile(r"\bdoesn\s*'?\s*t\b", re.IGNORECASE), 'does not'),
    (re.compile(r"\bdidn\s*'?\s*t\b", re.IGNORECASE), 'did not'),
    (re.compile(r"\bthere\s*'?\s*s\b", re.IGNORECASE), 'there is'),
    (re.compile(r"\bit\s*'\s*s\b", re.IGNORECASE), 'it is'),
    (re.compile(r"\bhe\s*'?\s*s\b", re.IGNORECASE), 'he is'),
    (re.compile(r"\bshe\s*'?\s*s\b", re.IGNORECASE), 'she is'),
    (re.compile(r"\bwe\s*'\s*re\b", re.IGNORECASE), 'we are'),
    (re.compile(r"\byou\s*'?\s*re\b", re.IGNORECASE), 'you are'),
    (re.compile(r"\bi\s*'\s*m\b", re.IGNORECASE), 'I am'),

    # Plural-to-singular normalization (Visage reports strongly prefer singular)
    # Only applied when NOT preceded by a number/quantifier to preserve "three fractures" etc.
    (re.compile(r'(?<!\d\s)(?<!\d)\b(abnormalities)\b', re.IGNORECASE), 'abnormality'),
    (re.compile(r'(?<!\d\s)(?<!\d)\b(effusions)\b', re.IGNORECASE), 'effusion'),
    (re.compile(r'\bfree\s+fluids\b', re.IGNORECASE), 'free fluid'),
    (re.compile(r'(?<!\d\s)(?<!\d)\b(concerns)\b', re.IGNORECASE), 'concern'),
    # Additional plural->singular from 208 report pair analysis
    (re.compile(r'(?<!\d\s)(?<!\d)\blesions\b', re.IGNORECASE), 'lesion'),
    (re.compile(r'(?<!\d\s)(?<!\d)\btears\b(?=\s|\.|\,|$)', re.IGNORECASE), 'tear'),
    (re.compile(r'(?<!\d\s)(?<!\d)\bfragments\b', re.IGNORECASE), 'fragment'),
    (re.compile(r'(?<!\d\s)(?<!\d)\bribs\b', re.IGNORECASE), 'rib'),
    (re.compile(r'\bpleural\s+fluids\b', re.IGNORECASE), 'pleural fluid'),

    # Spoken "comma" that Deepgram didn't convert (11x in extra words)
    (re.compile(r'\bcomma\b', re.IGNORECASE), ','),
]

# Filler/artifact words that appear in transcripts but not reports
# Only remove when they appear as standalone artifacts (start of sentence
# or after punctuation), not when part of meaningful text
_FILLER_PATTERNS = [
    # "Sorry" as dictation correction marker (41x in transcript-only words)
    (re.compile(r'\.\s*Sorry[,.]?\s*', re.IGNORECASE), '. '),
    (re.compile(r'^\s*Sorry[,.]?\s*', re.IGNORECASE), ''),
    # Mid-sentence "sorry" (dictator correcting themselves)
    (re.compile(r'\bsorry\s+(?:and\s+)?', re.IGNORECASE), ''),
    # "stop me" -> just "." (32x in transcripts -- dictator says "stop" + next word
    # starts with "me..." sound, Deepgram captures "stop me")
    (re.compile(r'\.\s*[Mm]e\.?\s*', re.IGNORECASE), '. '),
    (re.compile(r',\s*[Mm]e\.?\s+', re.IGNORECASE), '. '),
    # "stopped" as artifact (Deepgram interprets "stop" + trailing sound)
    (re.compile(r',?\s*stopped\b\.?\s*', re.IGNORECASE), '. '),
    # Trailing "me" artifacts from "let me" / "excuse me"
    (re.compile(r'\.\s*[Ll]et me[,.]?\s*$'), '.'),
    (re.compile(r'\.\s*[Ee]xcuse me[,.]?\s*', re.IGNORECASE), '. '),
    # "Good." as standalone filler (15x -- dictator says "good" between thoughts)
    (re.compile(r'\.\s*Good\.?\s+', re.IGNORECASE), '. '),
    (re.compile(r'^\s*Good\.?\s+', re.IGNORECASE | re.MULTILINE), ''),
    # "Okay." as standalone filler (dictator self-affirming)
    (re.compile(r'\.\s*Okay\.?\s+', re.IGNORECASE), '. '),
    (re.compile(r'^\s*Okay\.?\s+', re.IGNORECASE | re.MULTILINE), ''),
    # "Yeah" / "Yep" as filler
    (re.compile(r'(?:^|\.\s*)[Yy](?:eah|ep)\.?\s*', re.MULTILINE), ''),
    # "Heading" as a spoken command to the dictation system (not content)
    (re.compile(r'\b[Hh]eading\s+', re.IGNORECASE), ''),
    # "Copy" / "copy that" as spoken artifact
    (re.compile(r'\.\s*[Cc]opy(?:\s+that)?\.?\s*', re.IGNORECASE), '. '),
    # "Putting" as artifact (11x -- from "stop" being heard as "put in")
    (re.compile(r'\bputting\b', re.IGNORECASE), ''),
    # "question mark" spoken as command
    (re.compile(r'\bquestion\s+mark\b', re.IGNORECASE), '?'),
    # "Pause" as dictation command
    (re.compile(r'\.\s*[Pp]ause\.?\s*', re.IGNORECASE), '. '),

    # --- NEW filler patterns from 177-pair analysis ---

    # "So" as sentence opener filler (9x): "So, the findings..." -> "The findings..."
    # After a period: ". So, ..." -> ". " (preserve the period)
    (re.compile(r'\.\s+[Ss]o,?\s+'), '. '),
    # At start of text: "So, ..." -> ""
    (re.compile(r'^[Ss]o,?\s+', re.MULTILINE), ''),
    # "Again" as sentence opener filler (3x): "Again, there is..." -> "There is..."
    (re.compile(r'\.\s+[Aa]gain,?\s+'), '. '),
    (re.compile(r'^[Aa]gain,?\s+', re.MULTILINE), ''),

    # "Signing off" / "Signing out" -- end-of-dictation command (3x)
    (re.compile(r'\.?\s*[Ss]igning\s+(?:off|out)\.?\s*$'), '.'),
    (re.compile(r'\.?\s*[Ss]igning\s+(?:off|out)\.?\s*', re.IGNORECASE), '. '),

    # "Send report" / "send" as end-of-dictation command (3x)
    (re.compile(r'\.?\s*[Ss]end\s+report\.?\s*$'), '.'),
    (re.compile(r'\.?\s*[Ss]end\s+report\.?\s*', re.IGNORECASE), '. '),

    # "Correct" as standalone dictation correction command (3x)
    # Only strip when it appears as a standalone sentence, not as adjective
    # e.g. "Correct." or ". Correct." but not "correct position"
    (re.compile(r'\.\s*[Cc]orrect\.(?:\s+|$)'), '. '),
    (re.compile(r'^\s*[Cc]orrect\.\s*', re.MULTILINE), ''),

    # "Thank you" -- end-of-dictation artifact (3x, never in any Visage report)
    (re.compile(r'\.?\s*[Tt]hank\s+you\.?\s*$'), '.'),
    (re.compile(r'\.\s*[Tt]hank\s+you\.?\s+', re.IGNORECASE), '. '),

    # "stopping" artifact (9x — Deepgram interprets "stop" + trailing sound)
    (re.compile(r'\bstopping\b\.?\s*', re.IGNORECASE), '. '),

    # "Refer to combined report" / "I can't edit this" -- talking to assistant
    (re.compile(r'\.?\s*[Rr]efer\s+to\s+combined\s+report\.?\s*', re.IGNORECASE), '. '),
    (re.compile(r"\.?\s*I\s+can'?t\s+edit\s+this\.?\s*", re.IGNORECASE), '. '),

    # "Send it through for signing" -- end-of-dictation command
    (re.compile(r'\.?\s*[Ss]end\s+it\s+through\s+for\s+signing\.?\s*', re.IGNORECASE), '.'),

    # "Template" -- dictation system command artifact (128x in transcripts, 0x in reports)
    # The radiologist says "template" to trigger a template in their dictation system
    (re.compile(r'\.\s*[Tt]emplate\.?\s*', re.IGNORECASE), '. '),
    (re.compile(r'^\s*[Tt]emplate\.?\s*', re.IGNORECASE | re.MULTILINE), ''),
    (re.compile(r',?\s*template\b\.?\s*', re.IGNORECASE), '. '),
]

# ---------------------------------------------------------------------------
# Content-based section classification (learned from 15,000+ Visage reports)
# ---------------------------------------------------------------------------

# Patterns that indicate a dictator is explicitly naming a section.
# These patterns match and strip the spoken marker (including trailing
# "are", "is", ":", etc.) so the heading is not duplicated in the body.
# Longer patterns first to avoid partial matches.
_SPOKEN_SECTION_MARKERS = [
    (re.compile(r'^(?:the\s+)?clinical\s+(?:history|indication|details)\s*(?:is|are|:)?\s*', re.IGNORECASE), "CLINICAL HISTORY"),
    (re.compile(r'^(?:the\s+)?clinical\s+(?:history|indication|details)\b', re.IGNORECASE), "CLINICAL HISTORY"),
    (re.compile(r'^(?:the\s+)?history\b', re.IGNORECASE), "CLINICAL HISTORY"),
    (re.compile(r'^(?:the\s+)?indication\b', re.IGNORECASE), "CLINICAL HISTORY"),
    (re.compile(r'^(?:the\s+)?procedure\s+(?:is|was|:)\s*', re.IGNORECASE), "PROCEDURE"),
    (re.compile(r'^(?:the\s+)?procedure\b', re.IGNORECASE), "PROCEDURE"),
    (re.compile(r'^(?:the\s+)?technique\s*(?:is|was|:)?\s*', re.IGNORECASE), "PROCEDURE"),
    # "The findings are" / "findings are" -- 117x in transcripts (most common pattern)
    (re.compile(r'^(?:the\s+)?findings?\s+are\s*,?\s*', re.IGNORECASE), "FINDINGS"),
    (re.compile(r'^(?:the\s+)?findings?\s*(?::|,)\s*', re.IGNORECASE), "FINDINGS"),
    (re.compile(r'^(?:the\s+)?findings?\b', re.IGNORECASE), "FINDINGS"),
    (re.compile(r'^(?:the\s+)?report\b', re.IGNORECASE), "FINDINGS"),
    (re.compile(r'^(?:the\s+)?conclusion\s*(?:is|are|:)?\s*', re.IGNORECASE), "CONCLUSION"),
    (re.compile(r'^(?:the\s+)?impression\s*(?:is|are|:)?\s*', re.IGNORECASE), "CONCLUSION"),
    (re.compile(r'^(?:the\s+)?comment\b', re.IGNORECASE), "CONCLUSION"),
    (re.compile(r'^(?:the\s+)?opinion\b', re.IGNORECASE), "CONCLUSION"),
    (re.compile(r'^(?:the\s+)?summary\b', re.IGNORECASE), "CONCLUSION"),
    (re.compile(r'^in\s+(?:conclusion|summary)\b', re.IGNORECASE), "CONCLUSION"),
]

# Keywords strongly associated with CLINICAL HISTORY (from report analysis)
_CLINICAL_HISTORY_KEYWORDS = {
    "pain", "history", "chronic", "injury", "trauma", "complaint",
    "presenting", "referred", "referral", "symptoms", "worsening", "follow-up",
    "follow up", "known", "previous", "prior", "suspected", "query", "exclude",
    "rule out", "assess", "assessment", "investigate",
    "oa", "fracture", "bursitis", "dating scan",
}

# Keywords strongly associated with PROCEDURE section
_PROCEDURE_KEYWORDS = {
    "contrast", "non-contrast", "noncontrast", "post-contrast", "pre-contrast",
    "scan", "protocol", "technique", "sterile", "prep", "consent", "informed",
    "needle", "gauge", "injection", "injected", "administered", "sedation",
    "anaesthetic", "anesthetic", "lignocaine", "lidocaine",
    "euflexxa", "cortisone", "aseptic",
}

# Keywords strongly associated with FINDINGS section
_FINDINGS_KEYWORDS = {
    "there", "normal", "seen", "focal", "soft tissue", "echotexture", "architecture",
    "contour", "smooth", "unremarkable", "bilateral", "measures",
    "dimensions", "demonstrates", "shows", "reveals", "noted",
    "identified", "visualised", "visualized", "appear", "appears",
    "no evidence", "no significant", "no acute", "intact",
    "degenerative", "effusion", "calcification", "opacity",
    "attenuation", "enhancement", "parenchyma", "cortex",
    "liver", "kidney", "spleen", "pancreas", "gallbladder",
    "aorta", "vertebral", "disc", "joint", "tendon", "ligament",
    "transabdominal", "transvaginal", "uterus", "ovary",
    "bmd", "t-score", "z-score",
    "lungs", "pleural", "clear", "costophrenic",
    "symmetric", "non tender",
}

# Keywords strongly associated with CONCLUSION section
_CONCLUSION_KEYWORDS = {
    "major abnormality", "no major", "no significant abnormality",
    "unremarkable", "otherwise unremarkable",
    "could", "would", "should", "may", "suggest", "recommend",
    "clinical correlation", "correlate clinically",
    "consider", "advised", "advise", "if clinically",
    "further", "follow-up", "follow up", "review",
    "ongoing", "responds", "respond", "amenable",
    "bursitis", "tendinopathy", "impingement",
    "in keeping with", "consistent with", "suspicious",
    "no dvt", "no svt", "no fracture seen",
    "uncomplicated", "osteopaenia", "osteoporosis",
    "fatty liver", "prostatomegaly",
    "type 1 normal", "world health organization",
    "low-risk", "may respond",
}

# Opening phrases that strongly identify a section (from 15,000+ report analysis)
_FINDINGS_OPENERS = [
    # --- Organ/anatomy openers ---
    re.compile(r'^(?:the\s+)?liver\s+(?:echotexture|architecture|is)', re.IGNORECASE),
    # NEW: "The liver is echogenic/normal" (US, 383x)
    re.compile(r'^(?:the\s+)?liver\s+is\s+(?:echogenic|normal)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?kidneys?\s+(?:are|is)', re.IGNORECASE),
    # NEW: "The kidneys are symmetric/normal" (US, 513x)
    re.compile(r'^(?:the\s+)?kidneys?\s+(?:are|is)\s+(?:symmetric|normal)', re.IGNORECASE),
    re.compile(r'^there\s+(?:is|are)\s+(?:no|a|an|the|mild|moderate|severe)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:ac|glenohumeral|hip|knee|ankle|elbow|wrist)\s+joint', re.IGNORECASE),
    re.compile(r'^transabdominal\b', re.IGNORECASE),
    re.compile(r'^transvaginal\b', re.IGNORECASE),
    re.compile(r'^(?:i\s+)?(?:do\s+not|don\'t)\s+see\b', re.IGNORECASE),
    # NEW: "I do not see any" (CR, 180x)
    re.compile(r'^i\s+do\s+not\s+see\s+any', re.IGNORECASE),
    re.compile(r'^at\s+the\s+(?:region|level|site)\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:right|left|bilateral)\s+(?:kidney|ovary|breast|lung|hip|knee|shoulder|elbow|wrist|ankle)', re.IGNORECASE),
    re.compile(r'^bmd\b', re.IGNORECASE),
    re.compile(r'^ultrasound\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?uterus\b', re.IGNORECASE),
    # NEW: "The uterus is anteverted" (US, 207x)
    re.compile(r'^(?:the\s+)?uterus\s+is\s+anteverted', re.IGNORECASE),
    re.compile(r'^there\s+(?:is|are)\s+(?:five|four|six|seven)\s+lumbar', re.IGNORECASE),
    # NEW: "Five lumbar type" / "Seven cervical type" (CT, 74x/37x)
    re.compile(r'^(?:five|six|seven)\s+lumbar\s+type', re.IGNORECASE),
    re.compile(r'^(?:five|six|seven)\s+cervical\s+type', re.IGNORECASE),
    re.compile(r'^lungs?\s+and\s+pleural', re.IGNORECASE),
    # NEW: "The lungs are" (CR, 296x)
    re.compile(r'^(?:the\s+)?lungs?\s+(?:and\s+pleural|are)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:ac\s+joint|glenohumeral)\s+is', re.IGNORECASE),
    # New openers learned from 177+ transcript-report comparisons
    re.compile(r'^(?:the\s+)?(?:subacromial|subdeltoid)\s+bursa\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:biceps|triceps)\s+(?:tendon|insertion)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:common\s+)?(?:flexor|extensor)\s+(?:origin|tendon)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?carotid\s+artery', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:rotator\s+cuff|cuff)\s+(?:is|tendinopathy)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?patient\s+is\s+tender', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?region\s+of\s+interest', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?thyroid\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?gallbladder\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?spleen\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?pancreas\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?aorta\b', re.IGNORECASE),
    re.compile(r'^no\s+(?:pneumothorax|fracture|pleural)', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:endometri\w+|myometri\w+)\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?(?:ovaries|adnexa)\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?nerve\s+appears\b', re.IGNORECASE),
    re.compile(r'^(?:there\s+(?:is|are)\s+)?(?:\d+|five|four|six|seven)\s+lumbar', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?cardiomediastin\w+\b', re.IGNORECASE),
    re.compile(r'^single\s+live\s+intrauterine', re.IGNORECASE),

    # --- NEW openers from 15,000+ report analysis ---
    # "Both ovaries are" (US, 628x from trigrams)
    re.compile(r'^both\s+ovaries\s+are', re.IGNORECASE),
    # "The deep venous system" (US, 828x from trigrams)
    re.compile(r'^(?:the\s+)?deep\s+venous\s+system', re.IGNORECASE),
    # "Alignment is anatomic" (CR, 530x)
    re.compile(r'^alignment\s+is\s+anatomic', re.IGNORECASE),
    # "The tendons are/is" (US, 535x)
    re.compile(r'^(?:the\s+)?tendons?\s+(?:are|is)', re.IGNORECASE),
    # "The sacroiliac joints" (CT/CR, 572x)
    re.compile(r'^(?:the\s+)?sacroiliac\s+joints?', re.IGNORECASE),
    # "There is (no) intracranial/axillary" (CT, 249x)
    re.compile(r'^there\s+is\s+(?:no\s+)?(?:intracranial|axillary)', re.IGNORECASE),
    # "There is (diffuse) sinonasal" (CT, 44x)
    re.compile(r'^there\s+is\s+(?:diffuse\s+)?sinonasal', re.IGNORECASE),
    # "Liver, spleen, adrenal/and" (CT, 27x)
    re.compile(r'^liver,\s+spleen,?\s+(?:adrenal|and)', re.IGNORECASE),
    # "The patient's calcium score" (CT, 26x) -- could also be PROCEDURE for calcium scoring
    re.compile(r'^(?:the\s+)?patient\'?s?\s+calcium\s+score', re.IGNORECASE),
]

_CONCLUSION_OPENERS = [
    re.compile(r'^no\s+(?:major|significant)\s+(?:abnormality|finding|pathology)', re.IGNORECASE),
    re.compile(r'^(?:subacromial|trochanteric|olecranon|subdeltoid)\s+bursitis', re.IGNORECASE),
    re.compile(r'^degenerative\s+change', re.IGNORECASE),
    re.compile(r'^uncomplicated\b', re.IGNORECASE),
    re.compile(r'^(?:fatty|echogenic)\s+liver\b', re.IGNORECASE),
    re.compile(r'^(?:no\s+)?(?:dvt|svt|pe)\b', re.IGNORECASE),
    re.compile(r'^(?:prostatomegaly|hepatomegaly|splenomegaly)\b', re.IGNORECASE),
    re.compile(r'^(?:common\s+extensor|rotator\s+cuff)\s+tendinopathy', re.IGNORECASE),
    re.compile(r'^osteo(?:paenia|porosis)\b', re.IGNORECASE),
    re.compile(r'^type\s+1\s+normal\s+hips?\b', re.IGNORECASE),
    # Conclusion openers from 177+ comparisons
    re.compile(r'^mild\s+(?:common\s+)?(?:extensor|flexor)\s+tendinopathy', re.IGNORECASE),
    re.compile(r'^(?:no\s+significant\s+abnormality|nonspecific)\b', re.IGNORECASE),
    re.compile(r'^possible\s+(?:ulnar|carpal|radial)\b', re.IGNORECASE),
    re.compile(r'^(?:cuff|rotator)\s+(?:tendinopathy|tear)\b', re.IGNORECASE),
    re.compile(r'^(?:an?\s+)?(?:x-ray|mri|ct|ultrasound)\s+should\b', re.IGNORECASE),
    re.compile(r'^(?:an?\s+)?steroid\s+injection\b', re.IGNORECASE),
    re.compile(r'^if\s+(?:this\s+is\s+)?clinically\b', re.IGNORECASE),
    re.compile(r'^no\s+(?:other\s+)?(?:fracture|abnormality|pathology)\b', re.IGNORECASE),
    re.compile(r'^exact\s+cause\s+of\s+symptoms', re.IGNORECASE),
    re.compile(r'^(?:single\s+live\s+)?intrauterine\s+(?:gestation|pregnancy)', re.IGNORECASE),

    # --- NEW conclusion openers from 15,000+ report analysis ---
    # "Unremarkable" (US, 341x) -- VERY important opener
    re.compile(r'^unremarkable\b', re.IGNORECASE),
    # "Subacromial bursitis and impingement" (US, 249x)
    re.compile(r'^(?:subacromial\s+)?bursitis\s+and\s+impingement', re.IGNORECASE),
    # "No (major) abnormality (is) seen" (US, 106x+67x)
    re.compile(r'^no\s+(?:major\s+)?abnormality\s+(?:is\s+)?seen', re.IGNORECASE),
    # "No abnormality seen" (US, 67x)
    re.compile(r'^no\s+abnormality\s+seen', re.IGNORECASE),
    # "Trochanteric bursitis might/may/could" (US, 92x)
    re.compile(r'^trochanteric\s+bursitis\s+(?:might|may|could)', re.IGNORECASE),
    # "No morphologic abnormality" (US, 77x)
    re.compile(r'^no\s+morphologic(?:al)?\s+abnormality', re.IGNORECASE),
    # "No morphologic" shorthand (US, 77x)
    re.compile(r'^no\s+morphologic\b', re.IGNORECASE),
    # "(Uncomplicated) CT/ultrasound guided" (CT, 172x)
    re.compile(r'^(?:uncomplicated\s+)?(?:ct\s+guided|ultrasound\s+guided)', re.IGNORECASE),
    # "There is no major/significant intracranial" (CT, 72x)
    re.compile(r'^there\s+is\s+no\s+(?:major|significant)\s+intracranial', re.IGNORECASE),
    # "Degenerative changes (are) seen" (CT, 94x)
    re.compile(r'^degenerative\s+changes?\s+(?:are\s+)?seen', re.IGNORECASE),
    # "Multilevel (severe) degenerative" (CT, 34x)
    re.compile(r'^multilevel\s+(?:severe\s+)?degenerative', re.IGNORECASE),
    # "No convincing/significant fracture/intracranial" (CT/CR)
    re.compile(r'^no\s+(?:convincing|significant)\s+(?:fracture|intracranial)', re.IGNORECASE),
    # "Intermediate-risk superficial vein" (US, 552x)
    re.compile(r'^intermediate-?risk\s+superficial\s+vein', re.IGNORECASE),
    # "(An) isolated superficial vein" (US, 828x)
    re.compile(r'^(?:an?\s+)?isolated\s+superficial\s+vein', re.IGNORECASE),
]

_PROCEDURE_OPENERS = [
    re.compile(r'^(?:non[- ]?contrast|post[- ]?contrast|pre[- ]?contrast)\b', re.IGNORECASE),
    re.compile(r'^(?:sterile\s+prep)\b', re.IGNORECASE),
    re.compile(r'^(?:the\s+)?scan\s+(?:was|is)\b', re.IGNORECASE),
    # NEW: "informed consent (was) obtained" -- PROCEDURE section (253x+54x in CT/US)
    # Moved here from _FINDINGS_OPENERS since it belongs in PROCEDURE
    re.compile(r'^informed\s+consent\s+(?:was\s+)?obtained', re.IGNORECASE),
    # NEW: "Under ultrasound/ct guidance and aseptic" (604x)
    re.compile(r'^under\s+(?:ultrasound|ct)\s+guidance\s+and\s+aseptic', re.IGNORECASE),
    re.compile(r'^under\s+(?:ultrasound|ct)\s+guidance', re.IGNORECASE),
]

# Spine level sub-heading pattern: matches patterns like "L4/5:", "L3/4:", "C2/3:" etc.
# These are sub-headings WITHIN findings and should NOT trigger new top-level sections
_SPINE_LEVEL_SUBHEADING = re.compile(
    r'^[LCST]\d(?:/[LCST]?\d)?:\s*',
    re.IGNORECASE
)


def _classify_paragraph(text: str, modality_code: str | None = None) -> tuple[str | None, str]:
    """Classify a paragraph into a report section based on content.

    Returns (section_name, cleaned_text). Section name may be None if uncertain.
    When a spoken section marker is detected (e.g. dictator says "conclusion"),
    it is stripped from the paragraph text to avoid duplication with the heading.
    """
    text_stripped = text.strip()
    text_lower = text_stripped.lower()

    # 0. Check if this is a spine level sub-heading (e.g. "L4/5: ...")
    #    If so, do NOT classify it as a new section -- return None so it
    #    inherits the current section (should be FINDINGS)
    if _SPINE_LEVEL_SUBHEADING.match(text_stripped):
        return None, text_stripped

    # 1. Check for explicit spoken section markers
    for pattern, section in _SPOKEN_SECTION_MARKERS:
        m = pattern.search(text_stripped)
        if m:
            # Strip the spoken marker from the paragraph text
            remainder = text_stripped[m.end():].lstrip(' ,.:;-\n')
            if not remainder:
                # The entire paragraph was just the section name
                return section, ""
            # Capitalise the first letter of the remaining text
            remainder = remainder[0].upper() + remainder[1:] if remainder else remainder
            return section, remainder

    # 2. Check opening phrase patterns (text is not modified for these)
    for pattern in _PROCEDURE_OPENERS:
        if pattern.search(text_stripped):
            return "PROCEDURE", text_stripped

    for pattern in _CONCLUSION_OPENERS:
        if pattern.search(text_stripped):
            return "CONCLUSION", text_stripped

    for pattern in _FINDINGS_OPENERS:
        if pattern.search(text_stripped):
            return "FINDINGS", text_stripped

    # 3. Keyword scoring
    words = set(re.findall(r'[a-z]+(?:-[a-z]+)*', text_lower))
    # Also check bigrams for multi-word keywords
    bigrams = set()
    word_list = text_lower.split()
    for i in range(len(word_list) - 1):
        bigrams.add(f"{word_list[i]} {word_list[i+1]}")

    all_tokens = words | bigrams

    scores = {
        "CLINICAL HISTORY": 0,
        "PROCEDURE": 0,
        "FINDINGS": 0,
        "CONCLUSION": 0,
    }

    for kw in _CLINICAL_HISTORY_KEYWORDS:
        if kw in all_tokens:
            scores["CLINICAL HISTORY"] += 1

    for kw in _PROCEDURE_KEYWORDS:
        if kw in all_tokens:
            scores["PROCEDURE"] += 1

    for kw in _FINDINGS_KEYWORDS:
        if kw in all_tokens:
            scores["FINDINGS"] += 1

    for kw in _CONCLUSION_KEYWORDS:
        if kw in all_tokens:
            scores["CONCLUSION"] += 1

    # Get the top-scoring section
    max_score = max(scores.values())
    if max_score >= 2:
        best = max(scores, key=scores.get)
        return best, text_stripped

    return None, text_stripped


# ---------------------------------------------------------------------------
# Spoken command patterns (Australian English + general)
# Applied AFTER Deepgram's dictation mode, to catch what it misses
# ---------------------------------------------------------------------------

_SPOKEN_COMMANDS = [
    # --- Phase 1: Handle word-based commands BEFORE <\n> tokens ---
    # Full stop / stop -> period (common in AU/UK dictation)
    # Use [^\S\n]* instead of \s* to avoid eating newlines
    (re.compile(r'\bfull\s+stop\b\.?[^\S\n]*', re.IGNORECASE), '. '),
    # "Stop me" artifact (32x -- dictator says "stop" but Deepgram captures trailing "me")
    (re.compile(r'(?<=\w)[^\S\n]*[,.]?[^\S\n]*\bStop\s+me\b[^\S\n]*\.?[^\S\n]*', re.IGNORECASE), '. '),
    # "Stop" as a standalone sentence-ending command
    (re.compile(r'(?<=\w)[^\S\n]*[,.]?[^\S\n]*\bStop\b[^\S\n]*\.?[^\S\n]*', re.IGNORECASE), '. '),
    # "new line" / "next line" -> newline (backup if Deepgram misses)
    (re.compile(r'\b(?:new|next)\s+line\b\.?[^\S\n]*', re.IGNORECASE), '\n'),
    # "new paragraph" / "next paragraph" -> double newline
    (re.compile(r'\b(?:new|next)\s+paragraph\b\.?[^\S\n]*', re.IGNORECASE), '\n\n'),
    # "open bracket" / "close bracket"
    (re.compile(r'\bopen\s+(?:bracket|parenthesis)\b', re.IGNORECASE), '('),
    (re.compile(r'\bclose\s+(?:bracket|parenthesis)\b', re.IGNORECASE), ')'),
    # "semicolon"
    (re.compile(r'\bsemicolon\b', re.IGNORECASE), ';'),
    # "hyphen" / "dash"
    (re.compile(r'\b(?:hyphen|dash)\b', re.IGNORECASE), '-'),
    # "forward slash"
    (re.compile(r'\bforward\s+slash\b', re.IGNORECASE), '/'),
    # "colon"
    (re.compile(r'\bcolon\b(?!\s+(?:cancer|polyp|mass|lesion|biopsy))', re.IGNORECASE), ':'),
    # --- Phase 2: Handle <\n> tokens (Deepgram dictation mode) ---
    (re.compile(r'[^\S\n]*<\\n>[^\S\n]*<\\n>[^\S\n]*'), '\n\n'),
    (re.compile(r'[^\S\n]*<\\n>[^\S\n]*'), '\n'),
]

# Cleanup patterns applied after command substitution
_CLEANUP = [
    # Orphaned "New" from partially-processed "New line" commands
    (re.compile(r'\bNew\s+(?=[A-Z])'), '\n'),
    # Multiple periods -> single period
    (re.compile(r'\.{2,}'), '.'),
    # Period-space-period -> single period
    (re.compile(r'\.\s+\.'), '.'),
    # Space before period/comma
    (re.compile(r'\s+([.,;:!?])'), r'\1'),
    # Multiple spaces -> single space
    (re.compile(r'[ \t]{2,}'), ' '),
    # More than 2 consecutive newlines -> double newline
    (re.compile(r'\n{3,}'), '\n\n'),
    # Trailing whitespace on lines
    (re.compile(r'[ \t]+\n'), '\n'),
    # Leading whitespace on lines
    (re.compile(r'\n[ \t]+'), '\n'),
    # Capitalise first letter after period + space/newline
    (re.compile(r'(\.\s+)([a-z])'), lambda m: m.group(1) + m.group(2).upper()),
    (re.compile(r'(\n)([a-z])'), lambda m: m.group(1) + m.group(2).upper()),
    # Normalise period before newline (only strip horizontal whitespace)
    (re.compile(r'\.[ \t]*\n'), '.\n'),
]


def apply_spoken_commands(text: str) -> str:
    """Replace spoken dictation commands with their formatting equivalents."""
    for pattern, replacement in _SPOKEN_COMMANDS:
        text = pattern.sub(replacement, text)
    for pattern, replacement in _CLEANUP:
        if callable(replacement):
            text = pattern.sub(replacement, text)
        else:
            text = pattern.sub(replacement, text)
    return text.strip()


def apply_medical_corrections(text: str) -> str:
    """Apply medical term corrections learned from transcript-report comparison."""
    for pattern, replacement in _MEDICAL_CORRECTIONS:
        if callable(replacement):
            text = pattern.sub(replacement, text)
        else:
            text = pattern.sub(replacement, text)
    for pattern, replacement in _FILLER_PATTERNS:
        if callable(replacement):
            text = pattern.sub(replacement, text)
        else:
            text = pattern.sub(replacement, text)
    return text.strip()


# Modality abbreviation/name -> regex alternation for procedure echo matching
# Both abbreviations and full names map to all known forms
_MODALITY_EXPANSIONS = {
    "us": "ultrasound|us",
    "ultrasound": "ultrasound|us",
    "ct": "computed tomography|ct|cat scan",
    "mr": "magnetic resonance|mri|mr",
    "mri": "magnetic resonance|mri|mr",
    "cr": "x-ray|x ray|x-rays|radiograph|plain film|cr",
    "x-ray": "x-ray|x ray|x-rays|radiograph|plain film|cr",
    "x-rays": "x-ray|x ray|x-rays|radiograph|plain film|cr",
    "mg": "mammography|mammogram|mg",
    "mammography": "mammography|mammogram|mg",
    "nm": "nuclear medicine|nm",
    "bmd": "bone densitometry|dexa|dxa|bone density|bmd",
    "dsa": "angiography|angiogram|dsa",
}


def _strip_procedure_echo(text: str, procedure_description: str) -> str:
    """Remove the procedure description from the start of the transcript body.

    Dictators commonly begin with e.g. "Ultrasound of the abdomen" which
    duplicates the procedure title we already add as a heading. This strips that
    opening phrase (and any trailing period/comma) from the body text.

    Handles abbreviation expansion: "US ABDOMEN" matches "Ultrasound of the abdomen".
    """
    proc_lower = procedure_description.lower().strip()
    text_trimmed = text.lstrip()
    text_lower = text_trimmed.lower()

    # Try exact match at start of text
    if text_lower.startswith(proc_lower):
        remainder = text_trimmed[len(proc_lower):].lstrip(' ,.\n')
        if remainder:
            return remainder[0].upper() + remainder[1:]
        return remainder

    # Build flexible matching patterns from procedure description words
    proc_words = proc_lower.split()

    # Expand modality abbreviation if the first word is one
    expanded_first = None
    if proc_words and proc_words[0] in _MODALITY_EXPANSIONS:
        expanded_first = _MODALITY_EXPANSIONS[proc_words[0]]

    # Build regex parts for each word in the procedure description
    def _build_flex_pattern(words, first_alt=None):
        parts = []
        for i, w in enumerate(words):
            if i == 0 and first_alt:
                parts.append(r'(?:' + first_alt + r')\s+')
            elif w in ("the", "a", "an", "of"):
                parts.append(r'(?:the|a|an|of)\s+')
            else:
                parts.append(re.escape(w) + r'\s+')
        pattern_str = r'^\s*' + ''.join(parts)
        # Allow optional articles between words (e.g. "US ABDOMEN" -> "Ultrasound of the abdomen")
        pattern_str = pattern_str.rstrip(r'\s+')
        return re.compile(pattern_str + r'[,.\s]*', re.IGNORECASE)

    # Try with expanded modality name
    if expanded_first and len(proc_words) >= 2:
        # Insert optional "of the" between modality and body part
        remaining_words = proc_words[1:]
        alt_pattern = (
            r'^\s*(?:' + expanded_first + r')\s+(?:of\s+)?(?:the\s+)?'
            + r'\s+(?:of\s+)?(?:the\s+)?'.join(re.escape(w) for w in remaining_words)
            + r'[,.\s]*'
        )
        m = re.match(alt_pattern, text_trimmed, re.IGNORECASE)
        if m:
            remainder = text_trimmed[m.end():].lstrip()
            if remainder:
                return remainder[0].upper() + remainder[1:]
            return remainder

    # Try flexible match with original words
    if len(proc_words) >= 2:
        flex = _build_flex_pattern(proc_words, expanded_first)
        m = flex.match(text_trimmed)
        if m:
            remainder = text_trimmed[m.end():].lstrip()
            if remainder:
                return remainder[0].upper() + remainder[1:]
            return remainder

    return text


def add_section_headings(
    text: str,
    modality_code: str | None = None,
    procedure_description: str | None = None,
    clinical_history: str | None = None,
    doctor_id: str | None = None,
) -> str:
    """Add section headings using content-based classification.

    Uses keyword patterns learned from 15,000+ Visage radiology reports to
    classify each paragraph into the correct report section.
    Applies per-doctor heading preferences when a doctor profile exists.
    """
    # Use doctor-specific headings if available, otherwise global modality defaults
    available_headings = (
        _get_doctor_headings(doctor_id, modality_code)
        or _MODALITY_HEADINGS.get(modality_code or "", _DEFAULT_HEADINGS)
    )
    # Per-doctor heading renames (e.g. Dr. Ng uses "REPORT" instead of "FINDINGS")
    heading_map = _get_doctor_heading_map(doctor_id, modality_code)

    lines = []

    # Add procedure title
    if procedure_description:
        lines.append(procedure_description.upper())
        lines.append("")

    # If clinical history was provided from the order/referral, add it
    if clinical_history:
        lines.append("CLINICAL HISTORY")
        lines.append("")
        lines.append(clinical_history.strip())
        lines.append("")

    # Strip procedure description from transcript body to prevent duplication.
    # Dictators commonly start with "Ultrasound of the left shoulder. The findings
    # are..." -- if we've already added it as a title, remove it from the body.
    if procedure_description:
        text = _strip_procedure_echo(text, procedure_description)

    # Split transcript into paragraphs
    paragraphs = [p.strip() for p in re.split(r'\n\n+', text) if p.strip()]

    if not paragraphs:
        return "\n".join(lines) if lines else text

    # Classify each paragraph (returns section + cleaned text)
    classified = []
    for para in paragraphs:
        section, cleaned = _classify_paragraph(para, modality_code)
        if cleaned:  # Skip empty paragraphs (just a section name with no content)
            classified.append((section, cleaned))

    # Apply defaults for unclassified paragraphs using context
    # Rules:
    # 1. Unclassified paragraphs inherit from previous section, or default to FINDINGS
    # 2. Once CONCLUSION is reached, don't go backwards to earlier sections
    #    (standard report order: CLINICAL HISTORY -> PROCEDURE -> FINDINGS -> CONCLUSION)
    section_order = {"CLINICAL HISTORY": 0, "PROCEDURE": 1, "FINDINGS": 2, "CONCLUSION": 3}
    highest_section_seen = -1

    for i, (section, para) in enumerate(classified):
        if section is None:
            if i > 0 and classified[i - 1][0] is not None:
                classified[i] = (classified[i - 1][0], para)
            else:
                classified[i] = ("FINDINGS", para)
        else:
            # Prevent backwards section transitions (e.g. CONCLUSION -> FINDINGS)
            current_order = section_order.get(section, 2)
            if current_order < highest_section_seen:
                # Keep the current highest section
                prev_section = classified[i - 1][0] if i > 0 else "FINDINGS"
                classified[i] = (prev_section, para)
            else:
                highest_section_seen = max(highest_section_seen, current_order)

    # If no CLINICAL HISTORY was provided and no paragraph was classified as
    # CLINICAL HISTORY, skip that heading entirely
    has_clinical_history = clinical_history or any(
        s == "CLINICAL HISTORY" for s, _ in classified
    )

    # Determine whether to include CONCLUSION section.
    # Check doctor profile first; fall back to modality-level heuristic (CR rarely uses it).
    doctor_conclusion = _doctor_uses_conclusion(doctor_id, modality_code)
    if doctor_conclusion is False:
        # Doctor rarely uses CONCLUSION for this modality — only include if
        # the classifier explicitly detected conclusion content
        include_conclusion = any(s == "CONCLUSION" for s, _ in classified)
    elif doctor_conclusion is True:
        include_conclusion = True
    else:
        # No doctor profile — use legacy CR heuristic
        include_conclusion = modality_code != "CR" or any(
            s == "CONCLUSION" for s, _ in classified
        )

    # Build output with headings, only inserting a heading when the section changes
    current_section = None
    # Skip CLINICAL HISTORY heading if it was already added from the referral
    already_added_clinical = bool(clinical_history)

    for section, para in classified:
        # Filter: only use headings available for this modality
        if section not in available_headings:
            # Map to closest available heading
            if section == "PROCEDURE" and "PROCEDURE" not in available_headings:
                section = "FINDINGS"
            elif section == "CLINICAL HISTORY" and not has_clinical_history:
                section = "FINDINGS"

        # Suppress CONCLUSION if doctor/modality doesn't use it
        if section == "CONCLUSION" and not include_conclusion:
            section = "FINDINGS"

        if section != current_section:
            if section == "CLINICAL HISTORY" and already_added_clinical:
                # Don't repeat clinical history heading
                pass
            else:
                # Apply per-doctor heading renames
                display_heading = heading_map.get(section, section) if heading_map else section
                lines.append(display_heading)
                lines.append("")
            current_section = section

        # Ensure paragraph starts with a capital letter
        if para and para[0].islower():
            para = para[0].upper() + para[1:]
        lines.append(para)
        lines.append("")

    return "\n".join(lines).strip()


# Patterns that create inline section breaks -- "The findings are..." mid-sentence
# converts to a paragraph break so the section classifier can detect the transition
_INLINE_SECTION_BREAKS = [
    # "The findings are" / "findings are" mid-sentence (117x in transcripts)
    (re.compile(r'[.]\s*(?:the\s+)?findings?\s+are\s*[,.]?\s*', re.IGNORECASE), '.\n\n'),
    # "The procedure is" mid-sentence (25x)
    (re.compile(r'[.]\s*(?:the\s+)?procedure\s+(?:is|was)\s*[,.]?\s*', re.IGNORECASE), '.\n\n'),
    # "The conclusion is" / "conclusion:" mid-sentence
    (re.compile(r'[.]\s*(?:the\s+)?conclusion\s*(?:is|:)\s*', re.IGNORECASE), '.\n\n'),
    # NEW: "The impression is/:" mid-sentence (conclusion transition)
    (re.compile(r'[.]\s*(?:the\s+)?impression\s*(?:is|:)\s*', re.IGNORECASE), '.\n\n'),
    # NEW: "The comment is/:" mid-sentence
    (re.compile(r'[.]\s*(?:the\s+)?comment\s*(?:is|:)\s*', re.IGNORECASE), '.\n\n'),
]


def format_transcript(
    text: str,
    modality_code: str | None = None,
    procedure_description: str | None = None,
    clinical_history: str | None = None,
    doctor_id: str | None = None,
) -> str:
    """Full formatting pipeline: spoken commands -> corrections -> sections -> headings.

    When doctor_id is provided, applies per-doctor formatting preferences
    learned from retrospective analysis of Visage reports:
    - Doctor-specific word corrections (e.g. architecture -> echotexture)
    - Per-doctor section heading sequences
    - CONCLUSION inclusion/suppression based on doctor's usage patterns
    - Heading renames (e.g. FINDINGS -> REPORT for some doctors)
    """
    text = apply_spoken_commands(text)
    text = apply_medical_corrections(text)
    # Apply doctor-specific word corrections from profile
    doctor_corrections = _get_doctor_word_corrections(doctor_id, modality_code)
    for pattern, replacement in doctor_corrections:
        text = pattern.sub(replacement, text)
    # Convert inline section markers to paragraph breaks so the classifier
    # can detect section transitions that occur mid-sentence
    for pattern, replacement in _INLINE_SECTION_BREAKS:
        text = pattern.sub(replacement, text)
    text = add_section_headings(
        text,
        modality_code,
        procedure_description,
        clinical_history,
        doctor_id,
    )
    # Final cleanup pass: capitalise after periods/newlines, fix spacing
    text = re.sub(r'(\.\s+)([a-z])', lambda m: m.group(1) + m.group(2).upper(), text)
    text = re.sub(r'(\n)([a-z])', lambda m: m.group(1) + m.group(2).upper(), text)
    # Clean up orphaned periods and double spaces
    text = re.sub(r'\.\s+\.', '.', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text
