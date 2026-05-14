"""Radiology keyterms for Deepgram keyword boosting.

Terms ranked by frequency from analysis of 15,000+ Visage reports.
High-frequency terms get priority within Deepgram's 100-keyterm limit.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_CUSTOM_KEYTERMS: list[str] | None = None
_CUSTOM_KEYTERMS_PATHS = [
    Path(__file__).resolve().parent.parent.parent / "data" / "custom_corrections.json",
    Path("/opt/crowdtrans/data/custom_corrections.json"),
]


_KARISMA_DICT: list[str] | None = None
_KARISMA_DICT_PATHS = [
    Path(__file__).resolve().parent.parent.parent / "data" / "karisma_dictionary.json",
    Path("/opt/crowdtrans/data/karisma_dictionary.json"),
]

_DOCTOR_PROFILES: dict | None = None
_DOCTOR_PROFILES_MTIME: float | None = None
_DOCTOR_PROFILES_PATHS = [
    Path(__file__).resolve().parent.parent.parent / "data" / "doctor_profiles.json",
    Path("/opt/crowdtrans/data/doctor_profiles.json"),
]

# Words too generic to boost — boosting these hurts more than helps because
# Deepgram already gets them right and the keyterm slot is wasted.
_DOCTOR_KEYTERM_STOPWORDS = {
    "right", "left", "both", "the", "a", "an", "and", "or", "of", "to", "in",
    "on", "at", "is", "no", "not", "with", "without", "for", "from", "by",
    "this", "that", "there", "are", "was", "were", "has", "have", "had",
    "?", "??", "???",
}


def _is_valid_keyterm(term: str) -> bool:
    """Reject sentences/macros stored as 'replacements' — Deepgram keyterms expect words or short phrases."""
    if not term:
        return False
    t = term.strip()
    if len(t) < 3 or len(t) > 60:
        return False
    # Macro expansions tend to be sentences ending with terminal punctuation.
    if t[-1] in ".!?":
        return False
    if t.count(",") > 0:
        return False
    if len(t.split()) > 4:
        return False
    return True


def _load_karisma_dictionary() -> list[str]:
    """Load Karisma medical dictionary words (cached). Used as additional keyterm pool."""
    global _KARISMA_DICT
    if _KARISMA_DICT is not None:
        return _KARISMA_DICT
    _KARISMA_DICT = []
    for path in _KARISMA_DICT_PATHS:
        if path.exists():
            try:
                _KARISMA_DICT = json.loads(path.read_text(encoding="utf-8"))
                logger.info("Loaded %d Karisma dictionary terms from %s", len(_KARISMA_DICT), path)
                break
            except Exception:
                pass
    return _KARISMA_DICT


def sync_karisma_dictionary():
    """Fetch medical dictionary from Karisma and save locally for keyterm use."""
    from crowdtrans.config_store import get_config_store
    store = get_config_store()
    sites = store.get_enabled_site_configs()
    karisma_sites = [s for s in sites if s.ris_type == "karisma"]
    if not karisma_sites:
        logger.warning("No Karisma site configured — cannot sync dictionary")
        return 0

    from crowdtrans.karisma import fetch_medical_dictionary
    words = fetch_medical_dictionary(karisma_sites[0])
    if not words:
        return 0

    # Save to local file
    out_path = _KARISMA_DICT_PATHS[0]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(words, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Synced %d Karisma dictionary terms to %s", len(words), out_path)

    # Reset cache
    global _KARISMA_DICT
    _KARISMA_DICT = None
    return len(words)


def _load_doctor_profiles() -> dict:
    """Load doctor profile JSON (cached, auto-reloads when file mtime changes)."""
    global _DOCTOR_PROFILES, _DOCTOR_PROFILES_MTIME
    for path in _DOCTOR_PROFILES_PATHS:
        if path.exists():
            try:
                mtime = path.stat().st_mtime
                if _DOCTOR_PROFILES is not None and mtime == _DOCTOR_PROFILES_MTIME:
                    return _DOCTOR_PROFILES
                _DOCTOR_PROFILES = json.loads(path.read_text(encoding="utf-8"))
                _DOCTOR_PROFILES_MTIME = mtime
                return _DOCTOR_PROFILES
            except Exception as e:
                logger.warning("Failed to load doctor profiles from %s: %s", path, e)
    if _DOCTOR_PROFILES is None:
        _DOCTOR_PROFILES = {}
    return _DOCTOR_PROFILES


def _modality_to_profile_group(modality_code: str | None) -> str | None:
    """Map fine-grained modality codes to the buckets used in doctor_profiles.json."""
    if not modality_code:
        return None
    code = modality_code.upper()
    # The learner groups everything as "RAD" today; future-proof with passthrough.
    if code in {"CR", "DX", "XA", "RF"}:
        return "RAD"
    return code


def _doctor_profile_keyterms(doctor_id: str | None, modality_code: str | None, limit: int = 25) -> list[str]:
    """Top replacement words this doctor uses (i.e. words Deepgram has historically mis-heard for them)."""
    if not doctor_id:
        return []
    profiles = _load_doctor_profiles()
    profile = profiles.get(doctor_id) or profiles.get(doctor_id.upper())
    if not profile:
        return []
    modalities = profile.get("modalities", {})

    # Try the matched modality bucket first, then fall back to all modalities for this doctor.
    candidate_corrections: list[tuple[str, int]] = []
    group = _modality_to_profile_group(modality_code)
    if group and group in modalities:
        for entry in modalities[group].get("word_corrections", []):
            if len(entry) >= 3:
                candidate_corrections.append((entry[1], entry[2]))
    else:
        for mod_data in modalities.values():
            for entry in mod_data.get("word_corrections", []):
                if len(entry) >= 3:
                    candidate_corrections.append((entry[1], entry[2]))

    candidate_corrections.sort(key=lambda x: -x[1])
    out: list[str] = []
    seen: set[str] = set()
    for word, _count in candidate_corrections:
        w = (word or "").strip()
        wl = w.lower()
        if wl in _DOCTOR_KEYTERM_STOPWORDS or wl in seen:
            continue
        if not _is_valid_keyterm(w):
            continue
        seen.add(wl)
        out.append(w)
        if len(out) >= limit:
            break
    return out


def _doctor_db_keyterms(doctor_id: str | None, limit: int = 15) -> list[str]:
    """Replacement targets from per-doctor word_replacements DB rows."""
    if not doctor_id:
        return []
    try:
        from crowdtrans.database import SessionLocal
        from crowdtrans.models import WordReplacement
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        with SessionLocal() as session:
            rows = (
                session.query(WordReplacement.replacement)
                .filter(WordReplacement.doctor_id == doctor_id)
                .filter(WordReplacement.enabled.is_(True))
                .all()
            )
            for (repl,) in rows:
                r = (repl or "").strip()
                rl = r.lower()
                if rl in _DOCTOR_KEYTERM_STOPWORDS or rl in seen:
                    continue
                if not _is_valid_keyterm(r):
                    continue
                seen.add(rl)
                out.append(r)
                if len(out) >= limit:
                    break
    except Exception as e:
        logger.warning("Failed to load doctor word_replacements for %s: %s", doctor_id, e)
    return out


def _load_custom_keyterms() -> list[str]:
    """Load user-defined keyterms from custom_corrections.json. Cached."""
    global _CUSTOM_KEYTERMS
    if _CUSTOM_KEYTERMS is not None:
        return _CUSTOM_KEYTERMS
    _CUSTOM_KEYTERMS = []
    for path in _CUSTOM_KEYTERMS_PATHS:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                _CUSTOM_KEYTERMS = data.get("keyterms", [])
                break
            except Exception:
                pass
    return _CUSTOM_KEYTERMS


# Always included regardless of modality
BASE_TERMS = [
    "radiology", "radiologist", "impression", "findings", "conclusion",
    "clinical history", "comparison", "technique", "indication",
    "unremarkable", "within normal limits", "no acute abnormality",
    "no significant abnormality", "stable", "unchanged", "interval",
    "bilateral", "unilateral", "ipsilateral", "contralateral",
    "anterior", "posterior", "superior", "inferior", "lateral", "medial",
    "proximal", "distal", "periosteal", "parenchymal", "subchondral",
    "degenerative", "degeneration", "atherosclerotic", "calcification", "effusion",
    "consolidation", "atelectasis", "opacity", "lucency",
    "lymphadenopathy", "hepatomegaly", "splenomegaly",
    "cardiomegaly", "pneumothorax", "pleural effusion",
    "no drainable collection", "correlate clinically",
    # High-frequency terms from 15,000+ report analysis
    "impingement", "tendinopathy", "subacromial", "bursitis", "hyperaemia",
    "pathology", "bulging", "hypertrophy", "adenopathy", "glenohumeral",
    "echotexture", "ganglion", "sacroiliac", "trochanteric", "plantar",
    "endplate", "rotator cuff", "contour", "anterolisthesis",
    # Common multi-word medical phrases (>100 occurrences in 15K reports)
    "disc bulging", "facet hypertrophy", "foraminal narrowing",
    "foraminal stenosis", "neural foraminal", "disc desiccation",
    "no abnormality", "no significant", "normal appearance",
]

MODALITY_TERMS: dict[str, list[str]] = {
    "US": [
        "ultrasound", "sonographic", "echogenicity", "anechoic", "hyperechoic",
        "hypoechoic", "isoechoic", "heterogeneous", "homogeneous",
        "Doppler", "colour Doppler", "spectral Doppler", "resistive index",
        "transducer", "acoustic shadowing", "posterior enhancement",
        "gallbladder", "common bile duct", "intrahepatic ducts",
        "portal vein", "hepatic vein", "aorta", "IVC",
        "hydronephrosis", "renal cortex", "thyroid nodule", "TIRADS",
        # Musculoskeletal US (most common US exams in this practice)
        "subacromial bursa", "subdeltoid", "rotator cuff", "supraspinatus",
        "infraspinatus", "subscapularis", "biceps tendon", "glenohumeral",
        "bunching", "capsulitis", "volar plate", "collateral ligament",
        "common extensor", "common flexor", "triscaphe",
        # High-frequency MSK US terms from 15K analysis
        "plantar fasciitis", "plantar fascia", "Achilles tendon",
        "Baker's cyst", "de Quervain's", "greater trochanter",
        "Morton's neuroma", "carpal tunnel", "trigger finger",
        "lateral epicondylitis", "medial epicondylitis",
        "superficial vein thrombosis", "deep venous system",
        # Obstetric/gynae US
        "intrauterine", "foetal", "crown-rump", "endometrial", "endometrium",
        "myometrium", "adnexal", "subchorionic", "follicles",
        "endometrial thickness", "both ovaries",
        # Abdominal US
        "biliary system", "abdominal aorta", "corticomedullary",
        "corticomedullary differentiation", "echotexture",
        # Vascular US
        "antegrade", "haemodynamic", "thrombosis",
        "saphenofemoral junction", "sapheno-femoral",
        "incompetent", "incompetence", "reflux",
    ],
    "CT": [
        "computed tomography", "Hounsfield units", "contrast enhancement",
        "arterial phase", "portal venous phase", "delayed phase",
        "non-contrast", "post-contrast", "axial", "coronal", "sagittal",
        "multiplanar reconstruction", "pulmonary embolism",
        "ground-glass opacity", "tree-in-bud", "mosaic attenuation",
        "herniation", "stenosis", "aneurysm", "dissection",
        "appendicitis", "diverticulitis", "bowel obstruction",
        "hepatic steatosis", "adrenal adenoma",
        # Spine CT (common in this practice)
        "anterolisthesis", "spondylolisthesis", "thoracolumbar",
        "demineralisation", "facet hypertrophy",
        "foraminal narrowing", "central canal stenosis",
        "disc bulging", "disc protrusion",
        # Sinonasal CT
        "sinonasal", "polyps", "mucosal thickening",
        # CT guidance procedures
        "under CT guidance", "aseptic technique",
        "Celestone", "informed consent",
    ],
    "MR": [
        "magnetic resonance", "T1-weighted", "T2-weighted", "FLAIR",
        "diffusion-weighted", "ADC map", "post-gadolinium",
        "disc desiccation", "disc protrusion", "disc extrusion",
        "annular fissure", "neural foraminal stenosis", "spinal stenosis",
        "ligamentum flavum", "meniscal tear", "cruciate ligament",
        "rotator cuff", "labral tear", "bone marrow oedema",
        "chondromalacia", "synovitis", "tendinopathy",
        "signal abnormality", "enhancement pattern",
        "foraminal narrowing", "central canal stenosis",
        "facet hypertrophy", "disc bulging",
    ],
    "CR": [
        "radiograph", "X-ray", "x-rays", "radiolucent", "radiopaque",
        "cortical", "trabecular", "joint space", "osteophyte",
        "fracture", "dislocation", "subluxation", "alignment",
        "cardiomediastinal silhouette", "cardiomediastinum",
        "costophrenic angle",
        "lung fields", "hilar", "mediastinal", "trachea",
        "soft tissues", "prosthesis", "hardware",
        "endplate", "degeneration", "scoliosis",
        "no acute bony abnormality", "degenerative changes",
    ],
    "MG": [
        "mammography", "mammographic", "BI-RADS", "breast density",
        "microcalcifications", "architectural distortion", "mass",
        "asymmetry", "skin thickening", "axillary lymph node",
        "craniocaudal", "mediolateral oblique", "spot compression",
        "tomosynthesis", "screening", "diagnostic",
    ],
    "NM": [
        "nuclear medicine", "scintigraphy", "radiotracer", "uptake",
        "photopenia", "hot spot", "cold spot", "biodistribution",
        "bone scan", "thyroid scan", "renal scan", "DTPA", "MAG3",
        "DMSA", "ventilation perfusion", "V/Q scan",
    ],
    "BMD": [
        "bone densitometry", "DEXA", "DXA", "T-score", "Z-score",
        "osteoporosis", "osteopenia", "bone mineral density",
        "lumbar spine", "femoral neck", "total hip",
        "fracture risk", "FRAX",
    ],
    "SCR": [
        "screening", "mammographic screening", "BI-RADS",
        "recall", "interval cancer",
    ],
    "DSA": [
        "digital subtraction angiography", "angiogram", "catheter",
        "stenosis", "occlusion", "collateral", "embolisation",
        "fluoroscopy", "contrast injection",
    ],
    "CONS": [
        "consultation", "multidisciplinary", "clinical correlation",
        "recommend", "suggest", "advise",
    ],
}


def get_keyterms(
    modality_code: str | None = None,
    patient_name_parts: list[str] | None = None,
    doctor_name: str | None = None,
    referrer_name: str | None = None,
    procedure_description: str | None = None,
    doctor_id: str | None = None,
) -> list[str]:
    """Build a keyterm list for a specific study, capped at 100.

    Per-doctor terms (from learned profile + word_replacements) are inserted
    after modality-specific terms so they survive the 100-term cap.
    """
    terms = list(BASE_TERMS)

    # Per-doctor terms come BEFORE the modality block so they survive the 100-term cap.
    # These are words historically mis-heard by Deepgram for this specific doctor —
    # the highest-signal boost we have, second only to the always-on BASE_TERMS.
    terms.extend(_doctor_db_keyterms(doctor_id))
    terms.extend(_doctor_profile_keyterms(doctor_id, modality_code))

    # Add modality-specific terms
    if modality_code and modality_code in MODALITY_TERMS:
        terms.extend(MODALITY_TERMS[modality_code])

    # Context boosting: names and procedure
    context_terms = []
    if patient_name_parts:
        context_terms.extend([p for p in patient_name_parts if len(p) > 2])
    if doctor_name:
        context_terms.append(doctor_name)
    if referrer_name:
        context_terms.append(referrer_name)
    if procedure_description:
        # Add significant words from procedure description
        for word in procedure_description.split():
            if len(word) > 3 and word.lower() not in {"with", "without", "left", "right", "both"}:
                context_terms.append(word)

    terms.extend(context_terms)

    # Add user-defined custom keyterms
    terms.extend(_load_custom_keyterms())

    # Add relevant terms from Karisma medical dictionary
    # Filter by procedure description words to stay within the 100-term cap
    karisma_dict = _load_karisma_dictionary()
    if karisma_dict and procedure_description:
        proc_words = {w.lower() for w in procedure_description.split() if len(w) > 3}
        for term in karisma_dict:
            term_lower = term.lower()
            # Include dictionary terms that share a root with procedure words
            if any(pw in term_lower or term_lower in pw for pw in proc_words):
                terms.append(term)

    # Deduplicate while preserving order, cap at 100
    seen = set()
    unique = []
    for t in terms:
        key = t.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique[:100]
