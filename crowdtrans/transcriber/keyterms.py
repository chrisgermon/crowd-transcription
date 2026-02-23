"""Radiology keyterms for Deepgram keyword boosting."""

# Always included regardless of modality
BASE_TERMS = [
    "radiology", "radiologist", "impression", "findings", "conclusion",
    "clinical history", "comparison", "technique", "indication",
    "unremarkable", "within normal limits", "no acute abnormality",
    "no significant abnormality", "stable", "unchanged", "interval",
    "bilateral", "unilateral", "ipsilateral", "contralateral",
    "anterior", "posterior", "superior", "inferior", "lateral", "medial",
    "proximal", "distal", "periosteal", "parenchymal", "subchondral",
    "degenerative", "atherosclerotic", "calcification", "effusion",
    "consolidation", "atelectasis", "opacity", "lucency",
    "lymphadenopathy", "hepatomegaly", "splenomegaly",
    "cardiomegaly", "pneumothorax", "pleural effusion",
    "no drainable collection", "correlate clinically",
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
    ],
    "CT": [
        "computed tomography", "Hounsfield units", "contrast enhancement",
        "arterial phase", "portal venous phase", "delayed phase",
        "non-contrast", "post-contrast", "axial", "coronal", "sagittal",
        "multiplanar reconstruction", "pulmonary embolism",
        "ground glass opacity", "tree-in-bud", "mosaic attenuation",
        "herniation", "stenosis", "aneurysm", "dissection",
        "appendicitis", "diverticulitis", "bowel obstruction",
        "hepatic steatosis", "adrenal adenoma",
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
    ],
    "CR": [
        "radiograph", "X-ray", "radiolucent", "radiopaque",
        "cortical", "trabecular", "joint space", "osteophyte",
        "fracture", "dislocation", "subluxation", "alignment",
        "cardiomediastinal silhouette", "costophrenic angle",
        "lung fields", "hilar", "mediastinal", "trachea",
        "soft tissues", "prosthesis", "hardware",
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
) -> list[str]:
    """Build a keyterm list for a specific study, capped at 100."""
    terms = list(BASE_TERMS)

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

    # Deduplicate while preserving order, cap at 100
    seen = set()
    unique = []
    for t in terms:
        key = t.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(t)
    return unique[:100]
