"""Continuous learning agent — discovers formatting rules from transcript-report pairs.

Compares our formatted transcriptions against Visage final reports to:
1. Build/update per-doctor formatting profiles (section structures, word corrections)
2. Discover new Deepgram mishears and spelling patterns
3. Generate a suggestions report of candidate global rules

Designed to run periodically (via CLI command + systemd timer) so the system
improves automatically as more transcriptions accumulate.
"""

import json
import logging
import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription

logger = logging.getLogger(__name__)

# Where we store learned data
_DATA_DIR = Path("/opt/crowdtrans/data")
_DATA_DIR_DEV = Path(__file__).resolve().parent.parent.parent / "data"

PROFILES_FILENAME = "doctor_profiles.json"
SUGGESTIONS_FILENAME = "learning_suggestions.json"


def _get_data_dir() -> Path:
    """Return the data directory (production or dev)."""
    if _DATA_DIR.exists():
        return _DATA_DIR
    _DATA_DIR_DEV.mkdir(parents=True, exist_ok=True)
    return _DATA_DIR_DEV


def _get_visage_connection():
    """Get a read-only connection to the Visage RIS PostgreSQL database."""
    store = get_config_store()
    sites = store.get_enabled_site_configs()
    visage = next((s for s in sites if s.ris_type == "visage"), None)
    if not visage:
        return None
    return psycopg2.connect(
        host=visage.db_host,
        port=visage.db_port,
        dbname=visage.db_name,
        user=visage.db_user,
        password=visage.db_password,
        options="-c default_transaction_read_only=on",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def _strip_html(html: str) -> str:
    """Strip HTML tags from Visage report body."""
    if not html:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── Section structure extraction ──────────────────────────────────────────

_HEADING_RE = re.compile(
    r"\b(CLINICAL HISTORY|CLINICAL INDICATION|CLINICAL DETAILS|"
    r"FINDINGS|CONCLUSION|PROCEDURE|TECHNIQUE|IMPRESSION|COMMENT|REPORT)\b"
)

# Canonical heading mapping
_HEADING_CANONICAL = {
    "CLINICAL HISTORY": "CLINICAL HISTORY",
    "CLINICAL INDICATION": "CLINICAL HISTORY",
    "CLINICAL DETAILS": "CLINICAL HISTORY",
    "FINDINGS": "FINDINGS",
    "REPORT": "REPORT",
    "CONCLUSION": "CONCLUSION",
    "IMPRESSION": "CONCLUSION",
    "COMMENT": "CONCLUSION",
    "PROCEDURE": "PROCEDURE",
    "TECHNIQUE": "TECHNIQUE",
}


def _extract_section_sequence(report_html: str) -> list[str]:
    """Extract the sequence of section headings from a Visage report."""
    plain = re.sub(r"<[^>]+>", " ", report_html)
    headings = []
    seen = set()
    for m in _HEADING_RE.finditer(plain):
        canonical = _HEADING_CANONICAL.get(m.group(1), m.group(1))
        if canonical not in seen:
            seen.add(canonical)
            headings.append(canonical)
    return headings


# ── Word-level comparison ─────────────────────────────────────────────────

_NORMALISE_RE = re.compile(
    r"^\s*(CLINICAL HISTORY|CLINICAL INDICATION|CLINICAL DETAILS|"
    r"FINDINGS|CONCLUSION|PROCEDURE|TECHNIQUE|IMPRESSION|COMMENT|REPORT)\s*$",
    re.MULTILINE,
)


def _normalise_text(text: str) -> str:
    """Normalise text for word comparison — strip headings and extra whitespace."""
    if not text:
        return ""
    text = _NORMALISE_RE.sub("", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip().lower()


def _tokenize(text: str) -> list[str]:
    """Split into words for comparison."""
    return re.findall(r"[a-z]+(?:[-'][a-z]+)*|\d+(?:\.\d+)?", text.lower())


def _find_word_replacements(
    our_tokens: list[str], report_tokens: list[str]
) -> list[tuple[str, str]]:
    """Find word-level replacements between transcript and report.

    Returns list of (transcript_word, report_word) pairs from 'replace' opcodes.
    """
    sm = SequenceMatcher(None, our_tokens, report_tokens, autojunk=False)
    replacements = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "replace":
            our_span = our_tokens[i1:i2]
            report_span = report_tokens[j1:j2]
            # Only single-word replacements for now (most reliable)
            if len(our_span) == 1 and len(report_span) == 1:
                replacements.append((our_span[0], report_span[0]))
    return replacements


# ── Core analysis ─────────────────────────────────────────────────────────

# Words to ignore in correction analysis (too common, not real corrections)
_NOISE_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "in", "at", "of", "for",
    "to", "and", "or", "on", "with", "as", "by", "it", "this", "that",
    "no", "not", "be", "has", "have", "had", "do", "does", "did",
    "so", "but", "if", "then", "than", "from", "up", "out", "about",
}


def analyze_pairs(limit: int = 0) -> dict[str, Any]:
    """Analyze all transcript-report pairs and return comprehensive results.

    Returns a dict with:
    - doctor_profiles: per-doctor, per-modality formatting data
    - global_corrections: candidate word corrections across all doctors
    - transcript_only_words: words frequent in transcripts but absent in reports
    - report_only_words: words frequent in reports but absent in transcripts
    - stats: summary statistics
    """
    # Fetch all completed Visage transcriptions
    with SessionLocal() as session:
        query = (
            session.query(Transcription)
            .filter(
                Transcription.status == "complete",
                Transcription.formatted_text.isnot(None),
                Transcription.site_id == "visage",
            )
        )
        if limit > 0:
            query = query.limit(limit)
        txns = query.all()

        # Extract data while session is open
        txn_data = []
        for txn in txns:
            txn_data.append({
                "dictation_id": txn.source_dictation_id,
                "doctor_id": str(txn.doctor_id) if txn.doctor_id else None,
                "doctor_name": txn.doctor_family_name,
                "modality_code": txn.modality_code,
                "formatted_text": txn.formatted_text,
                "procedure_description": txn.procedure_description,
            })

    if not txn_data:
        logger.warning("No completed Visage transcriptions found")
        return {"doctor_profiles": {}, "global_corrections": [], "stats": {"pairs": 0}}

    # Fetch matching Visage reports
    conn = _get_visage_connection()
    if not conn:
        logger.error("Cannot connect to Visage database")
        return {"doctor_profiles": {}, "global_corrections": [], "stats": {"pairs": 0}}

    dict_ids = [t["dictation_id"] for t in txn_data]

    try:
        with conn.cursor() as cur:
            # Fetch reports in batches to avoid huge IN clauses
            reports = {}
            batch_size = 500
            for i in range(0, len(dict_ids), batch_size):
                batch = dict_ids[i : i + batch_size]
                placeholders = ",".join(["%s"] * len(batch))
                cur.execute(
                    "SELECT d.id AS dictation_id, cd.report_body "
                    "FROM dictation d "
                    "JOIN clinical_document cd ON cd.id = d.clinical_document_id "
                    "WHERE cd.status = 'FINAL' "
                    "AND cd.report_body IS NOT NULL "
                    "AND d.id IN (" + placeholders + ")",
                    batch,
                )
                for row in cur.fetchall():
                    reports[row["dictation_id"]] = row["report_body"]
    finally:
        conn.close()

    logger.info("Fetched %d Visage reports for %d transcriptions", len(reports), len(txn_data))

    # ── Analyze each pair ──────────────────────────────────────────────
    # Per-doctor accumulators
    doctor_data = defaultdict(lambda: {
        "name": None,
        "modalities": defaultdict(lambda: {
            "count": 0,
            "section_structures": Counter(),
            "section_presence": Counter(),
            "word_corrections": Counter(),
            "similarity_sum": 0.0,
        }),
    })

    # Global accumulators
    global_corrections = Counter()  # (transcript_word, report_word) -> count
    transcript_word_freq = Counter()
    report_word_freq = Counter()
    total_pairs = 0
    total_similarity = 0.0

    for txn in txn_data:
        report_html = reports.get(txn["dictation_id"])
        if not report_html:
            continue

        report_text = _strip_html(report_html)
        our_text = txn["formatted_text"]
        doctor_id = txn["doctor_id"]
        modality = txn["modality_code"] or "UNKNOWN"

        # Strip procedure title from our text for comparison
        if txn["procedure_description"]:
            proc_upper = txn["procedure_description"].upper().strip()
            lines = our_text.split("\n")
            if lines and lines[0].strip().upper() == proc_upper:
                lines = lines[1:]
            our_text = "\n".join(lines)

        # Normalise both
        our_norm = _normalise_text(our_text)
        report_norm = _normalise_text(report_text)

        if not our_norm or not report_norm:
            continue

        total_pairs += 1

        # Tokenize
        our_tokens = _tokenize(our_norm)
        report_tokens = _tokenize(report_norm)

        # Similarity
        sm = SequenceMatcher(None, our_tokens, report_tokens, autojunk=False)
        ratio = sm.ratio()
        total_similarity += ratio

        # Word frequencies
        transcript_word_freq.update(set(our_tokens))
        report_word_freq.update(set(report_tokens))

        # Word replacements
        replacements = _find_word_replacements(our_tokens, report_tokens)
        for wrong, right in replacements:
            if wrong in _NOISE_WORDS or right in _NOISE_WORDS:
                continue
            if wrong == right:
                continue
            if len(wrong) <= 1 or len(right) <= 1:
                continue
            global_corrections[(wrong, right)] += 1

        # Section structure from report
        sections = _extract_section_sequence(report_html)
        section_seq = " > ".join(sections) if sections else "(none)"

        # Per-doctor accumulation
        if doctor_id:
            doc = doctor_data[doctor_id]
            doc["name"] = txn["doctor_name"]
            mod = doc["modalities"][modality]
            mod["count"] += 1
            mod["section_structures"][section_seq] += 1
            for s in sections:
                mod["section_presence"][s] += 1
            mod["similarity_sum"] += ratio
            for wrong, right in replacements:
                if wrong in _NOISE_WORDS or right in _NOISE_WORDS:
                    continue
                if wrong == right or len(wrong) <= 1 or len(right) <= 1:
                    continue
                mod["word_corrections"][(wrong, right)] += 1

    # ── Build doctor profiles ──────────────────────────────────────────
    profiles = {}
    for doctor_id, data in doctor_data.items():
        profile = {
            "doctor_name": data["name"],
            "modalities": {},
        }
        for mod_code, mod_data in data["modalities"].items():
            count = mod_data["count"]
            if count < 3:
                continue  # Not enough data

            # Section presence percentages
            presence_pct = {}
            for section, cnt in mod_data["section_presence"].items():
                presence_pct[section] = round(cnt / count * 100, 1)

            # Top word corrections (count >= 2)
            corrections = [
                [wrong, right, cnt]
                for (wrong, right), cnt in mod_data["word_corrections"].most_common(100)
                if cnt >= 2
            ]

            profile["modalities"][mod_code] = {
                "count": count,
                "avg_similarity": round(mod_data["similarity_sum"] / count * 100, 1),
                "section_structure": dict(mod_data["section_structures"].most_common(10)),
                "section_presence_pct": presence_pct,
                "word_corrections": corrections,
            }

        if profile["modalities"]:
            profiles[doctor_id] = profile

    # ── Build global correction candidates ─────────────────────────────
    # Filter to corrections that appear 3+ times and aren't noise
    significant_corrections = [
        {"transcript": wrong, "report": right, "count": cnt}
        for (wrong, right), cnt in global_corrections.most_common(200)
        if cnt >= 3
        and wrong not in _NOISE_WORDS
        and right not in _NOISE_WORDS
        and wrong != right
    ]

    # ── Find transcript-only and report-only words ─────────────────────
    transcript_only = []
    for word, t_count in transcript_word_freq.most_common(500):
        r_count = report_word_freq.get(word, 0)
        if t_count >= 5 and r_count <= max(1, t_count * 0.05):
            transcript_only.append({
                "word": word,
                "transcript_count": t_count,
                "report_count": r_count,
            })

    report_only = []
    for word, r_count in report_word_freq.most_common(500):
        t_count = transcript_word_freq.get(word, 0)
        if r_count >= 5 and t_count <= max(1, r_count * 0.05):
            report_only.append({
                "word": word,
                "report_count": r_count,
                "transcript_count": t_count,
            })

    avg_sim = (total_similarity / total_pairs * 100) if total_pairs else 0

    return {
        "doctor_profiles": profiles,
        "global_corrections": significant_corrections[:100],
        "transcript_only_words": transcript_only[:50],
        "report_only_words": report_only[:50],
        "stats": {
            "pairs": total_pairs,
            "avg_similarity": round(avg_sim, 1),
            "doctors": len(profiles),
            "correction_candidates": len(significant_corrections),
        },
    }


# ── File output ───────────────────────────────────────────────────────────


def save_profiles(profiles: dict, path: Path | None = None) -> Path:
    """Save doctor profiles to JSON file."""
    if path is None:
        path = _get_data_dir() / PROFILES_FILENAME
    path.write_text(json.dumps(profiles, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Saved %d doctor profiles to %s", len(profiles), path)
    return path


def save_suggestions(results: dict, path: Path | None = None) -> Path:
    """Save learning suggestions (corrections, patterns) to JSON file."""
    if path is None:
        path = _get_data_dir() / SUGGESTIONS_FILENAME
    suggestions = {
        "stats": results["stats"],
        "global_corrections": results["global_corrections"],
        "transcript_only_words": results["transcript_only_words"],
        "report_only_words": results["report_only_words"],
    }
    path.write_text(json.dumps(suggestions, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Saved learning suggestions to %s", path)
    return path


def run_learning(limit: int = 0, reformat: bool = True) -> dict[str, Any]:
    """Run the full learning pipeline.

    1. Analyze all transcript-report pairs
    2. Update doctor_profiles.json
    3. Save suggestions for review
    4. Optionally reformat all transcriptions with updated profiles

    Returns the analysis results dict.
    """
    logger.info("Starting learning analysis...")
    results = analyze_pairs(limit=limit)

    if results["stats"]["pairs"] == 0:
        logger.warning("No pairs to analyze, skipping profile update")
        return results

    logger.info(
        "Analyzed %d pairs: %.1f%% avg similarity, %d doctors, %d correction candidates",
        results["stats"]["pairs"],
        results["stats"]["avg_similarity"],
        results["stats"]["doctors"],
        results["stats"]["correction_candidates"],
    )

    # Save profiles
    save_profiles(results["doctor_profiles"])

    # Save suggestions
    save_suggestions(results)

    # Force reload of profiles in formatter
    from crowdtrans.transcriber import formatter
    formatter._DOCTOR_PROFILES = None

    # Reformat all transcriptions with updated profiles
    if reformat:
        from crowdtrans.transcriber.formatter import format_transcript

        logger.info("Reformatting transcriptions with updated profiles...")
        with SessionLocal() as session:
            txns = (
                session.query(Transcription)
                .filter(
                    Transcription.status == "complete",
                    Transcription.transcript_text.isnot(None),
                )
                .all()
            )
            for i, txn in enumerate(txns, 1):
                txn.formatted_text = format_transcript(
                    txn.transcript_text,
                    modality_code=txn.modality_code,
                    procedure_description=txn.procedure_description,
                    clinical_history=txn.complaint,
                    doctor_id=txn.doctor_id,
                )
                if i % 500 == 0:
                    session.commit()
                    logger.info("  Reformatted %d/%d", i, len(txns))
            session.commit()
            logger.info("Reformatted %d transcriptions", len(txns))

    # Log top suggestions
    if results["global_corrections"]:
        logger.info("Top correction candidates:")
        for c in results["global_corrections"][:10]:
            logger.info(
                "  %s -> %s (%dx)",
                c["transcript"], c["report"], c["count"],
            )

    if results["transcript_only_words"]:
        logger.info("Top transcript-only words (possible fillers/mishears):")
        for w in results["transcript_only_words"][:10]:
            logger.info(
                "  '%s' (%dx in transcripts, %dx in reports)",
                w["word"], w["transcript_count"], w["report_count"],
            )

    return results
