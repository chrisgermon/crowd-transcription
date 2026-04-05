"""Compare CrowdScription transcriptions against Karisma typed reports."""

import logging
import re
import time
from difflib import SequenceMatcher
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy.exc import OperationalError

from sqlalchemy.orm import defer

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription
from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/compare")

PAGE_SIZE = 25

# ── Karisma report fetching ─────────────────────────────────────────────


def _fetch_karisma_reports(transaction_keys: list[int]) -> dict[int, str]:
    """Fetch typed report text for a batch of Karisma TransactionKeys."""
    if not transaction_keys:
        return {}
    store = get_config_store()
    sites = store.get_enabled_site_configs()
    karisma = next((s for s in sites if s.ris_type == "karisma"), None)
    if not karisma:
        return {}
    from crowdtrans.karisma import fetch_reports
    return fetch_reports(karisma, transaction_keys)


# ── Text normalisation for comparison ───────────────────────────────────

# Section headings to strip (these appear in both Visage and our output
# but may be formatted differently — not actual content differences)
_HEADING_PATTERN = re.compile(
    r"^\s*(CLINICAL HISTORY|CLINICAL INDICATION|CLINICAL DETAILS|"
    r"FINDINGS|CONCLUSION|PROCEDURE|TECHNIQUE|IMPRESSION|COMMENT|"
    r"OPINION|REPORT)\s*$",
    re.MULTILINE,
)


def _normalise_for_compare(text: str, procedure_description: str | None = None) -> str:
    """Normalise text for comparison by stripping structural elements.

    Removes section headings and procedure title lines so the diff focuses
    on actual report content rather than formatting differences.
    """
    if not text:
        return ""
    # Strip procedure title (usually first line in our formatted output)
    if procedure_description:
        proc_upper = procedure_description.upper().strip()
        lines = text.split("\n")
        if lines and lines[0].strip().upper() == proc_upper:
            lines = lines[1:]
        text = "\n".join(lines)
    # Strip section heading lines
    text = _HEADING_PATTERN.sub("", text)
    # Normalise whitespace
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()


# ── Diff computation ────────────────────────────────────────────────────


def _tokenize(text: str) -> list[str]:
    """Split text into tokens (words + punctuation) preserving newlines."""
    tokens = []
    for line in text.split("\n"):
        if tokens:
            tokens.append("\n")
        words = re.findall(r"\S+", line)
        tokens.extend(words)
    return tokens


def _compute_word_diff(our_text: str, visage_text: str) -> list[dict[str, Any]]:
    """Compute word-level diff between our formatted text and Visage report.

    Returns a list of diff segments:
      {"type": "equal"|"insert"|"delete"|"replace", "our": str, "visage": str}

    - equal:   text matches
    - delete:  text in our transcript but NOT in visage report (we have extra)
    - insert:  text in visage report but NOT in our transcript (we're missing)
    - replace: text differs between both
    """
    our_tokens = _tokenize(our_text or "")
    visage_tokens = _tokenize(visage_text or "")

    sm = SequenceMatcher(None, our_tokens, visage_tokens, autojunk=False)
    segments = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        our_chunk = " ".join(our_tokens[i1:i2]).replace(" \n ", "\n").replace(" \n", "\n").replace("\n ", "\n")
        visage_chunk = " ".join(visage_tokens[j1:j2]).replace(" \n ", "\n").replace(" \n", "\n").replace("\n ", "\n")
        segments.append({
            "type": tag,
            "our": our_chunk,
            "visage": visage_chunk,
        })

    return segments


def _similarity_ratio(our_text: str, visage_text: str) -> float:
    """Return 0.0–1.0 similarity ratio between two texts."""
    if not our_text and not visage_text:
        return 1.0
    if not our_text or not visage_text:
        return 0.0
    our_tokens = _tokenize(our_text)
    visage_tokens = _tokenize(visage_text)
    sm = SequenceMatcher(None, our_tokens, visage_tokens, autojunk=False)
    return sm.ratio()


# ── Routes ──────────────────────────────────────────────────────────────


@router.get("/")
def compare_list(
    request: Request,
    modality: str = Query("", description="Filter by modality"),
    worksite: str = Query("", description="Filter by worksite"),
    doctor: str = Query("", description="Filter by doctor"),
    sort: str = Query("similarity", description="Sort by: similarity, date, id"),
    page: int = Query(1, ge=1),
):
    """List transcriptions with their matching Karisma reports and similarity scores."""
    for attempt in range(3):
        try:
            return _compare_list_impl(request, modality, worksite, doctor, sort, page)
        except OperationalError as e:
            logger.warning("SQLite error on compare list (attempt %d): %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(1)
            else:
                raise HTTPException(status_code=503, detail="Database temporarily unavailable, please retry")


def _compare_list_impl(request, modality, worksite, doctor, sort, page):
    with SessionLocal() as session:
        # Base query with heavy columns deferred
        base_query = (
            session.query(Transcription)
            .options(
                defer(Transcription.words_json),
                defer(Transcription.paragraphs_json),
                defer(Transcription.transcript_text),
            )
            .filter(
                Transcription.status == "complete",
                Transcription.formatted_text.isnot(None),
                Transcription.site_id == "karisma",
            )
        )
        if modality:
            base_query = base_query.filter(Transcription.modality_code == modality)
        if worksite:
            base_query = base_query.filter(Transcription.facility_name == worksite)
        if doctor:
            base_query = base_query.filter(Transcription.doctor_family_name.ilike(f"%{doctor}%"))

        # Get total count (lightweight)
        total_count = base_query.count()

        # Paginate at database level — sort by date by default
        # (similarity sort requires loading all rows, so we only support it
        # for small result sets)
        offset = (page - 1) * PAGE_SIZE
        page_txns = (
            base_query.order_by(Transcription.dictation_date.desc().nullslast())
            .offset(offset)
            .limit(PAGE_SIZE)
            .all()
        )

        # Fetch Visage reports only for the current page
        dict_ids = [t.source_dictation_id for t in page_txns]
        visage_reports = _fetch_karisma_reports(dict_ids)

        # Build comparison items for this page
        items = []
        for txn in page_txns:
            visage_text = visage_reports.get(txn.source_dictation_id)
            if visage_text is None:
                continue
            our_norm = _normalise_for_compare(txn.formatted_text, txn.procedure_description)
            visage_norm = _normalise_for_compare(visage_text)
            ratio = _similarity_ratio(our_norm, visage_norm)
            llm_ratio = None
            if txn.llm_formatted_text:
                llm_norm = _normalise_for_compare(txn.llm_formatted_text, txn.procedure_description)
                llm_ratio = _similarity_ratio(llm_norm, visage_norm)
            items.append({
                "txn": txn,
                "similarity": ratio,
                "llm_similarity": llm_ratio,
                "visage_preview": visage_text[:150] + "..." if len(visage_text) > 150 else visage_text,
                "our_preview": (txn.formatted_text[:150] + "...") if len(txn.formatted_text) > 150 else txn.formatted_text,
            })

        # Sort within page if requested
        if sort == "similarity":
            items.sort(key=lambda x: x["similarity"])

        # Stats (approximate — based on total count, not matched count)
        total_matched = total_count
        avg_similarity = (
            sum(x["similarity"] for x in items) / len(items) * 100
            if items else 0
        )

        total_pages = max(1, (total_matched + PAGE_SIZE - 1) // PAGE_SIZE)

        # Filter dropdowns
        modalities = [
            r[0] for r in session.query(Transcription.modality_code)
            .filter(Transcription.modality_code.isnot(None), Transcription.site_id == "karisma")
            .distinct()
            .order_by(Transcription.modality_code)
            .all()
        ]
        worksites = [
            r[0] for r in session.query(Transcription.facility_name)
            .filter(Transcription.facility_name.isnot(None), Transcription.site_id == "karisma")
            .distinct()
            .order_by(Transcription.facility_name)
            .all()
        ]

    return templates.TemplateResponse("compare/list.html", {
        "request": request,
        "items": items,
        "total_matched": total_matched,
        "avg_similarity": avg_similarity,
        "page": page,
        "total_pages": total_pages,
        "modality": modality,
        "worksite": worksite,
        "doctor": doctor,
        "sort": sort,
        "modalities": modalities,
        "worksites": worksites,
    })


@router.get("/{transcription_id}")
def compare_detail(request: Request, transcription_id: int):
    """Side-by-side diff of a single transcription vs Visage report."""
    for attempt in range(3):
        try:
            return _compare_detail_impl(request, transcription_id)
        except OperationalError as e:
            logger.warning("SQLite error on compare detail (attempt %d): %s", attempt + 1, e)
            if attempt < 2:
                time.sleep(1)
            else:
                raise HTTPException(status_code=503, detail="Database temporarily unavailable, please retry")


def _compare_detail_impl(request, transcription_id):
    with SessionLocal() as session:
        txn = (
            session.query(Transcription)
            .options(
                defer(Transcription.words_json),
                defer(Transcription.paragraphs_json),
            )
            .filter_by(id=transcription_id)
            .first()
        )
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")

        # Fetch matching Visage report
        visage_reports = _fetch_karisma_reports([txn.source_dictation_id])
        visage_text = visage_reports.get(txn.source_dictation_id)

        if visage_text is None:
            raise HTTPException(status_code=404, detail="No matching Visage report found")

        # Normalise both texts for fair comparison (strip headings/titles)
        our_norm = _normalise_for_compare(txn.formatted_text, txn.procedure_description)
        visage_norm = _normalise_for_compare(visage_text)

        # Compute diff on normalised text
        diff_segments = _compute_word_diff(our_norm, visage_norm)
        similarity = _similarity_ratio(our_norm, visage_norm)

        # LLM similarity (when LLM output exists)
        llm_similarity = None
        llm_diff_segments = None
        if txn.llm_formatted_text:
            llm_norm = _normalise_for_compare(txn.llm_formatted_text, txn.procedure_description)
            llm_similarity = _similarity_ratio(llm_norm, visage_norm)
            llm_diff_segments = _compute_word_diff(llm_norm, visage_norm)

        # Count differences by type
        diff_stats = {"equal": 0, "insert": 0, "delete": 0, "replace": 0}
        for seg in diff_segments:
            word_count = max(
                len(seg["our"].split()) if seg["our"] else 0,
                len(seg["visage"].split()) if seg["visage"] else 0,
            )
            diff_stats[seg["type"]] += word_count

        site_cfg = get_config_store().get_site(txn.site_id)
        site_name = site_cfg.site_name if site_cfg else txn.site_id

    return templates.TemplateResponse("compare/detail.html", {
        "request": request,
        "txn": txn,
        "site_name": site_name,
        "visage_text": visage_text,
        "our_normalised": our_norm,
        "visage_normalised": visage_norm,
        "diff_segments": diff_segments,
        "similarity": similarity,
        "llm_similarity": llm_similarity,
        "llm_diff_segments": llm_diff_segments,
        "diff_stats": diff_stats,
    })
