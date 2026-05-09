"""Typist worklist — shows Deepgram-completed transcriptions ready to copy into Karisma."""

import datetime
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import func
from sqlalchemy.orm import defer

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription
from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/worklist")

PAGE_SIZE = 25


_SORT_COLUMNS = {
    "patient": Transcription.patient_family_name,
    "accession": Transcription.accession_number,
    "procedure": Transcription.procedure_description,
    "modality": Transcription.modality_code,
    "doctor": Transcription.doctor_family_name,
    "date": Transcription.dictation_date,
    "processed": Transcription.transcription_completed_at,
    "location": Transcription.facility_name,
}


@router.get("/")
def worklist_list(
    request: Request,
    wl_status: str = Query("ready", description="Filter by worklist status"),
    modality: str = Query("", description="Filter by modality"),
    doctor: str = Query("", description="Filter by doctor"),
    date_from: str = Query("", description="Filter from date"),
    date_to: str = Query("", description="Filter to date"),
    location: Optional[list[str]] = Query(None, description="Filter by facility names"),
    sort: str = Query("", description="Sort column"),
    sort_dir: str = Query("", description="Sort direction: asc or desc"),
    page: int = Query(1, ge=1),
):
    """List transcriptions ready for typists to copy into Karisma."""
    # Default to today if no date filter specified
    if not date_from and not date_to:
        today_str = datetime.date.today().isoformat()
        date_from = today_str
        date_to = today_str

    with SessionLocal() as session:
        base = (
            session.query(Transcription)
            .options(
                defer(Transcription.words_json),
                defer(Transcription.paragraphs_json),
                defer(Transcription.transcript_text),
            )
            .filter(
                Transcription.status == "complete",
                Transcription.formatted_text.isnot(None),
            )
        )

        # Worklist status filter
        if wl_status == "all":
            pass  # show everything
        elif wl_status in ("ready", "copied", "verified"):
            base = base.filter(Transcription.worklist_status == wl_status)
        else:
            # Default: show ready items
            base = base.filter(Transcription.worklist_status == "ready")

        if modality:
            base = base.filter(Transcription.modality_code == modality)
        if doctor:
            base = base.filter(Transcription.doctor_family_name.ilike(f"%{doctor}%"))
        if date_from:
            base = base.filter(Transcription.dictation_date >= date_from)
        if date_to:
            base = base.filter(Transcription.dictation_date <= date_to + " 23:59:59")
        if location:
            base = base.filter(Transcription.facility_name.in_(location))

        total = base.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        offset = (page - 1) * PAGE_SIZE

        # Sorting
        sort_col = _SORT_COLUMNS.get(sort)
        if sort_col is not None:
            if sort_dir == "desc":
                order = sort_col.desc().nullslast()
            else:
                order = sort_col.asc().nullslast()
        elif wl_status == "ready":
            order = Transcription.dictation_date.asc().nullslast()
        else:
            order = Transcription.dictation_date.desc().nullslast()

        items = base.order_by(order).offset(offset).limit(PAGE_SIZE).all()

        # Status counts for the badges
        count_q = (
            session.query(Transcription.worklist_status, func.count())
            .filter(
                Transcription.status == "complete",
                Transcription.formatted_text.isnot(None),
            )
            .group_by(Transcription.worklist_status)
        )
        status_counts = dict(count_q.all())

        # Filter dropdowns
        modalities = [
            r[0] for r in session.query(Transcription.modality_code)
            .filter(Transcription.modality_code.isnot(None))
            .distinct()
            .order_by(Transcription.modality_code)
            .all()
        ]

        # Facility list for location filter (exclude worksites hidden in settings)
        excluded_raw = get_config_store().get_global("excluded_worksites") or ""
        excluded_set = {s.strip() for s in excluded_raw.split(",") if s.strip()}
        facilities_q = (
            session.query(Transcription.facility_name)
            .filter(Transcription.facility_name.isnot(None))
            .distinct()
            .order_by(Transcription.facility_name)
        )
        if excluded_set:
            facilities_q = facilities_q.filter(
                Transcription.facility_name.notin_(excluded_set)
            )
        facilities = [r[0] for r in facilities_q.all()]

        # Date helpers for quick-filter buttons
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        week_start = today - datetime.timedelta(days=today.weekday())  # Monday
        month_start = today.replace(day=1)

    return templates.TemplateResponse("worklist/list.html", {
        "request": request,
        "items": items,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "wl_status": wl_status,
        "modality": modality,
        "doctor": doctor,
        "date_from": date_from,
        "date_to": date_to,
        "location": location or [],
        "sort": sort,
        "sort_dir": sort_dir,
        "modalities": modalities,
        "facilities": facilities,
        "status_counts": status_counts,
        "today": today.isoformat(),
        "yesterday": yesterday.isoformat(),
        "week_start": week_start.isoformat(),
        "month_start": month_start.isoformat(),
    })


@router.get("/{transcription_id}")
def worklist_detail(request: Request, transcription_id: int):
    """Detail view for a single worklist item with copy and mark-as-copied."""
    with SessionLocal() as session:
        txn = (
            session.query(Transcription)
            .options(defer(Transcription.words_json), defer(Transcription.paragraphs_json))
            .filter_by(id=transcription_id)
            .first()
        )
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")

        site_cfg = get_config_store().get_site(txn.site_id)
        site_name = site_cfg.site_name if site_cfg else txn.site_id

        # Find prev/next items in the worklist (ready items, FIFO order)
        ready_ids = (
            session.query(Transcription.id)
            .filter(
                Transcription.status == "complete",
                Transcription.formatted_text.isnot(None),
                Transcription.worklist_status == "ready",
            )
            .order_by(Transcription.dictation_date.asc().nullslast())
            .all()
        )
        ready_id_list = [r[0] for r in ready_ids]
        prev_id = None
        next_id = None
        if transcription_id in ready_id_list:
            idx = ready_id_list.index(transcription_id)
            if idx > 0:
                prev_id = ready_id_list[idx - 1]
            if idx < len(ready_id_list) - 1:
                next_id = ready_id_list[idx + 1]

        ready_count = len(ready_id_list)

    return templates.TemplateResponse("worklist/detail.html", {
        "request": request,
        "txn": txn,
        "site_name": site_name,
        "prev_id": prev_id,
        "next_id": next_id,
        "ready_count": ready_count,
    })
