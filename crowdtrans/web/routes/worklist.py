"""Typist worklist — shows Deepgram-completed transcriptions ready to copy into Karisma."""

import datetime
import logging

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


@router.get("/")
def worklist_list(
    request: Request,
    wl_status: str = Query("ready", description="Filter by worklist status"),
    modality: str = Query("", description="Filter by modality"),
    doctor: str = Query("", description="Filter by doctor"),
    date_from: str = Query("", description="Filter from date"),
    date_to: str = Query("", description="Filter to date"),
    page: int = Query(1, ge=1),
):
    """List transcriptions ready for typists to copy into Karisma."""
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

        total = base.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        offset = (page - 1) * PAGE_SIZE

        # FIFO: oldest first for ready items, newest first for copied/verified
        if wl_status == "ready":
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
        "modalities": modalities,
        "status_counts": status_counts,
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

    return templates.TemplateResponse("worklist/detail.html", {
        "request": request,
        "txn": txn,
        "site_name": site_name,
    })
