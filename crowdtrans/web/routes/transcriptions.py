"""Transcription browse/search/detail routes."""

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import or_

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription
from crowdtrans.web.app import templates

router = APIRouter(prefix="/transcriptions")

PAGE_SIZE = 25


@router.get("/")
def list_transcriptions(
    request: Request,
    q: str = Query("", description="Search query"),
    site: str = Query("", description="Filter by site"),
    status: str = Query("", description="Filter by status"),
    modality: str = Query("", description="Filter by modality code"),
    doctor: str = Query("", description="Filter by doctor family name"),
    date_from: str = Query("", description="Filter from date (YYYY-MM-DD)"),
    date_to: str = Query("", description="Filter to date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1),
):
    with SessionLocal() as session:
        query = session.query(Transcription)

        if site:
            query = query.filter(Transcription.site_id == site)

        if q:
            like = f"%{q}%"
            query = query.filter(
                or_(
                    Transcription.accession_number.ilike(like),
                    Transcription.patient_family_name.ilike(like),
                    Transcription.patient_given_names.ilike(like),
                    Transcription.patient_ur.ilike(like),
                    Transcription.transcript_text.ilike(like),
                    Transcription.procedure_description.ilike(like),
                )
            )

        if status:
            query = query.filter(Transcription.status == status)
        if modality:
            query = query.filter(Transcription.modality_code == modality)
        if doctor:
            query = query.filter(Transcription.doctor_family_name.ilike(f"%{doctor}%"))
        if date_from:
            query = query.filter(Transcription.dictation_date >= date_from)
        if date_to:
            query = query.filter(Transcription.dictation_date <= date_to + " 23:59:59")

        total = query.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        offset = (page - 1) * PAGE_SIZE

        items = (
            query.order_by(Transcription.dictation_date.desc().nullslast(), Transcription.id.desc())
            .offset(offset)
            .limit(PAGE_SIZE)
            .all()
        )

        # Filter dropdowns
        modalities = [
            r[0] for r in session.query(Transcription.modality_code)
            .filter(Transcription.modality_code.isnot(None))
            .distinct()
            .order_by(Transcription.modality_code)
            .all()
        ]
        statuses = [
            r[0] for r in session.query(Transcription.status)
            .distinct()
            .order_by(Transcription.status)
            .all()
        ]
        site_configs = get_config_store().get_site_configs()

    return templates.TemplateResponse("transcriptions/list.html", {
        "request": request,
        "items": items,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "q": q,
        "site": site,
        "status": status,
        "modality": modality,
        "doctor": doctor,
        "date_from": date_from,
        "date_to": date_to,
        "modalities": modalities,
        "statuses": statuses,
        "site_configs": site_configs,
    })


@router.get("/{transcription_id}")
def detail(request: Request, transcription_id: int):
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        # Resolve site name
        site_cfg = get_config_store().get_site(txn.site_id)
        site_name = site_cfg.site_name if site_cfg else txn.site_id

    return templates.TemplateResponse("transcriptions/detail.html", {
        "request": request,
        "txn": txn,
        "site_name": site_name,
    })
