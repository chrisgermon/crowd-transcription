"""Voice recognition worklist — undictated studies ready for browser-based dictation."""

import datetime
import logging
import threading
import time
from typing import Optional

from fastapi import APIRouter, Query, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import PendingStudy
from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/voice")

PAGE_SIZE = 50
SYNC_STALE_SECONDS = 300  # Auto-sync if data is older than 5 minutes
_sync_lock = threading.Lock()


def _auto_sync_if_stale():
    """Check if cached data is stale and trigger background sync."""
    with SessionLocal() as session:
        last = session.query(PendingStudy.synced_at).order_by(
            PendingStudy.synced_at.desc()
        ).first()

    if last:
        age = (datetime.datetime.utcnow() - last[0]).total_seconds()
        if age < SYNC_STALE_SECONDS:
            return  # Fresh enough

    # Run sync in background thread to not block page load
    if _sync_lock.locked():
        return  # Another sync already running
    thread = threading.Thread(target=_do_sync, daemon=True)
    thread.start()


def _do_sync():
    """Perform the actual sync (runs in background thread)."""
    if not _sync_lock.acquire(blocking=False):
        return
    try:
        store = get_config_store()
        excluded_raw = store.get_global("excluded_worksites") or ""
        excluded_set = {s.strip() for s in excluded_raw.split(",") if s.strip()}

        for site_cfg in store.get_enabled_site_configs():
            if site_cfg.ris_type != "karisma":
                continue
            try:
                from crowdtrans.karisma import sync_pending_studies
                sync_pending_studies(site_cfg.site_id, site_cfg, excluded_set)
            except Exception as e:
                logger.error("Background sync failed for %s: %s", site_cfg.site_id, e)
    finally:
        _sync_lock.release()

_SORT_COLUMNS = {
    "patient": PendingStudy.patient_last_name,
    "accession": PendingStudy.accession_number,
    "procedure": PendingStudy.service_name,
    "modality": PendingStudy.modality_code,
    "doctor": PendingStudy.doctor_surname,
    "date": PendingStudy.registered_date,
    "location": PendingStudy.facility_name,
}


@router.get("/")
def voice_list(
    request: Request,
    modality: str = Query("", description="Filter by modality"),
    doctor: str = Query("", description="Filter by doctor surname"),
    location: Optional[list[str]] = Query(None, description="Filter by facility names"),
    sort: str = Query("date", description="Sort column"),
    sort_dir: str = Query("desc", description="Sort direction"),
    page: int = Query(1, ge=1),
):
    """List undictated studies from local cache — instant."""
    _auto_sync_if_stale()

    with SessionLocal() as session:
        base = session.query(PendingStudy)

        if modality:
            base = base.filter(PendingStudy.modality_code == modality)
        if doctor:
            base = base.filter(PendingStudy.doctor_surname.ilike(f"%{doctor}%"))
        if location:
            base = base.filter(PendingStudy.facility_name.in_(location))

        total = base.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        offset = (page - 1) * PAGE_SIZE

        # Sorting
        sort_col = _SORT_COLUMNS.get(sort, PendingStudy.registered_date)
        if sort_dir == "desc":
            order = sort_col.desc().nullslast()
        else:
            order = sort_col.asc().nullslast()

        items = base.order_by(order).offset(offset).limit(PAGE_SIZE).all()

        # Get last sync time
        last_sync = session.query(PendingStudy.synced_at).order_by(
            PendingStudy.synced_at.desc()
        ).first()
        last_sync_time = last_sync[0] if last_sync else None

        # Filter dropdown values
        modalities = [
            r[0] for r in session.query(PendingStudy.modality_code)
            .filter(PendingStudy.modality_code.isnot(None))
            .distinct().order_by(PendingStudy.modality_code).all()
        ]
        facilities = [
            r[0] for r in session.query(PendingStudy.facility_name)
            .filter(PendingStudy.facility_name.isnot(None))
            .distinct().order_by(PendingStudy.facility_name).all()
        ]

    return templates.TemplateResponse("voice/list.html", {
        "request": request,
        "items": items,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "modality": modality,
        "doctor": doctor,
        "location": location or [],
        "sort": sort,
        "sort_dir": sort_dir,
        "modalities": modalities,
        "facilities": facilities,
        "last_sync_time": last_sync_time,
    })


@router.post("/sync")
def voice_sync(request: Request):
    """Trigger a manual sync of pending studies from Karisma."""
    store = get_config_store()
    excluded_raw = store.get_global("excluded_worksites") or ""
    excluded_set = {s.strip() for s in excluded_raw.split(",") if s.strip()}

    total = 0
    for site_cfg in store.get_enabled_site_configs():
        if site_cfg.ris_type != "karisma":
            continue
        try:
            from crowdtrans.karisma import sync_pending_studies
            count = sync_pending_studies(site_cfg.site_id, site_cfg, excluded_set)
            total += count
        except Exception as e:
            logger.error("Sync failed for %s: %s", site_cfg.site_id, e)
            return JSONResponse({"error": str(e)}, status_code=500)

    return JSONResponse({"status": "ok", "synced": total})


@router.get("/{service_key}")
def voice_detail(request: Request, service_key: int):
    """Detail view for a single study — record and transcribe."""
    with SessionLocal() as session:
        study = session.query(PendingStudy).filter(
            PendingStudy.service_key == service_key
        ).first()

        if not study:
            raise HTTPException(status_code=404, detail="Study not found or already dictated")

        # Convert to dict for template
        study_dict = {
            "ServiceKey": study.service_key,
            "AccessionNumber": study.accession_number,
            "InternalIdentifier": study.internal_identifier,
            "PatientTitle": study.patient_title,
            "PatientFirstName": study.patient_first_name,
            "PatientLastName": study.patient_last_name,
            "PatientId": study.patient_id,
            "PatientDOB": study.patient_dob,
            "ServiceName": study.service_name,
            "ServiceCode": study.service_code,
            "ModalityCode": study.modality_code,
            "ModalityName": study.modality_name,
            "PractitionerCode": study.doctor_code,
            "PractitionerTitle": study.doctor_title,
            "PractitionerFirstName": study.doctor_first_name,
            "PractitionerSurname": study.doctor_surname,
            "WorkSiteName": study.facility_name,
            "RegisteredDate": study.registered_date,
        }

    return templates.TemplateResponse("voice/detail.html", {
        "request": request,
        "study": study_dict,
    })


@router.post("/transcribe")
async def voice_transcribe(
    request: Request,
    audio: UploadFile = File(...),
    modality_code: str = Form(""),
    procedure_description: str = Form(""),
    clinical_history: str = Form(""),
    doctor_code: str = Form(""),
    patient_name: str = Form(""),
):
    """Receive recorded audio, transcribe via Deepgram, format, and return."""
    start_time = time.monotonic()

    audio_data = await audio.read()
    if not audio_data:
        return JSONResponse({"error": "No audio data received"}, status_code=400)

    logger.info(
        "Voice transcription request: %d bytes, modality=%s, procedure=%s",
        len(audio_data), modality_code, procedure_description,
    )

    try:
        from crowdtrans.transcriber.deepgram_client import transcribe_buffer
        dg_result = transcribe_buffer(
            audio_data,
            content_type=audio.content_type or "audio/webm",
            label="voice-recording",
        )

        from crowdtrans.transcriber.formatter import format_transcript
        formatted = format_transcript(
            dg_result.transcript_text,
            modality_code=modality_code or None,
            procedure_description=procedure_description or None,
            clinical_history=clinical_history or None,
            doctor_id=doctor_code or None,
            patient_name=patient_name or None,
        )

        elapsed_ms = int((time.monotonic() - start_time) * 1000)

        return JSONResponse({
            "status": "ok",
            "raw_transcript": dg_result.transcript_text,
            "formatted_text": formatted,
            "confidence": dg_result.confidence,
            "processing_ms": elapsed_ms,
        })

    except Exception as e:
        logger.error("Voice transcription failed: %s", e, exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)
