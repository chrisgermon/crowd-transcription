"""JSON + HTMX API endpoints."""

import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription

router = APIRouter(prefix="/api")


@router.get("/stats")
def stats(site: str = Query("", description="Filter by site")):
    with SessionLocal() as session:
        base = session.query(Transcription)
        if site:
            base = base.filter(Transcription.site_id == site)

        status_rows = (
            base.with_entities(Transcription.status, func.count())
            .group_by(Transcription.status)
            .all()
        )
        status_counts = dict(status_rows)
        total = sum(status_counts.values())

        avg_confidence = (
            base.filter(Transcription.status == "complete")
            .with_entities(func.avg(Transcription.confidence))
            .scalar()
        )

        today = datetime.date.today()
        today_count = (
            base.filter(
                Transcription.transcription_completed_at >= datetime.datetime.combine(today, datetime.time.min),
                Transcription.status == "complete",
            )
            .count()
        )

    return {
        "total": total,
        "status_counts": status_counts,
        "today_completed": today_count or 0,
        "avg_confidence": round(avg_confidence * 100, 1) if avg_confidence else None,
    }


@router.get("/audio/{transcription_id}")
def stream_audio(transcription_id: int):
    """Stream the dictation audio file for a transcription."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        if not txn.audio_relative_path or not txn.audio_basename:
            raise HTTPException(status_code=404, detail="No audio file associated with this transcription")

        # Get the audio mount path from the site config
        store = get_config_store()
        site_cfg = store.get_site(txn.site_id)
        if not site_cfg or not site_cfg.audio_mount_path:
            raise HTTPException(status_code=404, detail="Audio mount not configured for this site")

        mount = Path(site_cfg.audio_mount_path)
        # Try with .opus extension first, then without
        audio_path = mount / txn.audio_relative_path / f"{txn.audio_basename}.opus"
        if not audio_path.exists():
            audio_path = mount / txn.audio_relative_path / txn.audio_basename
            if not audio_path.exists():
                raise HTTPException(status_code=404, detail="Audio file not found on disk")

        # Determine content type
        mime = txn.audio_mime_type or "audio/ogg"

    return FileResponse(
        path=str(audio_path),
        media_type=mime,
        filename=f"{txn.accession_number or txn.audio_basename}.opus",
    )


@router.post("/retry/{transcription_id}")
def retry(transcription_id: int):
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        if txn.status not in ("failed", "skipped"):
            raise HTTPException(status_code=400, detail=f"Cannot retry transcription with status '{txn.status}'")

        txn.status = "pending"
        txn.error_message = None
        session.commit()

    return {"status": "ok", "message": f"Transcription {transcription_id} queued for retry"}


@router.post("/reformat")
def reformat_all():
    """Re-format all completed transcriptions using the latest formatter."""
    from crowdtrans.transcriber.formatter import format_transcript

    with SessionLocal() as session:
        txns = (
            session.query(Transcription)
            .filter(Transcription.status == "complete", Transcription.transcript_text.isnot(None))
            .all()
        )
        count = 0
        for txn in txns:
            txn.formatted_text = format_transcript(
                txn.transcript_text,
                modality_code=txn.modality_code,
                procedure_description=txn.procedure_description,
                clinical_history=txn.complaint,
            )
            count += 1
        session.commit()

    return {"status": "ok", "reformatted": count}
