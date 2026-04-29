"""JSON + HTMX API endpoints."""

import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
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

        # Karisma: audio stored as SQL blob
        if txn.extent_key:
            store = get_config_store()
            site_cfg = store.get_site(txn.site_id)
            if not site_cfg:
                raise HTTPException(status_code=404, detail="Site not configured")

            from crowdtrans.karisma import fetch_audio_blob
            from crowdtrans.transcriber.audio import process_karisma_blob

            raw_blob = fetch_audio_blob(site_cfg, txn.extent_key)
            if raw_blob is None:
                raise HTTPException(status_code=404, detail="Audio blob not found in database")

            audio = process_karisma_blob(
                raw_blob, txn.extent_offset, txn.extent_length, txn.source_dictation_id,
            )
            if audio is None:
                raise HTTPException(status_code=500, detail="Audio decompression failed")

            return Response(
                content=audio.data,
                media_type=audio.content_type,
                headers={
                    "Content-Disposition": f'inline; filename="{txn.accession_number or txn.source_dictation_id}.wav"',
                },
            )

        # Visage: audio stored as NFS file
        if not txn.audio_relative_path or not txn.audio_basename:
            raise HTTPException(status_code=404, detail="No audio file associated with this transcription")

        store = get_config_store()
        site_cfg = store.get_site(txn.site_id)
        if not site_cfg or not site_cfg.audio_mount_path:
            raise HTTPException(status_code=404, detail="Audio mount not configured for this site")

        mount = Path(site_cfg.audio_mount_path)
        audio_path = mount / txn.audio_relative_path / f"{txn.audio_basename}.opus"
        if not audio_path.exists():
            audio_path = mount / txn.audio_relative_path / txn.audio_basename
            if not audio_path.exists():
                raise HTTPException(status_code=404, detail="Audio file not found on disk")

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


# ---------------------------------------------------------------------------
# Custom corrections CRUD
# ---------------------------------------------------------------------------

_CORRECTIONS_PATHS = [
    Path(__file__).resolve().parent.parent.parent.parent / "data" / "custom_corrections.json",
    Path("/opt/crowdtrans/data/custom_corrections.json"),
]


def _corrections_path() -> Path:
    """Return the writable custom_corrections.json path."""
    for p in _CORRECTIONS_PATHS:
        if p.exists():
            return p
    # Create at first candidate
    p = _CORRECTIONS_PATHS[0]
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_corrections() -> dict:
    import json
    p = _corrections_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"corrections": [], "filler_removals": [], "keyterms": []}


def _write_corrections(data: dict):
    import json
    p = _corrections_path()
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    # Reload cached patterns in formatter
    from crowdtrans.transcriber.formatter import reload_custom_corrections
    reload_custom_corrections()


@router.get("/corrections")
def list_corrections():
    """List all custom corrections."""
    return _read_corrections()


@router.post("/corrections")
async def add_correction(request: Request):
    """Add a custom correction, filler removal, or keyterm."""
    body = await request.json()
    data = _read_corrections()

    ctype = body.get("type", "correction")
    if ctype == "correction":
        find = (body.get("find") or "").strip()
        replace = (body.get("replace") or "").strip()
        if not find:
            raise HTTPException(status_code=400, detail="'find' is required")
        # Check for duplicate
        for existing in data["corrections"]:
            if existing.get("find", "").lower() == find.lower():
                raise HTTPException(status_code=409, detail=f"Correction for '{find}' already exists")
        entry = {"find": find, "replace": replace, "case_sensitive": body.get("case_sensitive", False)}
        if body.get("note"):
            entry["note"] = body["note"]
        data["corrections"].append(entry)
    elif ctype == "filler":
        phrase = (body.get("phrase") or "").strip()
        if not phrase:
            raise HTTPException(status_code=400, detail="'phrase' is required")
        data.setdefault("filler_removals", [])
        for existing in data["filler_removals"]:
            p = existing.get("phrase", existing) if isinstance(existing, dict) else existing
            if p.lower() == phrase.lower():
                raise HTTPException(status_code=409, detail=f"Filler '{phrase}' already exists")
        entry = {"phrase": phrase}
        if body.get("note"):
            entry["note"] = body["note"]
        data["filler_removals"].append(entry)
    elif ctype == "keyterm":
        term = (body.get("term") or "").strip()
        if not term:
            raise HTTPException(status_code=400, detail="'term' is required")
        data.setdefault("keyterms", [])
        if term.lower() in [t.lower() for t in data["keyterms"]]:
            raise HTTPException(status_code=409, detail=f"Keyterm '{term}' already exists")
        data["keyterms"].append(term)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown type '{ctype}'")

    _write_corrections(data)
    return {"status": "ok", "data": data}


@router.delete("/corrections/{index}")
def delete_correction(index: int, ctype: str = Query("correction")):
    """Delete a custom correction by index."""
    data = _read_corrections()
    if ctype == "correction":
        lst = data.get("corrections", [])
    elif ctype == "filler":
        lst = data.get("filler_removals", [])
    elif ctype == "keyterm":
        lst = data.get("keyterms", [])
    else:
        raise HTTPException(status_code=400, detail=f"Unknown type '{ctype}'")

    if index < 0 or index >= len(lst):
        raise HTTPException(status_code=404, detail="Index out of range")
    removed = lst.pop(index)
    _write_corrections(data)
    return {"status": "ok", "removed": removed}


@router.get("/formatting-additions/{transcription_id}")
def formatting_additions(transcription_id: int):
    """Show what the formatter added that wasn't in the dictation."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Not found")
        if not txn.formatted_text or not txn.transcript_text:
            return {"segments": [], "additions": []}

        additions = []

        # Procedure title (uppercase, first line)
        if txn.procedure_description:
            proc_upper = txn.procedure_description.upper()
            if txn.formatted_text.startswith(proc_upper):
                additions.append({
                    "type": "procedure_title",
                    "text": proc_upper,
                    "source": "procedure_description from RIS",
                })

        # Section headings
        import re
        heading_patterns = [
            "CLINICAL HISTORY", "FINDINGS", "CONCLUSION", "PROCEDURE",
            "REPORT", "IMPRESSION", "COMMENT", "OPINION", "SUMMARY",
        ]
        for line in txn.formatted_text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            is_heading = False
            for hp in heading_patterns:
                if stripped.upper() == hp or stripped.rstrip(":").upper() == hp:
                    is_heading = True
                    break
            if not is_heading and stripped.endswith(":") and len(stripped) < 30:
                if stripped.lower() not in txn.transcript_text.lower():
                    is_heading = True
            if is_heading and stripped.lower() not in txn.transcript_text.lower():
                additions.append({
                    "type": "heading",
                    "text": stripped,
                    "source": "section classification",
                })

        # Clinical history from referral
        if txn.complaint:
            if txn.complaint.strip() in txn.formatted_text and txn.complaint.strip() not in txn.transcript_text:
                additions.append({
                    "type": "clinical_history",
                    "text": txn.complaint.strip(),
                    "source": "clinical notes from RIS request",
                })

        # Footer
        if txn.doctor_id:
            from crowdtrans.transcriber.formatter import _get_doctor_footer_template
            footer_template = _get_doctor_footer_template(txn.doctor_id)
            if footer_template:
                additions.append({
                    "type": "footer",
                    "text": "(doctor sign-off block)",
                    "source": "learned from historical reports",
                })

        return {"additions": additions}
