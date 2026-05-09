"""JSON + HTMX API endpoints."""

import datetime
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from sqlalchemy import func, or_

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription, TranscriptionEdit, WordReplacement

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
            _pn = " ".join(p for p in [txn.patient_given_names, txn.patient_family_name] if p) or None
            txn.formatted_text = format_transcript(
                txn.transcript_text,
                modality_code=txn.modality_code,
                procedure_description=txn.procedure_description,
                clinical_history=txn.complaint,
                doctor_id=txn.doctor_id,
                patient_name=_pn,
            )
            count += 1
        session.commit()

    return {"status": "ok", "reformatted": count}


# ---------------------------------------------------------------------------
# Worklist actions
# ---------------------------------------------------------------------------


@router.post("/worklist/{transcription_id}/mark-copied")
def mark_copied(transcription_id: int):
    """Mark a worklist item as copied."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        txn.worklist_status = "copied"
        txn.copied_at = datetime.datetime.utcnow()
        session.commit()
    return {"status": "ok"}


@router.post("/worklist/{transcription_id}/mark-ready")
def mark_ready(transcription_id: int):
    """Undo — mark a worklist item back as ready."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        txn.worklist_status = "ready"
        txn.copied_at = None
        session.commit()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Audio-synced editor: word data, text editing, edit history
# ---------------------------------------------------------------------------


@router.get("/words/{transcription_id}")
def get_words(transcription_id: int):
    """Return word-level and paragraph-level timing data for audio sync."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        words = json.loads(txn.words_json) if txn.words_json else []
        paragraphs = json.loads(txn.paragraphs_json) if txn.paragraphs_json else []
    return {"words": words, "paragraphs": paragraphs}


@router.post("/worklist/{transcription_id}/edit-text")
async def edit_text(transcription_id: int, request: Request):
    """Save an edit to the formatted text, with audit trail."""
    body = await request.json()
    new_text = body.get("text", "").strip()
    editor = body.get("editor", "").strip() or "anonymous"

    if not new_text:
        raise HTTPException(status_code=400, detail="Text cannot be empty")

    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")

        original_text = txn.formatted_text or ""

        if new_text == original_text:
            return {"status": "ok", "message": "No changes detected"}

        # Save audit record
        edit = TranscriptionEdit(
            transcription_id=transcription_id,
            original_text=original_text,
            edited_text=new_text,
            editor=editor,
        )
        session.add(edit)

        # Update the live text
        txn.formatted_text = new_text
        session.commit()

    return {"status": "ok", "message": "Text saved", "edit_id": edit.id}


@router.get("/worklist/{transcription_id}/edit-history")
def edit_history(transcription_id: int):
    """Return the edit history for a transcription."""
    with SessionLocal() as session:
        edits = (
            session.query(TranscriptionEdit)
            .filter_by(transcription_id=transcription_id)
            .order_by(TranscriptionEdit.created_at.desc())
            .all()
        )
        return [
            {
                "id": e.id,
                "editor": e.editor,
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "original_text": e.original_text,
                "edited_text": e.edited_text,
            }
            for e in edits
        ]


@router.post("/worklist/{transcription_id}/revert/{edit_id}")
def revert_edit(transcription_id: int, edit_id: int):
    """Revert to the original text from a specific edit."""
    with SessionLocal() as session:
        edit_record = (
            session.query(TranscriptionEdit)
            .filter_by(id=edit_id, transcription_id=transcription_id)
            .first()
        )
        if not edit_record:
            raise HTTPException(status_code=404, detail="Edit not found")

        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")

        # Create a new audit entry for the revert
        revert = TranscriptionEdit(
            transcription_id=transcription_id,
            original_text=txn.formatted_text or "",
            edited_text=edit_record.original_text,
            editor="revert",
            edit_summary=f"Reverted to version before edit #{edit_id}",
        )
        session.add(revert)
        txn.formatted_text = edit_record.original_text
        session.commit()

    return {"status": "ok", "message": "Reverted successfully"}


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


@router.post("/word-replacement")
async def add_word_replacement(request: Request):
    """Add a word replacement rule, optionally per-doctor."""
    data = await request.json()
    original = (data.get("original") or "").strip()
    replacement = (data.get("replacement") or "").strip()
    doctor_id = (data.get("doctor_id") or "").strip() or None

    if not original or not replacement:
        return {"error": "Both original and replacement are required"}
    if original.lower() == replacement.lower():
        return {"error": "Original and replacement are the same"}

    with SessionLocal() as session:
        # Check for existing rule
        existing = session.query(WordReplacement).filter(
            WordReplacement.original == original,
            WordReplacement.doctor_id == doctor_id,
        ).first()
        if existing:
            existing.replacement = replacement
            existing.enabled = True
        else:
            session.add(WordReplacement(
                original=original,
                replacement=replacement,
                doctor_id=doctor_id,
            ))
        session.commit()

    # Clear formatter cache so the new rule takes effect
    try:
        from crowdtrans.transcriber.formatter import _clear_word_replacement_cache
        _clear_word_replacement_cache()
    except Exception:
        pass

    return {"status": "ok", "original": original, "replacement": replacement, "doctor_id": doctor_id}


@router.get("/word-replacements")
def list_word_replacements(doctor_id: str = Query("", description="Filter by doctor")):
    """List word replacement rules."""
    with SessionLocal() as session:
        q = session.query(WordReplacement).filter(WordReplacement.enabled.is_(True))
        if doctor_id:
            from sqlalchemy import or_
            q = q.filter(or_(
                WordReplacement.doctor_id == doctor_id,
                WordReplacement.doctor_id.is_(None),
            ))
        q = q.order_by(WordReplacement.original)
        rules = [
            {
                "id": r.id,
                "original": r.original,
                "replacement": r.replacement,
                "doctor_id": r.doctor_id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in q.all()
        ]
    return {"rules": rules}


@router.delete("/word-replacement/{rule_id}")
def delete_word_replacement(rule_id: int):
    """Delete a word replacement rule."""
    with SessionLocal() as session:
        rule = session.query(WordReplacement).filter_by(id=rule_id).first()
        if rule:
            session.delete(rule)
            session.commit()
    try:
        from crowdtrans.transcriber.formatter import _clear_word_replacement_cache
        _clear_word_replacement_cache()
    except Exception:
        pass
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Patient search typeahead
# ---------------------------------------------------------------------------


@router.get("/patient-search")
def patient_search(q: str = Query("", min_length=2, description="Search query")):
    """Google-style search across patient name, UR, accession, procedure, doctor, modality."""
    if len(q) < 2:
        return {"results": []}

    # Split query into words — each word must match at least one searchable field
    terms = [t.strip() for t in q.split() if t.strip()]
    if not terms:
        return {"results": []}

    searchable_fields = [
        Transcription.patient_family_name,
        Transcription.patient_given_names,
        Transcription.patient_ur,
        Transcription.accession_number,
        Transcription.procedure_description,
        Transcription.modality_code,
        Transcription.doctor_family_name,
        Transcription.doctor_given_names,
        Transcription.facility_name,
    ]

    with SessionLocal() as session:
        query = session.query(
            Transcription.id,
            Transcription.patient_family_name,
            Transcription.patient_given_names,
            Transcription.patient_ur,
            Transcription.accession_number,
            Transcription.procedure_description,
            Transcription.modality_code,
            Transcription.dictation_date,
            Transcription.worklist_status,
        ).filter(
            Transcription.status == "complete",
            Transcription.formatted_text.isnot(None),
        )

        # Each term must match at least one field (AND across terms, OR across fields)
        for term in terms[:5]:  # cap at 5 terms
            like = f"%{term}%"
            query = query.filter(
                or_(*(col.ilike(like) for col in searchable_fields))
            )

        matches = (
            query
            .order_by(Transcription.dictation_date.desc().nullslast())
            .limit(20)
            .all()
        )

    return {
        "results": [
            {
                "id": r.id,
                "patient_name": f"{r.patient_family_name or ''}, {r.patient_given_names or ''}".strip(", "),
                "patient_ur": r.patient_ur or "",
                "accession": r.accession_number or "",
                "procedure": r.procedure_description or "",
                "modality": r.modality_code or "",
                "date": r.dictation_date.strftime("%Y-%m-%d") if r.dictation_date else "",
                "status": r.worklist_status or "",
            }
            for r in matches
        ],
    }


# ---------------------------------------------------------------------------
# Report templates from Karisma
# ---------------------------------------------------------------------------


@router.get("/templates/{transcription_id}")
def get_templates(transcription_id: int):
    """Fetch report templates matching the doctor and procedure for a transcription."""
    import logging
    logger = logging.getLogger(__name__)

    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")

        doctor_surname = txn.doctor_family_name
        site_id = txn.site_id
        procedure = txn.procedure_description

    if not doctor_surname:
        return {"templates": [], "message": "No doctor name available"}

    store = get_config_store()
    site_cfg = store.get_site(site_id)
    if not site_cfg:
        return {"templates": [], "message": "Site not configured"}

    try:
        from crowdtrans.karisma import fetch_report_templates

        # Search by doctor surname + procedure keywords
        templates_list = fetch_report_templates(
            site_cfg,
            doctor_surname=doctor_surname,
            procedure_desc=procedure,
        )

        # If no results with procedure filter, try just doctor surname
        if not templates_list and procedure:
            templates_list = fetch_report_templates(
                site_cfg,
                doctor_surname=doctor_surname,
            )

        return {"templates": templates_list}
    except Exception as e:
        logger.warning("Failed to fetch templates: %s", e)
        return {"templates": [], "message": str(e)[:200]}
