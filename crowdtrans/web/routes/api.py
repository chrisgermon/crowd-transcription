"""JSON + HTMX API endpoints."""

import datetime
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from sqlalchemy import func, or_

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import CorrectionFeedback, ReportTemplate, Transcription, TranscriptionEdit, WordReplacement

router = APIRouter(prefix="/api")


# ---------------------------------------------------------------------------
# Stats snapshot — shared by /api/stats and the SSE stream
# ---------------------------------------------------------------------------


def _build_stats_snapshot(site: str = "") -> dict:
    """Single source of truth for what the dashboard shows.

    Returns the same shape /api/stats has historically returned, plus the
    fields the live dashboard needs (watermarks, recent completions). Keeping
    these in one helper means the SSE stream and the JSON endpoint can't drift.
    """
    from crowdtrans.models import Watermark

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

        recent_rows = (
            base.filter(Transcription.status == "complete")
            .order_by(Transcription.transcription_completed_at.desc())
            .limit(10)
            .all()
        )
        recent = [
            {
                "id": r.id,
                "site_id": r.site_id,
                "accession_number": r.accession_number,
                "patient_family_name": r.patient_family_name,
                "patient_given_names": r.patient_given_names,
                "modality_code": r.modality_code,
                "doctor_family_name": r.doctor_family_name,
                "confidence": r.confidence,
                "transcription_completed_at": r.transcription_completed_at.isoformat()
                    if r.transcription_completed_at else None,
            }
            for r in recent_rows
        ]

        watermarks = [
            {
                "site_id": w.site_id,
                "last_dictation_id": w.last_dictation_id,
                "last_poll_at": w.last_poll_at.isoformat() if w.last_poll_at else None,
            }
            for w in session.query(Watermark).all()
        ]

    return {
        "total": total,
        "status_counts": status_counts,
        "today_completed": today_count or 0,
        "avg_confidence": round(avg_confidence * 100, 1) if avg_confidence else None,
        "recent": recent,
        "watermarks": watermarks,
        "ts": datetime.datetime.utcnow().isoformat(),
    }


@router.get("/changelog")
def changelog(limit: int = Query(10, ge=1, le=100)):
    """Return the most recent changelog entries parsed from CHANGELOG.md.

    Markdown layout: `## YYYY-MM-DD` headings, bullet items beneath.
    Each item becomes {date, title, body} where title is the bold lead-in
    (e.g. "**Verify workflow**") and body is the remainder.
    """
    candidates = [
        Path("/opt/crowdtrans/CHANGELOG.md"),
        Path(__file__).resolve().parent.parent.parent.parent / "CHANGELOG.md",
    ]
    src = next((p for p in candidates if p.exists()), None)
    if not src:
        return {"entries": []}
    raw = src.read_text(encoding="utf-8")
    entries = []
    current_date = None
    import re as _re
    for line in raw.splitlines():
        m = _re.match(r"^##\s+(\d{4}-\d{2}-\d{2})\s*$", line)
        if m:
            current_date = m.group(1)
            continue
        m = _re.match(r"^-\s+(.*)$", line)
        if not m or not current_date:
            continue
        text = m.group(1).strip()
        title = text
        body = ""
        bold = _re.match(r"^\*\*(.+?)\*\*\s*[—-]?\s*(.*)$", text)
        if bold:
            title = bold.group(1).strip()
            body = bold.group(2).strip()
        entries.append({"date": current_date, "title": title, "body": body})
        if len(entries) >= limit:
            break
    return {"entries": entries}


@router.get("/stats")
def stats(site: str = Query("", description="Filter by site")):
    snap = _build_stats_snapshot(site)
    # Preserve original endpoint shape (no recent/watermarks here — those go
    # through /api/stream/stats for the live dashboard).
    return {
        "total": snap["total"],
        "status_counts": snap["status_counts"],
        "today_completed": snap["today_completed"],
        "avg_confidence": snap["avg_confidence"],
    }


@router.get("/stream/stats")
async def stream_stats(request: Request, site: str = Query("", description="Filter by site")):
    """Server-sent-events stream pushing live stats snapshots to the dashboard.

    Emits a snapshot every ~3s only if anything changed vs the last sent
    snapshot. Heartbeat comments are interleaved so proxies don't drop
    idle connections.
    """
    import asyncio
    import hashlib

    from fastapi.responses import StreamingResponse

    POLL_SECONDS = 3
    HEARTBEAT_SECONDS = 25
    MAX_DURATION_SECONDS = 60 * 30  # 30 min cap per connection; client reconnects

    def _digest(snap: dict) -> str:
        # Hash everything except the timestamp so identical state doesn't re-emit
        payload = {k: v for k, v in snap.items() if k != "ts"}
        return hashlib.md5(
            json.dumps(payload, sort_keys=True, default=str).encode()
        ).hexdigest()

    async def _event_stream():
        last_digest: str | None = None
        last_heartbeat = 0.0
        start = asyncio.get_event_loop().time()

        # Emit an immediate snapshot so the client doesn't have to wait POLL_SECONDS
        snap = await asyncio.to_thread(_build_stats_snapshot, site)
        last_digest = _digest(snap)
        yield f"event: snapshot\ndata: {json.dumps(snap, default=str)}\n\n"

        while True:
            if await request.is_disconnected():
                return
            now = asyncio.get_event_loop().time()
            if now - start > MAX_DURATION_SECONDS:
                # Tell the client we're closing politely; EventSource will reconnect
                yield "event: bye\ndata: {}\n\n"
                return

            await asyncio.sleep(POLL_SECONDS)

            try:
                snap = await asyncio.to_thread(_build_stats_snapshot, site)
            except Exception:
                # On a transient DB error, send a heartbeat instead of crashing
                # the connection — client stays subscribed.
                yield ": db-error\n\n"
                continue

            d = _digest(snap)
            if d != last_digest:
                last_digest = d
                last_heartbeat = now
                yield f"event: snapshot\ndata: {json.dumps(snap, default=str)}\n\n"
            elif now - last_heartbeat > HEARTBEAT_SECONDS:
                last_heartbeat = now
                yield ": keepalive\n\n"

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            # Critical: disable buffering at any reverse proxy (nginx, Cloudflare).
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/audio/{transcription_id}")
def stream_audio(transcription_id: int, request: Request):
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

            data = audio.data
            content_type = audio.content_type
            total = len(data)
            filename = txn.accession_number or txn.source_dictation_id

            # Support HTTP range requests for audio seeking
            range_header = request.headers.get("range")
            if range_header:
                # Parse "bytes=START-END"
                range_spec = range_header.replace("bytes=", "").strip()
                parts = range_spec.split("-")
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else total - 1
                end = min(end, total - 1)
                chunk = data[start:end + 1]
                return Response(
                    content=chunk,
                    status_code=206,
                    media_type=content_type,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{total}",
                        "Accept-Ranges": "bytes",
                        "Content-Length": str(len(chunk)),
                        "Content-Disposition": f'inline; filename="{filename}.wav"',
                    },
                )

            return Response(
                content=data,
                media_type=content_type,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Length": str(total),
                    "Content-Disposition": f'inline; filename="{filename}.wav"',
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
                patient_ur=txn.patient_ur,
            )
            count += 1
        session.commit()

    return {"status": "ok", "reformatted": count}


# ---------------------------------------------------------------------------
# Worklist actions
# ---------------------------------------------------------------------------


@router.post("/worklist/{transcription_id}/mark-copied")
def mark_copied(transcription_id: int):
    """Mark a worklist item as copied. Drops the cached attachments."""
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        txn.worklist_status = "copied"
        txn.copied_at = datetime.datetime.utcnow()
        session.commit()
    from crowdtrans.transcriber.attachments import drop_cache
    drop_cache(transcription_id)
    return {"status": "ok"}


@router.post("/worklist/{transcription_id}/verify")
async def verify_report(transcription_id: int, request: Request):
    """Sign off a report. Appends the radiologist's signature block, freezes
    the result into final_text, and marks the worklist item as 'verified'.

    Body: {verifier: str}
    """
    body = await request.json()
    verifier = (body.get("verifier") or "").strip() or "anonymous"

    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        if not txn.formatted_text:
            raise HTTPException(status_code=400, detail="No formatted text to verify")

        from crowdtrans.transcriber.formatter import finalize_report_text
        final = finalize_report_text(
            txn.formatted_text,
            str(txn.doctor_id) if txn.doctor_id else None,
        )
        txn.final_text = final
        txn.worklist_status = "verified"
        txn.verified_at = datetime.datetime.utcnow()
        txn.verified_by = verifier
        session.commit()

    from crowdtrans.transcriber.attachments import drop_cache
    drop_cache(transcription_id)
    return {"status": "ok", "verified_at": txn.verified_at.isoformat(), "verified_by": verifier}


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


# ---------------------------------------------------------------------------
# Confidence highlighting
# ---------------------------------------------------------------------------

# Words to silently treat as confident even if Deepgram scored them low
# (very common short tokens whose low scores are usually noise, not real errors).
_CONFIDENCE_IGNORE = {
    "a", "an", "the", "is", "are", "was", "were", "of", "to", "in", "on",
    "at", "and", "or", "but", "if", "by", "as", "it",
}


def _conf_band(conf: float | None) -> str:
    """Map a confidence score to a UI band: high / medium / low / unknown."""
    if conf is None:
        return "unknown"
    if conf >= 0.9:
        return "high"
    if conf >= 0.75:
        return "medium"
    return "low"


def _tokenise_with_spans(text: str) -> list[dict]:
    """Split text into (token, start, end) spans, preserving punctuation positions.

    Returns a list of {"text": str, "start": int, "end": int, "is_word": bool}.
    Word tokens are matched on letters/digits/apostrophes/hyphens; everything
    else (whitespace, punctuation) becomes a non-word chunk.
    """
    import re as _re
    tokens = []
    pos = 0
    pattern = _re.compile(r"[A-Za-z0-9][A-Za-z0-9'\-]*")
    for m in pattern.finditer(text):
        if m.start() > pos:
            tokens.append({"text": text[pos:m.start()], "start": pos, "end": m.start(), "is_word": False})
        tokens.append({"text": m.group(0), "start": m.start(), "end": m.end(), "is_word": True})
        pos = m.end()
    if pos < len(text):
        tokens.append({"text": text[pos:], "start": pos, "end": len(text), "is_word": False})
    return tokens


@router.get("/confidence/{transcription_id}")
def get_confidence(transcription_id: int):
    """Return word-level confidence data + aligned formatted text tokens.

    Response shape:
      {
        "raw_words": [{word, start, end, confidence, band, alternatives?}, ...],
        "formatted_tokens": [{text, is_word, confidence?, band, start?, end?}, ...],
        "llm_tokens":      [{text, is_word, confidence?, band, start?, end?}, ...],
        "summary": {
            "min_confidence", "avg_confidence",
            "low_count", "medium_count", "high_count",
            "lowest": [{word, confidence, start}, ...]
        }
      }
    """
    from difflib import SequenceMatcher

    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        raw_words = json.loads(txn.words_json) if txn.words_json else []
        formatted_text = txn.formatted_text or ""
        llm_text = txn.llm_formatted_text or ""

    # Annotate raw words with band
    annotated_raw = []
    confidences = []
    for w in raw_words:
        conf = w.get("confidence")
        if conf is not None:
            confidences.append(conf)
        band = _conf_band(conf)
        # Suppress low-band warnings for noise stopwords
        if band == "low" and (w.get("word") or "").lower() in _CONFIDENCE_IGNORE:
            band = "medium"
        entry = {
            "word": w.get("word"),
            "start": w.get("start"),
            "end": w.get("end"),
            "confidence": conf,
            "band": band,
        }
        if w.get("alternatives"):
            entry["alternatives"] = w["alternatives"]
        annotated_raw.append(entry)

    def _align(text: str) -> list[dict]:
        if not text or not annotated_raw:
            return []
        tokens = _tokenise_with_spans(text)
        # Build lowercase word-only sequences for matching
        raw_seq = [(w.get("word") or "").lower() for w in annotated_raw]
        token_word_indices = [i for i, t in enumerate(tokens) if t["is_word"]]
        token_seq = [tokens[i]["text"].lower() for i in token_word_indices]

        sm = SequenceMatcher(None, token_seq, raw_seq, autojunk=False)
        # Map each formatted word-index → raw word-index (or None)
        token_to_raw: dict[int, int] = {}
        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            if tag == "equal":
                for k in range(i2 - i1):
                    token_to_raw[i1 + k] = j1 + k

        out = []
        for i, t in enumerate(tokens):
            entry = {"text": t["text"], "is_word": t["is_word"]}
            if t["is_word"]:
                tw_idx = None
                # Find this token's position in token_word_indices
                # (small list, linear scan is fine for typical report sizes)
                try:
                    tw_idx = token_word_indices.index(i)
                except ValueError:
                    tw_idx = None
                if tw_idx is not None and tw_idx in token_to_raw:
                    raw = annotated_raw[token_to_raw[tw_idx]]
                    entry["confidence"] = raw.get("confidence")
                    entry["band"] = raw.get("band")
                    entry["start"] = raw.get("start")
                    entry["end"] = raw.get("end")
                    if raw.get("alternatives"):
                        entry["alternatives"] = raw["alternatives"]
                else:
                    entry["confidence"] = None
                    entry["band"] = "unknown"
            out.append(entry)
        return out

    formatted_tokens = _align(formatted_text)
    llm_tokens = _align(llm_text) if llm_text else []

    # Summary stats
    low_count = sum(1 for w in annotated_raw if w["band"] == "low")
    medium_count = sum(1 for w in annotated_raw if w["band"] == "medium")
    high_count = sum(1 for w in annotated_raw if w["band"] == "high")
    lowest = sorted(
        [w for w in annotated_raw if w["confidence"] is not None],
        key=lambda w: w["confidence"],
    )[:10]
    summary = {
        "min_confidence": min(confidences) if confidences else None,
        "avg_confidence": sum(confidences) / len(confidences) if confidences else None,
        "low_count": low_count,
        "medium_count": medium_count,
        "high_count": high_count,
        "lowest": [
            {"word": w["word"], "confidence": w["confidence"], "start": w["start"]}
            for w in lowest
        ],
    }

    return {
        "raw_words": annotated_raw,
        "formatted_tokens": formatted_tokens,
        "llm_tokens": llm_tokens,
        "summary": summary,
    }


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

        # Auto-learn: derive (wrong, right) pairs from this edit and queue
        # them as pending CorrectionFeedback rows for approval. Best-effort;
        # never block the save on a learner failure.
        feedback_inserted = 0
        try:
            from crowdtrans.transcriber.learner import record_edit_feedback
            feedback_inserted = record_edit_feedback(
                session,
                transcription_id=transcription_id,
                doctor_id=str(txn.doctor_id) if txn.doctor_id else None,
                original_text=original_text,
                edited_text=new_text,
            )
        except Exception:
            import logging as _logging
            _logging.getLogger(__name__).exception("record_edit_feedback failed")

        session.commit()

    return {
        "status": "ok",
        "message": "Text saved",
        "edit_id": edit.id,
        "feedback_inserted": feedback_inserted,
    }


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
        is_regex = bool(body.get("regex", False))
        case_sensitive = bool(body.get("case_sensitive", False))
        if not find:
            raise HTTPException(status_code=400, detail="'find' is required")
        if is_regex:
            import re as _re
            try:
                _re.compile(find, 0 if case_sensitive else _re.IGNORECASE)
            except _re.error as exc:
                raise HTTPException(status_code=400, detail=f"Invalid regex: {exc}")
        # Check for duplicate
        for existing in data["corrections"]:
            if existing.get("find", "").lower() == find.lower():
                raise HTTPException(status_code=409, detail=f"Correction for '{find}' already exists")
        entry = {
            "find": find,
            "replace": replace,
            "case_sensitive": case_sensitive,
            "regex": is_regex,
        }
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


@router.post("/corrections/preview")
async def preview_correction(request: Request):
    """Preview a find/replace (regex or literal) against recent formatted_text rows.

    Body: {pattern, replace, regex, case_sensitive, modality?, doctor?, limit?}
    Returns up to `limit` matching transcriptions with before/after snippets.
    """
    import re as _re
    body = await request.json()
    pattern = (body.get("pattern") or body.get("find") or "").strip()
    replace = body.get("replace") or ""
    is_regex = bool(body.get("regex", False))
    case_sensitive = bool(body.get("case_sensitive", False))
    modality = (body.get("modality") or "").strip()
    doctor = (body.get("doctor") or "").strip()
    limit = int(body.get("limit") or 50)
    if not pattern:
        raise HTTPException(status_code=400, detail="'pattern' is required")

    flags = 0 if case_sensitive else _re.IGNORECASE
    try:
        if is_regex:
            compiled = _re.compile(pattern, flags)
        else:
            compiled = _re.compile(r"\b" + _re.escape(pattern) + r"\b", flags)
    except _re.error as exc:
        raise HTTPException(status_code=400, detail=f"Invalid regex: {exc}")

    with SessionLocal() as session:
        query = (
            session.query(Transcription)
            .filter(Transcription.formatted_text.isnot(None))
        )
        if modality:
            query = query.filter(Transcription.modality_code == modality)
        if doctor:
            query = query.filter(Transcription.doctor_family_name.ilike(f"%{doctor}%"))
        rows = (
            query.order_by(Transcription.id.desc())
            .limit(max(limit * 4, 100))
            .all()
        )

    matches = []
    total_scanned = 0
    total_matches = 0
    for txn in rows:
        total_scanned += 1
        text = txn.formatted_text or ""
        hits = list(compiled.finditer(text))
        if not hits:
            continue
        total_matches += len(hits)
        # Take first 3 hits per row; show 60-char window
        snippets = []
        for h in hits[:3]:
            start = max(0, h.start() - 40)
            end = min(len(text), h.end() + 40)
            before = text[start:end]
            after = compiled.sub(replace, before)
            snippets.append({
                "before": before,
                "after": after,
                "match": h.group(0),
            })
        matches.append({
            "id": txn.id,
            "accession": txn.accession_number,
            "doctor": txn.doctor_family_name,
            "modality": txn.modality_code,
            "match_count": len(hits),
            "snippets": snippets,
        })
        if len(matches) >= limit:
            break

    return {
        "scanned": total_scanned,
        "matched_rows": len(matches),
        "total_matches": total_matches,
        "matches": matches,
    }


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
# Attachments: referral documents and worksheets from Karisma
# ---------------------------------------------------------------------------


@router.get("/attachments/{transcription_id}")
def get_attachments(transcription_id: int):
    """Fetch referral PDFs and worksheet images for a transcription.

    Always serves from the local cache first (populated by the transcription
    service right after each transcribe). Falls back to a live Karisma+SILO
    fetch only if the cache is missing, and caches the result for next time.
    """
    import base64
    import logging
    logger = logging.getLogger(__name__)

    from crowdtrans.transcriber.attachments import (
        read_cache, cache_attachments, has_cache,
    )

    def _serialise(att: dict) -> dict:
        return {
            "name": att.get("name") or "",
            "format": att.get("format") or "",
            "length": att.get("length") or 0,
            "external": bool(att.get("external")),
            "data": base64.b64encode(att["data"]).decode("ascii") if att.get("data") else "",
        }

    # --- Cache hit: bypass Karisma entirely ---
    cached = read_cache(transcription_id)
    if cached is not None:
        return {
            "referrals": [_serialise(a) for a in cached["referrals"]],
            "worksheets": [_serialise(a) for a in cached["worksheets"]],
        }

    # --- Cache miss: pull live, then cache for next time ---
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=transcription_id).first()
        if not txn:
            raise HTTPException(status_code=404, detail="Transcription not found")
        if txn.site_id != "karisma":
            return {"referrals": [], "worksheets": []}

        from crowdtrans.config import Settings
        site_settings = Settings()
        site = site_settings.get_site("karisma")
        if not site:
            return {"referrals": [], "worksheets": []}

        try:
            cache_attachments(site, txn)
        except Exception:
            logger.exception("live cache_attachments failed for %s", transcription_id)

    cached = read_cache(transcription_id) or {"referrals": [], "worksheets": []}
    return {
        "referrals": [_serialise(a) for a in cached["referrals"]],
        "worksheets": [_serialise(a) for a in cached["worksheets"]],
    }


# ---------------------------------------------------------------------------
# Report templates from Karisma
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Correction feedback inbox — auto-learned pairs awaiting approval
# ---------------------------------------------------------------------------


@router.get("/correction-feedback")
def list_correction_feedback(status: str = Query("pending"), limit: int = Query(100, ge=1, le=500)):
    """Return CorrectionFeedback rows grouped by (original, corrected) pair.

    Each group includes occurrence count, distinct doctors involved, and a
    sample transcription id for context.
    """
    with SessionLocal() as session:
        rows = (
            session.query(CorrectionFeedback)
            .filter(CorrectionFeedback.status == status)
            .filter(CorrectionFeedback.correction_type == "word")
            .order_by(CorrectionFeedback.created_at.desc())
            .all()
        )
        groups: dict[tuple[str, str], dict] = {}
        for r in rows:
            key = (r.original_text.strip().lower(), r.corrected_text.strip().lower())
            g = groups.get(key)
            if g is None:
                g = {
                    "original": r.original_text,
                    "corrected": r.corrected_text,
                    "count": 0,
                    "doctors": set(),
                    "sample_transcription_id": r.transcription_id,
                    "first_seen": r.created_at.isoformat() if r.created_at else None,
                    "latest_seen": r.created_at.isoformat() if r.created_at else None,
                }
                groups[key] = g
            g["count"] += 1
            if r.doctor_id:
                g["doctors"].add(str(r.doctor_id))
            if r.created_at:
                iso = r.created_at.isoformat()
                if iso > (g["latest_seen"] or ""):
                    g["latest_seen"] = iso
        result = []
        for g in groups.values():
            g["doctors"] = sorted(g["doctors"])
            result.append(g)
        # Sort by occurrence count desc, then latest_seen desc
        result.sort(key=lambda x: (-x["count"], -(len(x["latest_seen"] or ""))))
        return {"groups": result[:limit], "total_groups": len(result)}


@router.post("/correction-feedback/approve")
async def approve_correction_feedback(request: Request):
    """Approve a (original, corrected) pair → create a WordReplacement, mark rows accepted.

    Body: {original, corrected, doctor_id? (string for per-doctor, omit for global)}
    """
    body = await request.json()
    original = (body.get("original") or "").strip()
    corrected = (body.get("corrected") or "").strip()
    doctor_id = (body.get("doctor_id") or "").strip() or None
    if not original or not corrected:
        raise HTTPException(status_code=400, detail="'original' and 'corrected' required")

    with SessionLocal() as session:
        # Insert WordReplacement if no identical rule exists
        existing_rule = (
            session.query(WordReplacement)
            .filter(WordReplacement.original == original)
            .filter(WordReplacement.replacement == corrected)
            .filter(WordReplacement.doctor_id == doctor_id)
            .first()
        )
        if not existing_rule:
            session.add(WordReplacement(
                original=original,
                replacement=corrected,
                doctor_id=doctor_id,
            ))

        # Mark all feedback rows for this pair (any doctor) as accepted
        feedback_rows = (
            session.query(CorrectionFeedback)
            .filter(CorrectionFeedback.correction_type == "word")
            .filter(func.lower(CorrectionFeedback.original_text) == original.lower())
            .filter(func.lower(CorrectionFeedback.corrected_text) == corrected.lower())
            .filter(CorrectionFeedback.status == "pending")
        )
        updated = feedback_rows.update({"status": "accepted"}, synchronize_session=False)
        session.commit()

    try:
        from crowdtrans.transcriber.formatter import _clear_word_replacement_cache
        _clear_word_replacement_cache()
    except Exception:
        pass

    return {"status": "ok", "rule_created": existing_rule is None, "feedback_rows_updated": updated}


@router.post("/correction-feedback/reject")
async def reject_correction_feedback(request: Request):
    """Reject a (original, corrected) pair → mark all matching rows rejected.

    Rejected pairs are never re-suggested by record_edit_feedback().
    """
    body = await request.json()
    original = (body.get("original") or "").strip()
    corrected = (body.get("corrected") or "").strip()
    if not original or not corrected:
        raise HTTPException(status_code=400, detail="'original' and 'corrected' required")

    with SessionLocal() as session:
        updated = (
            session.query(CorrectionFeedback)
            .filter(CorrectionFeedback.correction_type == "word")
            .filter(func.lower(CorrectionFeedback.original_text) == original.lower())
            .filter(func.lower(CorrectionFeedback.corrected_text) == corrected.lower())
            .filter(CorrectionFeedback.status == "pending")
            .update({"status": "rejected"}, synchronize_session=False)
        )
        session.commit()
    return {"status": "ok", "rows_updated": updated}


@router.get("/templates")
def list_templates(
    doctor_id: str = Query("", description="Filter by doctor id"),
    doctor_surname: str = Query("", description="Filter by doctor family name"),
    modality: str = Query("", description="Filter by modality code"),
    body_part: str = Query("", description="Filter by body part"),
    procedure: str = Query("", description="Substring match on procedure description"),
    q: str = Query("", description="Substring match on template text"),
    source: str = Query("", description="Filter by source: mined_existing or karisma_library"),
    enabled_only: bool = Query(True),
    limit: int = Query(25, ge=1, le=200),
):
    """List report templates from the local library.

    Templates are ranked by source_count (popularity) so the most-used templates
    surface first. Used by the SpeechMike agent and the web UI.
    """
    with SessionLocal() as session:
        query = session.query(ReportTemplate)
        if enabled_only:
            query = query.filter(ReportTemplate.enabled.is_(True))
        if doctor_id:
            query = query.filter(ReportTemplate.doctor_id == doctor_id)
        if doctor_surname:
            query = query.filter(ReportTemplate.doctor_surname.ilike(f"%{doctor_surname}%"))
        if modality:
            query = query.filter(ReportTemplate.modality_code == modality)
        if body_part:
            query = query.filter(ReportTemplate.body_part.ilike(f"%{body_part}%"))
        if procedure:
            query = query.filter(ReportTemplate.procedure_description.ilike(f"%{procedure}%"))
        if q:
            query = query.filter(ReportTemplate.template_text.ilike(f"%{q}%"))
        if source:
            query = query.filter(ReportTemplate.source == source)

        rows = (
            query.order_by(ReportTemplate.source_count.desc(), ReportTemplate.last_seen_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "templates": [
                {
                    "id": r.id,
                    "doctor_id": r.doctor_id,
                    "doctor_surname": r.doctor_surname,
                    "modality_code": r.modality_code,
                    "body_part": r.body_part,
                    "procedure_code": r.procedure_code,
                    "procedure_description": r.procedure_description,
                    "template_name": r.template_name,
                    "template_text": r.template_text,
                    "source": r.source,
                    "source_count": r.source_count,
                    "last_seen_at": r.last_seen_at.isoformat() if r.last_seen_at else None,
                }
                for r in rows
            ]
        }


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
