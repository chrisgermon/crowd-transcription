"""Main transcription polling loop — multi-site (Visage + Karisma)."""

import datetime
import logging
import signal
import time
from pathlib import Path

from crowdtrans.config import SiteConfig, settings
from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal, get_db
from crowdtrans.models import Transcription, Watermark
from crowdtrans.transcriber.deepgram_client import transcribe_buffer, transcribe_file
from crowdtrans.transcriber.formatter import format_transcript, format_transcript_hybrid
from crowdtrans.transcriber.keyterms import get_keyterms

logger = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down gracefully...", signum)
    _shutdown = True


def _sleep_interruptible(seconds: int):
    """Sleep in 1-second increments for responsive shutdown."""
    for _ in range(seconds):
        if _shutdown:
            return
        time.sleep(1)


# ---------------------------------------------------------------------------
# Visage helpers
# ---------------------------------------------------------------------------

def _resolve_visage_audio(site: SiteConfig, relative_path: str | None, basename: str) -> Path | None:
    if not relative_path or not site.audio_mount_path:
        return None
    mount = Path(site.audio_mount_path)
    path = mount / relative_path / f"{basename}.opus"
    if path.exists():
        return path
    path_bare = mount / relative_path / basename
    if path_bare.exists():
        return path_bare
    return None


def _discover_visage(session, site: SiteConfig, wm: Watermark) -> int:
    from crowdtrans.visage import fetch_new_dictations

    rows = fetch_new_dictations(site, wm.last_dictation_id, site.batch_size)
    if not rows:
        return 0

    count = 0
    max_id = wm.last_dictation_id
    for row in rows:
        dictation_id = row["dictation_id"]
        existing = (
            session.query(Transcription)
            .filter_by(site_id=site.site_id, source_dictation_id=dictation_id)
            .first()
        )
        if existing:
            max_id = max(max_id, dictation_id)
            continue

        t = Transcription(
            site_id=site.site_id,
            source_dictation_id=dictation_id,
            audio_basename=row["basename"],
            audio_relative_path=row.get("relative_path"),
            audio_mime_type=row.get("mime_type"),
            audio_duration_ms=row.get("duration"),
            patient_id=str(row["patient_id"]) if row.get("patient_id") else None,
            patient_ur=row.get("patient_ur"),
            patient_title=row.get("patient_title"),
            patient_given_names=row.get("patient_given_names"),
            patient_family_name=row.get("patient_family_name"),
            patient_dob=str(row["patient_dob"]) if row.get("patient_dob") else None,
            order_id=str(row["order_id"]) if row.get("order_id") else None,
            accession_number=row.get("accession_number"),
            complaint=row.get("complaint"),
            procedure_id=str(row["procedure_id"]) if row.get("procedure_id") else None,
            procedure_description=row.get("procedure_description"),
            reason_for_study=row.get("reason_for_study"),
            modality_code=row.get("modality_code"),
            modality_name=row.get("modality_name"),
            body_part=row.get("body_part"),
            doctor_id=str(row["doctor_id"]) if row.get("doctor_id") else None,
            doctor_title=row.get("doctor_title"),
            doctor_given_names=row.get("doctor_given_names"),
            doctor_family_name=row.get("doctor_family_name"),
            referrer_id=str(row["referrer_id"]) if row.get("referrer_id") else None,
            referrer_title=row.get("referrer_title"),
            referrer_given_names=row.get("referrer_given_names"),
            referrer_family_name=row.get("referrer_family_name"),
            facility_id=str(row["facility_id"]) if row.get("facility_id") else None,
            facility_name=row.get("facility_name"),
            dictation_date=row.get("dictation_date"),
            status="pending",
            discovered_at=datetime.datetime.utcnow(),
        )
        session.add(t)
        max_id = max(max_id, dictation_id)
        count += 1

    wm.last_dictation_id = max_id
    wm.last_poll_at = datetime.datetime.utcnow()
    session.commit()

    if count > 0:
        logger.info("[%s] Discovered %d new dictations (watermark now %d)", site.site_id, count, max_id)
    return count


def _process_visage(session, site: SiteConfig, txn: Transcription) -> bool:
    audio_path = _resolve_visage_audio(site, txn.audio_relative_path, txn.audio_basename)
    if audio_path is None:
        txn.status = "skipped"
        txn.error_message = f"Audio file not found: {txn.audio_relative_path}/{txn.audio_basename}"
        session.commit()
        logger.warning("[%s] Skipping dictation %d: audio not found", site.site_id, txn.source_dictation_id)
        return False

    keyterms = _build_keyterms(txn)

    txn.status = "transcribing"
    txn.transcription_started_at = datetime.datetime.utcnow()
    session.commit()

    try:
        result = transcribe_file(audio_path, keyterms)
    except Exception as e:
        _mark_failed(session, site, txn, e)
        return False

    _store_result(session, site, txn, result)
    return True


# ---------------------------------------------------------------------------
# Karisma helpers
# ---------------------------------------------------------------------------

def _get_excluded_worksites() -> set[str]:
    """Load excluded worksite names from config (cached per poll cycle)."""
    store = get_config_store()
    raw = store.get_global("excluded_worksites") or ""
    return {s.strip() for s in raw.split(",") if s.strip()}


def _discover_karisma(session, site: SiteConfig, wm: Watermark) -> int:
    import json as _json
    from crowdtrans.karisma import (
        fetch_all_request_notes,
        fetch_new_dictations,
        fetch_patient_conditions,
    )

    rows = fetch_new_dictations(site, wm.last_dictation_id, site.batch_size)
    if not rows:
        return 0

    excluded = _get_excluded_worksites()

    count = 0
    skipped_sites = 0
    max_tk = wm.last_dictation_id
    for row in rows:
        tk = row["TransactionKey"]
        existing = (
            session.query(Transcription)
            .filter_by(site_id=site.site_id, source_dictation_id=tk)
            .first()
        )
        if existing:
            max_tk = max(max_tk, tk)
            continue

        # Skip excluded worksites
        worksite_name = row.get("WorkSiteName") or ""
        if worksite_name in excluded:
            max_tk = max(max_tk, tk)
            skipped_sites += 1
            continue

        # Modality — use Department if available, fall back to Modality table
        dept_code = row.get("DepartmentCode") or ""
        dept_name = row.get("DepartmentName") or ""
        if dept_code:
            modality_code = dept_code
            modality_name = dept_name
        else:
            modality_name = row.get("ModalityName") or ""
            modality_code = row.get("ModalityCode") or ""
            if not modality_code and modality_name:
                modality_code = _karisma_modality_to_code(modality_name)

        # Fetch all note types if we have a request key
        clinical_notes = None
        worksheet_notes = None
        order_notes = None
        request_key = row.get("RequestKey")
        if request_key:
            try:
                all_notes = fetch_all_request_notes(site, request_key)
                clinical_notes = all_notes.get("clinical") or clinical_notes
                worksheet_notes = all_notes.get("worksheet")
                order_notes = all_notes.get("order")
            except Exception:
                logger.debug("Could not fetch notes for request %s", request_key)

        # Fetch patient conditions
        patient_conditions_json = None
        patient_key = row.get("PatientKey")
        if patient_key:
            try:
                conditions = fetch_patient_conditions(site, patient_key)
                if conditions:
                    patient_conditions_json = _json.dumps(conditions)
            except Exception:
                logger.debug("Could not fetch conditions for patient %s", patient_key)

        t = Transcription(
            site_id=site.site_id,
            source_dictation_id=tk,
            audio_basename=None,
            extent_key=row.get("ContentKey"),
            extent_offset=None,
            extent_length=None,
            report_instance_key=row.get("ReportInstanceKey"),
            report_process_status=row.get("ReportProcessStatus"),
            patient_id=str(row["PatientKey"]) if row.get("PatientKey") else None,
            patient_ur=row.get("PatientId"),
            patient_title=row.get("PatientTitle"),
            patient_given_names=row.get("PatientFirstName"),
            patient_family_name=row.get("PatientLastName"),
            patient_dob=str(row["PatientDateOfBirth"]) if row.get("PatientDateOfBirth") else None,
            patient_conditions=patient_conditions_json,
            order_id=str(row["RequestKey"]) if row.get("RequestKey") else None,
            accession_number=row.get("InternalIdentifier"),
            internal_identifier=row.get("InternalIdentifier"),
            complaint=clinical_notes,
            worksheet_notes=worksheet_notes,
            order_notes=order_notes,
            procedure_id=str(row["ServiceKey"]) if row.get("ServiceKey") else None,
            procedure_description=row.get("ServiceName"),
            service_code=row.get("ServiceCode"),
            modality_code=modality_code,
            modality_name=modality_name,
            doctor_id=row.get("DictatingPractitionerCode"),
            doctor_title=row.get("DictatingPractitionerTitle"),
            doctor_given_names=row.get("DictatingPractitionerFirstName"),
            doctor_family_name=row.get("DictatingPractitionerSurname"),
            referrer_id=str(row["ReferringPractitionerKey"]) if row.get("ReferringPractitionerKey") else None,
            referrer_given_names=row.get("ReferringPractitionerFirstName"),
            referrer_family_name=row.get("ReferringPractitionerSurname"),
            facility_id=str(row["WorkSiteKey"]) if row.get("WorkSiteKey") else None,
            facility_name=row.get("WorkSiteName"),
            facility_code=row.get("WorkSiteCode"),
            priority_name=row.get("PriorityName"),
            priority_rank=row.get("PriorityRank"),
            dictation_date=row.get("CreatedTime"),
            status="pending",
            discovered_at=datetime.datetime.utcnow(),
        )
        session.add(t)
        max_tk = max(max_tk, tk)
        count += 1

    wm.last_dictation_id = max_tk
    wm.last_poll_at = datetime.datetime.utcnow()
    session.commit()

    if count > 0 or skipped_sites > 0:
        logger.info(
            "[%s] Discovered %d new dictations, skipped %d excluded worksites (watermark now %d)",
            site.site_id, count, skipped_sites, max_tk,
        )
    return count


def _process_karisma(session, site: SiteConfig, txn: Transcription) -> bool:
    from crowdtrans.karisma import fetch_audio_blob, fetch_existing_report_content
    from crowdtrans.transcriber.audio import process_karisma_blob

    if not txn.extent_key:
        txn.status = "skipped"
        txn.error_message = "No ExtentKey for audio blob"
        session.commit()
        return False

    raw_blob = fetch_audio_blob(site, txn.extent_key)
    if raw_blob is None:
        txn.status = "skipped"
        txn.error_message = f"Audio blob not found for ExtentKey {txn.extent_key}"
        session.commit()
        logger.warning("[%s] Skipping dictation %d: blob not found", site.site_id, txn.source_dictation_id)
        return False

    audio = process_karisma_blob(raw_blob, txn.extent_offset, txn.extent_length, txn.source_dictation_id)
    if audio is None:
        txn.status = "failed"
        txn.error_message = "Audio decompression failed"
        txn.retry_count += 1
        session.commit()
        return False

    # Fetch pre-populated report content (sonographer template) if available
    if txn.report_instance_key and not txn.existing_report_text:
        try:
            existing = fetch_existing_report_content(
                site, txn.report_instance_key, txn.source_dictation_id,
            )
            if existing:
                txn.existing_report_text = existing
                logger.info(
                    "[%s] Found existing report content for dictation %d (%d chars)",
                    site.site_id, txn.source_dictation_id, len(existing),
                )
        except Exception:
            logger.debug("Could not fetch existing report for dictation %d", txn.source_dictation_id)

    keyterms = _build_keyterms(txn)

    txn.status = "transcribing"
    txn.transcription_started_at = datetime.datetime.utcnow()
    session.commit()

    try:
        result = transcribe_buffer(
            audio.data,
            content_type=audio.content_type,
            keyterms=keyterms,
            label=f"karisma-{txn.source_dictation_id}",
        )
    except Exception as e:
        _mark_failed(session, site, txn, e)
        return False

    _store_result(session, site, txn, result)
    return True


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_KARISMA_MODALITY_MAP = {
    "ultrasound": "US", "ct": "CT", "mri": "MR", "magnetic resonance": "MR",
    "x-ray": "CR", "radiograph": "CR", "mammography": "MG", "mammo": "MG",
    "nuclear medicine": "NM", "bone densitometry": "BMD", "dexa": "BMD",
    "fluoroscopy": "DSA", "angiography": "DSA",
}


def _karisma_modality_to_code(modality_name: str) -> str:
    """Map Karisma's modality name to a standard code for keyterm matching."""
    name_lower = modality_name.strip().lower()
    # Direct match first
    if name_lower in _KARISMA_MODALITY_MAP:
        return _KARISMA_MODALITY_MAP[name_lower]
    # Partial match
    for key, code in _KARISMA_MODALITY_MAP.items():
        if key in name_lower:
            return code
    # If it's already a short code like "CT", "US", return uppercase
    if len(modality_name) <= 4:
        return modality_name.upper()
    return modality_name


def _build_keyterms(txn: Transcription) -> list[str]:
    patient_parts = []
    if txn.patient_given_names:
        patient_parts.extend(txn.patient_given_names.split())
    if txn.patient_family_name:
        patient_parts.append(txn.patient_family_name)

    return get_keyterms(
        modality_code=txn.modality_code,
        patient_name_parts=patient_parts if patient_parts else None,
        doctor_name=txn.doctor_family_name,
        referrer_name=txn.referrer_family_name,
        procedure_description=txn.procedure_description,
        doctor_id=txn.doctor_id,
    )


def _mark_failed(session, site: SiteConfig, txn: Transcription, error: Exception):
    txn.status = "failed"
    txn.error_message = str(error)[:2000]
    txn.retry_count += 1
    txn.transcription_completed_at = datetime.datetime.utcnow()
    session.commit()
    logger.error("[%s] Failed to transcribe dictation %d: %s", site.site_id, txn.source_dictation_id, error)


def _store_result(session, site: SiteConfig, txn: Transcription, result):
    txn.status = "complete"
    txn.transcript_text = result.transcript_text

    # Hybrid formatter: regex always runs, LLM runs if enabled
    # Build patient name for stripping from transcript body
    _name_parts = [txn.patient_given_names, txn.patient_family_name]
    _patient_name = " ".join(p for p in _name_parts if p) or None

    regex_text, llm_result, method = format_transcript_hybrid(
        result.transcript_text,
        modality_code=txn.modality_code,
        procedure_description=txn.procedure_description,
        clinical_history=txn.complaint,
        doctor_id=txn.doctor_id,
        patient_name=_patient_name,
        patient_ur=txn.patient_ur,
        existing_report_text=txn.existing_report_text,
    )
    txn.formatted_text = regex_text
    txn.formatting_method = method
    if llm_result is not None:
        txn.llm_formatted_text = llm_result.formatted_text
        txn.llm_model_used = llm_result.model
        txn.llm_format_duration_ms = llm_result.duration_ms
        txn.llm_input_tokens = llm_result.input_tokens
        txn.llm_output_tokens = llm_result.output_tokens

    txn.confidence = result.confidence
    txn.deepgram_request_id = result.request_id
    txn.processing_duration_ms = result.processing_duration_ms
    txn.words_json = result.words_json
    txn.paragraphs_json = result.paragraphs_json
    txn.transcription_completed_at = datetime.datetime.utcnow()
    txn.error_message = None
    session.commit()

    logger.info(
        "[%s] Transcribed dictation %d (%s) — %.1f%% confidence, %dms, format=%s",
        site.site_id,
        txn.source_dictation_id,
        txn.accession_number or "no accession",
        (txn.confidence or 0) * 100,
        txn.processing_duration_ms or 0,
        method,
    )


# ---------------------------------------------------------------------------
# Patient data backfill
# ---------------------------------------------------------------------------
_last_backfill = 0.0
_BACKFILL_INTERVAL = 600  # 10 minutes


def _backfill_patient_data(session, site: SiteConfig):
    """Backfill patient/request data for Karisma dictations that were
    discovered before their Report link existed in the database."""
    import time
    global _last_backfill

    now = time.time()
    if now - _last_backfill < _BACKFILL_INTERVAL:
        return
    _last_backfill = now

    if site.ris_type != "karisma":
        return

    from crowdtrans.karisma import fetch_all_request_notes

    # Find transcriptions missing clinical notes (notes weren't available at discovery)
    missing = (
        session.query(Transcription)
        .filter(
            Transcription.site_id == site.site_id,
            Transcription.complaint.is_(None),
            Transcription.order_id.isnot(None),
        )
        .limit(200)
        .all()
    )
    if not missing:
        return

    updated = 0
    reformatted = 0
    for txn in missing:
        request_key = int(txn.order_id) if txn.order_id else None
        if not request_key:
            continue

        try:
            all_notes = fetch_all_request_notes(site, request_key)
        except Exception:
            continue

        if not all_notes:
            continue

        clinical = all_notes.get("clinical")
        if clinical:
            txn.complaint = clinical
        worksheet = all_notes.get("worksheet")
        if worksheet:
            txn.worksheet_notes = worksheet
        order = all_notes.get("order")
        if order:
            txn.order_notes = order

        updated += 1

        # Re-format transcript with the new context data
        if txn.transcript_text and txn.status == "complete" and clinical:
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
            reformatted += 1

    if updated:
        session.commit()
        logger.info(
            "[%s] Backfill: updated %d dictations with patient data (%d re-formatted)",
            site.site_id, updated, reformatted,
        )


# ---------------------------------------------------------------------------
# Dispatch: route to correct RIS backend
# ---------------------------------------------------------------------------

_DISCOVER = {
    "visage": _discover_visage,
    "karisma": _discover_karisma,
}

_PROCESS = {
    "visage": _process_visage,
    "karisma": _process_karisma,
}


def _discover(session, site: SiteConfig) -> int:
    wm = session.query(Watermark).filter_by(site_id=site.site_id).first()
    if not wm:
        logger.error("No watermark for site %s", site.site_id)
        return 0
    fn = _DISCOVER.get(site.ris_type)
    if not fn:
        logger.error("Unknown RIS type: %s", site.ris_type)
        return 0
    return fn(session, site, wm)


# ProcessStatus values that indicate Karisma has accepted typed report text.
# Empirically verified: status 3/4/7 == typed (every sampled report had content);
# status 0/1 == draft/in-progress.
_VERIFIED_PROCESS_STATUSES = {3, 4, 7}


def _sync_ready_worklist(session, site: SiteConfig, batch_size: int = 2000) -> int:
    """For each 'ready' worklist item, check Karisma for a finalised report.

    Two-pass check per item, in order of precedence:
      1. By accession (Request.Record.InternalIdentifier) — finds the typed
         report even when the typist abandoned our dictation and typed fresh
         into a new Report.Instance under the same Request.Record.
      2. By dictation TransactionKey (Path 1 + Path 2 dual join) — fallback
         for orphan dictations that have no accession.

    Marks items verified when ProcessStatus is in {3,4,7}. Also refreshes
    report_process_status / report_instance_key / priority on the local row.
    """
    if site.ris_type != "karisma":
        return 0
    try:
        from crowdtrans.karisma import (
            fetch_worklist_sync_state,
            fetch_worklist_sync_state_by_accession,
        )
    except Exception:
        return 0

    ready_items = (
        session.query(Transcription)
        .filter(
            Transcription.site_id == site.site_id,
            Transcription.status == "complete",
            Transcription.worklist_status == "ready",
            Transcription.source_dictation_id.isnot(None),
        )
        .limit(batch_size)
        .all()
    )
    if not ready_items:
        return 0

    accessions = list({
        t.internal_identifier for t in ready_items if t.internal_identifier
    })
    tk_orphans = [
        int(t.source_dictation_id)
        for t in ready_items
        if not t.internal_identifier
    ]
    try:
        by_acc = fetch_worklist_sync_state_by_accession(site, accessions)
    except Exception:
        logger.exception("[%s] worklist sync: by-accession lookup failed", site.site_id)
        by_acc = {}
    try:
        by_tk = fetch_worklist_sync_state(site, tk_orphans) if tk_orphans else {}
    except Exception:
        logger.exception("[%s] worklist sync: by-TK lookup failed", site.site_id)
        by_tk = {}

    moved = 0
    refreshed = 0
    now = datetime.datetime.utcnow()
    for t in ready_items:
        entry = None
        if t.internal_identifier:
            entry = by_acc.get(t.internal_identifier)
        if entry is None:
            entry = by_tk.get(int(t.source_dictation_id))
        if not entry:
            continue
        status = entry.get("process_status")
        rik = entry.get("report_instance_key")
        pname = entry.get("priority_name")
        prank = entry.get("priority_rank")

        # Update fields we now know about so downstream views stay fresh
        changed = False
        if rik and t.report_instance_key != rik:
            t.report_instance_key = rik
            changed = True
        if status is not None and t.report_process_status != status:
            t.report_process_status = status
            changed = True
        if pname and t.priority_name != pname:
            t.priority_name = pname
            changed = True
        if prank is not None and t.priority_rank != prank:
            t.priority_rank = prank
            changed = True
        if changed:
            refreshed += 1

        if status not in _VERIFIED_PROCESS_STATUSES:
            continue
        t.worklist_status = "verified"
        t.verified_at = now
        t.verified_by = "karisma"
        moved += 1
    if moved or refreshed:
        logger.info(
            "[%s] worklist sync: %d verified (typed in Karisma), %d field refreshes (batch=%d)",
            site.site_id, moved, refreshed, len(ready_items),
        )
    return moved


def _process_pending(session, site: SiteConfig) -> int:
    fn = _PROCESS.get(site.ris_type)
    if not fn:
        return 0

    # Order: urgent dictations first (lowest priority_rank wins), then newest
    # dictation first within each priority bucket. NULL priority sorts last so
    # orphans/unknown-priority items don't preempt prioritised work.
    pending = (
        session.query(Transcription)
        .filter(Transcription.site_id == site.site_id, Transcription.status == "pending")
        .order_by(
            Transcription.priority_rank.asc().nullslast(),
            Transcription.source_dictation_id.desc(),
        )
        .limit(site.batch_size)
        .all()
    )
    if not pending:
        return 0

    success = 0
    for txn in pending:
        if _shutdown:
            break
        try:
            if fn(session, site, txn):
                success += 1
        except Exception:
            logger.exception("[%s] Unexpected error processing dictation %d", site.site_id, txn.source_dictation_id)
    return success


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def run(site_id: str | None = None):
    """Main entry point. If site_id is None, processes all enabled sites."""
    global _shutdown
    _shutdown = False

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    store = get_config_store()
    initial_sites = store.get_enabled_site_configs()
    if site_id:
        initial_sites = [s for s in initial_sites if s.site_id == site_id]
        if not initial_sites:
            logger.error("Site '%s' not found or not enabled", site_id)
            return

    site_names = ", ".join(s.site_id for s in initial_sites)
    logger.info("Transcription service starting for sites: %s", site_names)

    while not _shutdown:
        # Re-read site configs each cycle so changes take effect without restart
        sites = store.get_enabled_site_configs()
        if site_id:
            sites = [s for s in sites if s.site_id == site_id]

        any_work = False
        for site in sites:
            if _shutdown:
                break
            try:
                with get_db() as session:
                    discovered = _discover(session, site)
                    processed = _process_pending(session, site)
                    moved = _sync_ready_worklist(session, site)
                    if discovered > 0 or processed > 0 or moved > 0:
                        any_work = True
                    # Backfill disabled — only process new dictations
                    # _backfill_patient_data(session, site)
            except Exception:
                logger.exception("[%s] Error in polling loop", site.site_id)

        if not any_work and not _shutdown:
            # Use shortest poll interval across active sites
            interval = min(s.poll_interval_seconds for s in sites)
            logger.debug("No work across all sites, sleeping %ds", interval)
            _sleep_interruptible(interval)

    logger.info("Transcription service stopped")
