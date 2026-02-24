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
from crowdtrans.transcriber.formatter import format_transcript
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

def _discover_karisma(session, site: SiteConfig, wm: Watermark) -> int:
    from crowdtrans.karisma import fetch_new_dictations

    rows = fetch_new_dictations(site, wm.last_dictation_id, site.batch_size)
    if not rows:
        return 0

    count = 0
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

        # Parse dictating practitioner name into parts
        doc_name = row.get("DictatingPractitionerName") or ""
        doc_parts = doc_name.rsplit(" ", 1) if doc_name else ["", ""]
        doc_given = doc_parts[0] if len(doc_parts) > 1 else ""
        doc_family = doc_parts[-1]

        # Karisma ModalityName is the full name (e.g. "Ultrasound", "CT")
        # Map common names to standard codes for keyterm matching
        modality_name = row.get("ModalityName") or ""
        modality_code = _karisma_modality_to_code(modality_name)

        t = Transcription(
            site_id=site.site_id,
            source_dictation_id=tk,
            audio_basename=None,
            extent_key=row.get("ExtentKey"),
            extent_offset=row.get("ExtentOffset"),
            extent_length=row.get("ExtentLength"),
            patient_id=str(row["PatientKey"]) if row.get("PatientKey") else None,
            patient_ur=row.get("PatientId"),
            patient_title=row.get("PatientTitle"),
            patient_given_names=row.get("PatientFirstName"),
            patient_family_name=row.get("PatientLastName"),
            patient_dob=str(row["PatientDateOfBirth"]) if row.get("PatientDateOfBirth") else None,
            order_id=str(row["RequestKey"]) if row.get("RequestKey") else None,
            accession_number=row.get("AccessionNumber"),
            internal_identifier=row.get("InternalIdentifier"),
            complaint=row.get("ClinicalNotes"),
            procedure_id=str(row["ServiceKey"]) if row.get("ServiceKey") else None,
            procedure_description=row.get("ServiceName"),
            service_code=row.get("ServiceCode"),
            modality_code=modality_code,
            modality_name=modality_name,
            doctor_id=row.get("DictatingPractitionerCode"),
            doctor_given_names=doc_given,
            doctor_family_name=doc_family,
            referrer_id=str(row["ReferringPractitionerKey"]) if row.get("ReferringPractitionerKey") else None,
            referrer_family_name=row.get("ReferringPractitionerName"),
            facility_id=str(row["WorkSiteKey"]) if row.get("WorkSiteKey") else None,
            facility_name=row.get("WorkSiteName"),
            facility_code=row.get("WorkSiteCode"),
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

    if count > 0:
        logger.info("[%s] Discovered %d new dictations (watermark now %d)", site.site_id, count, max_tk)
    return count


def _process_karisma(session, site: SiteConfig, txn: Transcription) -> bool:
    from crowdtrans.karisma import fetch_audio_blob
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
    txn.formatted_text = format_transcript(
        result.transcript_text,
        modality_code=txn.modality_code,
        procedure_description=txn.procedure_description,
        clinical_history=txn.complaint,
        doctor_id=txn.doctor_id,
    )
    txn.confidence = result.confidence
    txn.deepgram_request_id = result.request_id
    txn.processing_duration_ms = result.processing_duration_ms
    txn.words_json = result.words_json
    txn.paragraphs_json = result.paragraphs_json
    txn.transcription_completed_at = datetime.datetime.utcnow()
    txn.error_message = None
    session.commit()

    logger.info(
        "[%s] Transcribed dictation %d (%s) — %.1f%% confidence, %dms",
        site.site_id,
        txn.source_dictation_id,
        txn.accession_number or "no accession",
        (txn.confidence or 0) * 100,
        txn.processing_duration_ms or 0,
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


def _process_pending(session, site: SiteConfig) -> int:
    fn = _PROCESS.get(site.ris_type)
    if not fn:
        return 0

    pending = (
        session.query(Transcription)
        .filter(Transcription.site_id == site.site_id, Transcription.status == "pending")
        .order_by(Transcription.source_dictation_id.asc())
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
                    if discovered > 0 or processed > 0:
                        any_work = True
            except Exception:
                logger.exception("[%s] Error in polling loop", site.site_id)

        if not any_work and not _shutdown:
            # Use shortest poll interval across active sites
            interval = min(s.poll_interval_seconds for s in sites)
            logger.debug("No work across all sites, sleeping %ds", interval)
            _sleep_interruptible(interval)

    logger.info("Transcription service stopped")
