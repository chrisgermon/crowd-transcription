import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class GlobalSetting(Base):
    __tablename__ = "global_settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class SiteConfigRow(Base):
    __tablename__ = "site_configs"

    site_id = Column(String, primary_key=True)
    site_name = Column(String, nullable=False)
    ris_type = Column(String, nullable=False)  # "visage" or "karisma"
    enabled = Column(Boolean, nullable=False, default=True)

    db_host = Column(String, nullable=False)
    db_port = Column(Integer, nullable=False)
    db_name = Column(String, nullable=False)
    db_user = Column(String, nullable=False)
    db_password = Column(String, nullable=False)

    audio_source = Column(String, nullable=False)  # "nfs" or "sql_blob"
    audio_mount_path = Column(String, nullable=True)

    poll_interval_seconds = Column(Integer, nullable=False, default=30)
    batch_size = Column(Integer, nullable=False, default=10)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class Watermark(Base):
    __tablename__ = "watermark"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String, nullable=False, unique=True)
    last_dictation_id = Column(BigInteger, nullable=False, default=0)
    last_poll_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())


class Transcription(Base):
    __tablename__ = "transcriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Site identification
    site_id = Column(String, nullable=False)

    # Source (RIS dictation — generic across Visage/Karisma)
    source_dictation_id = Column(BigInteger, nullable=False)  # Visage: dictation.id, Karisma: TransactionKey
    audio_basename = Column(String, nullable=True)
    audio_relative_path = Column(String, nullable=True)
    audio_mime_type = Column(String, nullable=True)
    audio_duration_ms = Column(Integer, nullable=True)

    # Karisma-specific audio blob references
    extent_key = Column(BigInteger, nullable=True)
    extent_offset = Column(Integer, nullable=True)
    extent_length = Column(Integer, nullable=True)

    # Patient
    patient_id = Column(String, nullable=True)
    patient_ur = Column(String, nullable=True)
    patient_title = Column(String, nullable=True)
    patient_given_names = Column(String, nullable=True)
    patient_family_name = Column(String, nullable=True)
    patient_dob = Column(String, nullable=True)

    # Karisma report linkage
    report_instance_key = Column(BigInteger, nullable=True)
    report_process_status = Column(Integer, nullable=True)
    final_report_text = Column(Text, nullable=True)      # final typed report from Karisma
    existing_report_text = Column(Text, nullable=True)    # pre-populated template content (before dictation)

    # Order / Request
    order_id = Column(String, nullable=True)
    accession_number = Column(String, nullable=True)
    internal_identifier = Column(String, nullable=True)
    complaint = Column(Text, nullable=True)  # clinical notes

    # Procedure / Service
    procedure_id = Column(String, nullable=True)
    procedure_description = Column(Text, nullable=True)
    service_code = Column(String, nullable=True)
    reason_for_study = Column(Text, nullable=True)
    modality_code = Column(String, nullable=True)
    modality_name = Column(String, nullable=True)
    body_part = Column(String, nullable=True)

    # Patient extended data
    patient_conditions = Column(Text, nullable=True)  # JSON list of conditions
    worksheet_notes = Column(Text, nullable=True)
    order_notes = Column(Text, nullable=True)

    # Doctor (reporting/dictating radiologist)
    doctor_id = Column(String, nullable=True)
    doctor_title = Column(String, nullable=True)
    doctor_given_names = Column(String, nullable=True)
    doctor_family_name = Column(String, nullable=True)
    doctor_qualifications = Column(String, nullable=True)
    doctor_user_key = Column(BigInteger, nullable=True)

    # Referrer
    referrer_id = Column(String, nullable=True)
    referrer_title = Column(String, nullable=True)
    referrer_given_names = Column(String, nullable=True)
    referrer_family_name = Column(String, nullable=True)

    # Facility / WorkSite
    facility_id = Column(String, nullable=True)
    facility_name = Column(String, nullable=True)
    facility_code = Column(String, nullable=True)

    # Status
    status = Column(String, nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)

    # Transcription results
    transcript_text = Column(Text, nullable=True)  # raw Deepgram output
    formatted_text = Column(Text, nullable=True)    # post-processed with headings (regex)
    llm_formatted_text = Column(Text, nullable=True)  # LLM-formatted output
    formatting_method = Column(String, nullable=True, default="regex")  # regex, llm, hybrid
    llm_model_used = Column(String, nullable=True)
    llm_format_duration_ms = Column(Integer, nullable=True)
    llm_input_tokens = Column(Integer, nullable=True)
    llm_output_tokens = Column(Integer, nullable=True)
    confidence = Column(Float, nullable=True)
    deepgram_request_id = Column(String, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)
    words_json = Column(Text, nullable=True)
    paragraphs_json = Column(Text, nullable=True)

    # Worklist tracking
    worklist_status = Column(String, nullable=False, default="ready")  # ready, copied, verified
    copied_at = Column(DateTime, nullable=True)
    verified_at = Column(DateTime, nullable=True)
    verified_by = Column(String, nullable=True)
    final_text = Column(Text, nullable=True)  # frozen formatted_text + signature at verify time

    # Priority (from Karisma Request.PriorityType — ReportCompletion)
    priority_name = Column(String, nullable=True)
    priority_rank = Column(Integer, nullable=True)  # 1=most urgent (Immediate), 2=ASAP, 3=Same_Day, 4+=Routine/Low

    # Timestamps
    dictation_date = Column(DateTime, nullable=True)
    discovered_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    transcription_started_at = Column(DateTime, nullable=True)
    transcription_completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("site_id", "source_dictation_id", name="uq_site_dictation"),
        Index("ix_site_id", "site_id"),
        Index("ix_status", "status"),
        Index("ix_accession_number", "accession_number"),
        Index("ix_patient_family_name", "patient_family_name"),
        Index("ix_doctor_family_name", "doctor_family_name"),
        Index("ix_modality_code", "modality_code"),
        Index("ix_dictation_date", "dictation_date"),
        Index("ix_site_source", "site_id", "source_dictation_id"),
        Index("ix_worklist_status", "worklist_status"),
        Index("ix_priority_rank", "priority_rank"),
    )


class TranscriptionEdit(Base):
    """Audit trail for manual edits to formatted_text."""
    __tablename__ = "transcription_edits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transcription_id = Column(Integer, nullable=False)
    original_text = Column(Text, nullable=False)
    edited_text = Column(Text, nullable=False)
    editor = Column(String, nullable=True)  # username or identifier
    edit_summary = Column(String, nullable=True)  # optional description of what changed
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    __table_args__ = (
        Index("ix_te_transcription", "transcription_id"),
        Index("ix_te_created", "created_at"),
    )


class PendingStudy(Base):
    """Cached undictated studies from Karisma — registered but no report yet."""
    __tablename__ = "pending_studies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String, nullable=False)
    service_key = Column(BigInteger, nullable=False)  # Karisma Request.Service Key
    request_key = Column(BigInteger, nullable=True)

    accession_number = Column(String, nullable=True)
    internal_identifier = Column(String, nullable=True)

    patient_title = Column(String, nullable=True)
    patient_first_name = Column(String, nullable=True)
    patient_last_name = Column(String, nullable=True)
    patient_id = Column(String, nullable=True)
    patient_dob = Column(String, nullable=True)

    service_name = Column(String, nullable=True)
    service_code = Column(String, nullable=True)
    modality_code = Column(String, nullable=True)
    modality_name = Column(String, nullable=True)

    doctor_code = Column(String, nullable=True)
    doctor_title = Column(String, nullable=True)
    doctor_first_name = Column(String, nullable=True)
    doctor_surname = Column(String, nullable=True)

    facility_name = Column(String, nullable=True)
    facility_code = Column(String, nullable=True)

    registered_date = Column(DateTime, nullable=True)
    scheduled_date = Column(DateTime, nullable=True)

    synced_at = Column(DateTime, nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("site_id", "service_key", name="uq_pending_site_service"),
        Index("ix_pending_modality", "modality_code"),
        Index("ix_pending_facility", "facility_name"),
        Index("ix_pending_doctor", "doctor_surname"),
        Index("ix_pending_registered", "registered_date"),
    )


class Radiologist(Base):
    """Radiologist profiles with signature blocks for report formatting."""
    __tablename__ = "radiologists"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doctor_code = Column(String, nullable=True, unique=True)  # Karisma practitioner code
    title = Column(String, nullable=True)  # Dr
    first_name = Column(String, nullable=False)
    surname = Column(String, nullable=False)
    qualifications = Column(String, nullable=True)  # e.g. MBBS FRANZCR
    role = Column(String, nullable=True)  # e.g. Consultant Radiologist
    signature_text = Column(Text, nullable=True)  # full signature block
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_rad_surname", "surname"),
    )


class WordReplacement(Base):
    """User-defined word replacements, optionally per-doctor."""
    __tablename__ = "word_replacements"

    id = Column(Integer, primary_key=True, autoincrement=True)
    original = Column(String, nullable=False)  # word/phrase to find (case-insensitive)
    replacement = Column(String, nullable=False)  # what to replace it with
    doctor_id = Column(String, nullable=True)  # NULL = global, otherwise doctor code
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_wr_doctor", "doctor_id"),
        Index("ix_wr_original", "original"),
    )


class ReportTemplate(Base):
    """Local library of report templates, seeded from Karisma or mined from existing_report_text."""
    __tablename__ = "report_templates"

    id = Column(Integer, primary_key=True, autoincrement=True)

    doctor_id = Column(String, nullable=True)
    doctor_surname = Column(String, nullable=True)

    modality_code = Column(String, nullable=True)
    body_part = Column(String, nullable=True)
    procedure_code = Column(String, nullable=True)
    procedure_description = Column(String, nullable=True)

    template_name = Column(String, nullable=True)
    template_text = Column(Text, nullable=False)
    text_hash = Column(String, nullable=False)

    source = Column(String, nullable=False)  # 'mined_existing' or 'karisma_library'
    source_count = Column(Integer, nullable=False, default=1)
    last_seen_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    enabled = Column(Boolean, nullable=False, default=True)

    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("doctor_id", "procedure_description", "text_hash", name="uq_template"),
        Index("ix_tpl_doctor", "doctor_id"),
        Index("ix_tpl_modality", "modality_code"),
        Index("ix_tpl_proc", "procedure_description"),
        Index("ix_tpl_enabled", "enabled"),
    )


class CorrectionFeedback(Base):
    __tablename__ = "correction_feedback"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transcription_id = Column(Integer, nullable=False)
    doctor_id = Column(String, nullable=True)
    correction_type = Column(String, nullable=False)  # word, section, style, structure
    original_text = Column(Text, nullable=False)
    corrected_text = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending")  # pending, accepted, rejected
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)

    __table_args__ = (
        Index("ix_cf_transcription", "transcription_id"),
        Index("ix_cf_doctor", "doctor_id"),
        Index("ix_cf_status", "status"),
    )
