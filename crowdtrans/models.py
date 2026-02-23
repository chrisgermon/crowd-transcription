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

    # Doctor (reporting/dictating radiologist)
    doctor_id = Column(String, nullable=True)
    doctor_title = Column(String, nullable=True)
    doctor_given_names = Column(String, nullable=True)
    doctor_family_name = Column(String, nullable=True)

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
    transcript_text = Column(Text, nullable=True)
    confidence = Column(Float, nullable=True)
    deepgram_request_id = Column(String, nullable=True)
    processing_duration_ms = Column(Integer, nullable=True)
    words_json = Column(Text, nullable=True)
    paragraphs_json = Column(Text, nullable=True)

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
    )
