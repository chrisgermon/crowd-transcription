"""Tests for query structure, keyterms, and audio processing."""

import pytest


def test_visage_query_is_valid_sql():
    from crowdtrans.visage import DICTATION_QUERY

    assert "dictation d" in DICTATION_QUERY
    assert "dictation_procedure dp" in DICTATION_QUERY
    assert "procedure_ p" in DICTATION_QUERY
    assert "ORDER BY d.id ASC" in DICTATION_QUERY
    assert "LIMIT %s" in DICTATION_QUERY


def test_karisma_query_is_valid_sql():
    from crowdtrans.karisma import DICTATION_QUERY

    assert "[Version].[Karisma.Dictation.Instance]" in DICTATION_QUERY
    assert "[Version].[Karisma.Patient.Record]" in DICTATION_QUERY
    assert "TransactionKey" in DICTATION_QUERY
    assert "ORDER BY DI.TransactionKey ASC" in DICTATION_QUERY


def test_keyterms_base():
    from crowdtrans.transcriber.keyterms import get_keyterms

    terms = get_keyterms()
    assert "radiology" in terms
    assert "impression" in terms
    assert len(terms) <= 100


def test_keyterms_modality_boost():
    from crowdtrans.transcriber.keyterms import get_keyterms

    us_terms = get_keyterms(modality_code="US")
    assert any("Doppler" in t for t in us_terms)

    ct_terms = get_keyterms(modality_code="CT")
    assert any("Hounsfield" in t for t in ct_terms)

    mr_terms = get_keyterms(modality_code="MR")
    assert any("FLAIR" in t for t in mr_terms)


def test_keyterms_cap():
    from crowdtrans.transcriber.keyterms import get_keyterms

    terms = get_keyterms(
        modality_code="CT",
        patient_name_parts=["John", "Alexander", "Smith-Williams"],
        doctor_name="Dr Radiologist",
        referrer_name="Dr Referrer",
        procedure_description="CT Chest Abdomen Pelvis with contrast for staging malignancy follow up treatment response assessment",
    )
    assert len(terms) <= 100


def test_audio_processor_wav_passthrough():
    from crowdtrans.transcriber.audio import process_karisma_blob

    wav_data = b"RIFF" + b"\x00" * 100
    result = process_karisma_blob(wav_data, None, None, 1)
    assert result is not None
    assert result.content_type == "audio/wav"
    assert result.data == wav_data


def test_audio_processor_with_offset():
    from crowdtrans.transcriber.audio import process_karisma_blob

    # 10 bytes of junk + WAV header
    blob = b"\x00" * 10 + b"RIFF" + b"\x00" * 100
    result = process_karisma_blob(blob, offset=10, length=104, dictation_key=1)
    assert result is not None
    assert result.content_type == "audio/wav"
    assert result.data[:4] == b"RIFF"


def test_audio_processor_gzip():
    import gzip
    from crowdtrans.transcriber.audio import process_karisma_blob

    original = b"RIFF" + b"\x00" * 100
    compressed = gzip.compress(original)
    result = process_karisma_blob(compressed, None, None, 1)
    assert result is not None
    assert result.data == original
    assert result.content_type == "audio/wav"


def test_audio_processor_raw_fallback():
    from crowdtrans.transcriber.audio import process_karisma_blob

    raw = b"\xDE\xAD\xBE\xEF" + b"\x00" * 50
    result = process_karisma_blob(raw, None, None, 1)
    assert result is not None
    assert result.content_type == "audio/raw"


def test_site_configs():
    from crowdtrans.config import settings

    sites = settings.get_site_configs()
    site_ids = [s.site_id for s in sites]
    assert "visage" in site_ids
    assert "karisma" in site_ids

    visage = settings.get_site("visage")
    assert visage.ris_type == "visage"
    assert visage.audio_source == "nfs"

    karisma = settings.get_site("karisma")
    assert karisma.ris_type == "karisma"
    assert karisma.audio_source == "sql_blob"


@pytest.mark.skipif(True, reason="Requires live Visage PostgreSQL connection")
def test_visage_connection():
    from crowdtrans.config import settings
    from crowdtrans.visage import check_connection

    site = settings.get_site("visage")
    result = check_connection(site)
    assert result["status"] == "ok"


@pytest.mark.skipif(True, reason="Requires live Karisma MSSQL connection")
def test_karisma_connection():
    from crowdtrans.config import settings
    from crowdtrans.karisma import check_connection

    site = settings.get_site("karisma")
    result = check_connection(site)
    assert result["status"] == "ok"
