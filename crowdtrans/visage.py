"""Read-only PostgreSQL client for Visage RIS dictation data."""

import logging
from typing import Any

import psycopg2
import psycopg2.extras

from crowdtrans.config import SiteConfig

logger = logging.getLogger(__name__)

DICTATION_QUERY = """\
SELECT d.id AS dictation_id, d.basename, d.relative_path, d.mime_type, d.duration,
       d.dictation_date, d.status AS dictation_status,
       doc.id AS doctor_id, doc.title AS doctor_title, doc.given_names AS doctor_given_names,
       doc.family_name AS doctor_family_name,
       pt.id AS patient_id, pt.record_number AS patient_ur, pt.title AS patient_title,
       pt.given_names AS patient_given_names, pt.family_name AS patient_family_name,
       pt.birth_date AS patient_dob,
       o.id AS order_id, o.accession_number, o.complaint,
       p.id AS procedure_id, p.description AS procedure_description, p.reason_for_study,
       m.code AS modality_code, m.name AS modality_name,
       bp.name AS body_part,
       f.id AS facility_id, f.name AS facility_name,
       ref.id AS referrer_id, ref.title AS referrer_title,
       ref.given_names AS referrer_given_names, ref.family_name AS referrer_family_name
FROM dictation d
JOIN dictation_procedure dp ON dp.dictation_id = d.id
JOIN procedure_ p ON p.id = dp.procedure_id
LEFT JOIN procedure_type pt2 ON pt2.id = p.procedure_type_id
LEFT JOIN modality m ON m.id = pt2.modality_id
LEFT JOIN body_part bp ON bp.id = pt2.body_part_id
LEFT JOIN order_ o ON o.id = p.order_id
LEFT JOIN patient pt ON pt.id = o.patient_id
LEFT JOIN referrer ref ON ref.id = o.referrer_id
LEFT JOIN facility f ON f.id = o.facility_id
LEFT JOIN doctor doc ON doc.id = d.doctor_id
WHERE d.id > %s AND d.basename IS NOT NULL AND d.duration > 0
ORDER BY d.id ASC
LIMIT %s;
"""


def _get_connection(site: SiteConfig):
    return psycopg2.connect(
        host=site.db_host,
        port=site.db_port,
        dbname=site.db_name,
        user=site.db_user,
        password=site.db_password,
        options="-c default_transaction_read_only=on",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def fetch_new_dictations(site: SiteConfig, after_id: int, limit: int) -> list[dict[str, Any]]:
    """Fetch dictations with id > after_id, returning up to `limit` rows."""
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            cur.execute(DICTATION_QUERY, (after_id, limit))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


def check_connection(site: SiteConfig) -> dict[str, Any]:
    """Test connectivity and return basic table counts."""
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            counts = {}
            for table in ["dictation", "dictation_procedure", "procedure_", "patient", "doctor"]:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")  # noqa: S608
                counts[table] = cur.fetchone()["cnt"]
            return {"status": "ok", "counts": counts}
    finally:
        conn.close()
