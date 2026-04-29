"""Read-only MSSQL client for Karisma RIS dictation data."""

import logging
from typing import Any

import pymssql

from crowdtrans.config import SiteConfig

logger = logging.getLogger(__name__)

DICTATION_QUERY = """\
SELECT
    DI.TransactionKey,
    DI.[Key] AS DictationInstanceKey,
    DI.Status,
    DI.CreatedTime,
    DI.ExtentKey,
    DI.ExtentOffset,
    DI.ExtentLength,

    PR.[Key] AS PatientKey,
    PR.Identifier AS PatientId,
    PR.Title AS PatientTitle,
    PR.FirstName AS PatientFirstName,
    PR.Surname AS PatientLastName,
    PR.DateOfBirth AS PatientDateOfBirth,

    RR.[Key] AS RequestKey,
    RR.Identifier AS AccessionNumber,
    RR.InternalIdentifier,
    RR.RequestedTime,
    RR.EarliestPerformedStartTime AS PerformedStartTime,
    RR.ClinicalNotes,

    RS.[Key] AS ServiceKey,
    SD.[Name] AS ServiceName,
    SD.Code AS ServiceCode,
    SM.[Name] AS ModalityName,
    SDEPT.[Name] AS DepartmentName,
    SDEPT.Code AS DepartmentCode,

    WS.[Key] AS WorkSiteKey,
    WS.[Name] AS WorkSiteName,
    WS.Code AS WorkSiteCode,

    PRAC.FullName AS DictatingPractitionerName,
    PRAC.Code AS DictatingPractitionerCode,

    REFPRAC.[Key] AS ReferringPractitionerKey,
    REFPRAC.FullName AS ReferringPractitionerName

FROM [Version].[Karisma.Dictation.Instance] DI

LEFT JOIN [Version].[Karisma.Dictation.Record] DR
    ON DI.RecordKey = DR.[Key]
LEFT JOIN [Version].[Karisma.Request.Service] RS
    ON DR.ServiceKey = RS.[Key]
LEFT JOIN [Version].[Karisma.Request.Record] RR
    ON RS.RequestKey = RR.[Key]
LEFT JOIN [Version].[Karisma.Patient.Record] PR
    ON RR.PatientKey = PR.[Key]
LEFT JOIN [Version].[Karisma.WorkSite.Record] WS
    ON RR.WorkSiteKey = WS.[Key]
LEFT JOIN [Version].[Karisma.Service.Definition] SD
    ON RS.PerformedServiceDefinitionKey = SD.[Key]
LEFT JOIN [Version].[Karisma.Service.Modality] SM
    ON SD.ServiceModalityKey = SM.[Key]
LEFT JOIN [Version].[Karisma.Service.Department] SDEPT
    ON SD.ServiceDepartmentKey = SDEPT.[Key]
LEFT JOIN [Version].[Karisma.Practitioner.Record] PRAC
    ON DI.ActorKey = PRAC.[Key]
LEFT JOIN [Version].[Karisma.Practitioner.Assignment] PA
    ON RR.RequestingPractitionerAssignmentKey = PA.[Key]
LEFT JOIN [Version].[Karisma.Practitioner.Record] REFPRAC
    ON PA.PractitionerRecordKey = REFPRAC.[Key]

WHERE DI.TransactionKey > %d
  AND DI.ExtentKey IS NOT NULL
  AND DI.ExtentLength > 0

ORDER BY DI.TransactionKey ASC
"""

FETCH_AUDIO_BLOB_QUERY = """\
SELECT Data
FROM [System].[Extent]
WHERE [Key] = %d
"""


def _get_connection(site: SiteConfig):
    return pymssql.connect(
        server=site.db_host,
        port=site.db_port,
        database=site.db_name,
        user=site.db_user,
        password=site.db_password,
        login_timeout=30,
        timeout=120,
        as_dict=True,
    )


def fetch_new_dictations(site: SiteConfig, after_id: int, limit: int) -> list[dict[str, Any]]:
    """Fetch dictation instances with TransactionKey > after_id."""
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            # pymssql doesn't support LIMIT, so we fetch and slice
            cur.execute(DICTATION_QUERY, (after_id,))
            rows = []
            for row in cur:
                rows.append(dict(row))
                if len(rows) >= limit:
                    break
            return rows
    finally:
        conn.close()


def fetch_audio_blob(site: SiteConfig, extent_key: int) -> bytes | None:
    """Fetch raw audio blob from System.Extent."""
    conn = _get_connection(site)
    try:
        with conn.cursor(as_dict=False) as cur:
            cur.execute(FETCH_AUDIO_BLOB_QUERY, (extent_key,))
            row = cur.fetchone()
            if row and row[0]:
                data = bytes(row[0])
                logger.debug("Fetched audio blob for ExtentKey %d: %d bytes", extent_key, len(data))
                return data
            logger.warning("No audio blob found for ExtentKey %d", extent_key)
            return None
    finally:
        conn.close()


REPORT_QUERY = """\
SELECT
    DI.TransactionKey AS DictationTK,
    E.Buffer AS ReportXML
FROM [Version].[Karisma.Dictation.Instance] DI
JOIN [Version].[Karisma.Dictation.Instance-ReportInstanceChange] DIRC
    ON DI.[Key] = DIRC.ParentKey
JOIN [Version].[Karisma.Report.InstanceChange] RIC
    ON DIRC.ChildKey = RIC.[Key]
JOIN [Version].[Karisma.Report.InstanceValue] RIV
    ON RIV.ReportInstanceChangeKey = RIC.[Key]
    AND RIV.[Current] = 1
JOIN [System].[Extent] E
    ON RIV.BlobKey = E.[Key]
WHERE RIV.BlobKey IS NOT NULL
  AND E.Buffer IS NOT NULL
  AND DI.TransactionKey IN ({placeholders})
"""


def _parse_report_xml(xml_bytes: bytes) -> str:
    """Extract plain text from Karisma WordProcessor XML report."""
    import re
    try:
        text = xml_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = xml_bytes.decode("utf-16")
        except UnicodeDecodeError:
            return ""

    # Extract text content from <Text ...>content</Text> and bare <Paragraph> text
    paragraphs = []
    current_para = []

    for match in re.finditer(r"<(Paragraph|Text|/Paragraph)[^>]*>([^<]*)", text):
        tag = match.group(1)
        content = match.group(2).strip()
        if tag == "Text" and content:
            current_para.append(content)
        elif tag == "/Paragraph":
            if current_para:
                paragraphs.append(" ".join(current_para))
            else:
                paragraphs.append("")  # empty paragraph = blank line
            current_para = []

    # Join paragraphs, collapse multiple blank lines
    result = "\n".join(paragraphs)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()


def fetch_reports(site: SiteConfig, transaction_keys: list[int]) -> dict[int, str]:
    """Fetch typed report text for a batch of dictation TransactionKeys.

    Returns {transaction_key: plain_text_report} for reports that exist.
    """
    if not transaction_keys:
        return {}
    conn = _get_connection(site)
    try:
        reports = {}
        batch_size = 200
        for i in range(0, len(transaction_keys), batch_size):
            batch = transaction_keys[i:i + batch_size]
            placeholders = ",".join(["%d"] * len(batch))
            query = REPORT_QUERY.format(placeholders=placeholders)
            with conn.cursor() as cur:
                cur.execute(query, tuple(batch))
                for row in cur:
                    xml_bytes = bytes(row["ReportXML"])
                    text = _parse_report_xml(xml_bytes)
                    if text and len(text) > 20:
                        reports[row["DictationTK"]] = text
        return reports
    finally:
        conn.close()


def check_connection(site: SiteConfig) -> dict[str, Any]:
    """Test MSSQL connectivity and return basic info."""
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            counts = {}
            for table in [
                "[Version].[Karisma.Dictation.Instance]",
                "[Version].[Karisma.Patient.Record]",
                "[Version].[Karisma.Practitioner.Record]",
            ]:
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
                row = cur.fetchone()
                counts[table] = row["cnt"]

            # Get max TransactionKey
            cur.execute(
                "SELECT ISNULL(MAX(TransactionKey), 0) AS max_tk "
                "FROM [Version].[Karisma.Dictation.Instance]"
            )
            row = cur.fetchone()
            counts["max_transaction_key"] = row["max_tk"]

            return {"status": "ok", "counts": counts}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Patient conditions (diabetic, pregnant, blood thinners, claustrophobic, etc.)
# ---------------------------------------------------------------------------

PATIENT_CONDITIONS_QUERY = """\
SELECT CD.[Name] AS ConditionName
FROM [Version].[Karisma.Patient.ConditionInstance] CI
JOIN [Version].[Karisma.Patient.ConditionDefinition] CD
    ON CI.ConditionDefinitionKey = CD.[Key]
WHERE CI.PatientRecordKey = %d
  AND CI.IsDiscarded = 0
  AND CI.Key_Deleted = 0
"""


def fetch_patient_conditions(site: SiteConfig, patient_key: int) -> list[str]:
    """Fetch active conditions for a patient (e.g. Diabetic, Blood Thinners, Pregnant)."""
    if not patient_key:
        return []
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            cur.execute(PATIENT_CONDITIONS_QUERY, (patient_key,))
            return [row["ConditionName"] for row in cur]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# All clinical note types from Request.Note
# ---------------------------------------------------------------------------

ALL_NOTES_QUERY = """\
SELECT RN.NoteStyle, E.Buffer
FROM [Version].[Karisma.Request.Note] RN
JOIN [System].[Extent] E ON E.[Key] = RN.BufferKey
WHERE RN.RequestRecordKey = %d
  AND RN.IsDiscarded = 0
  AND RN.Key_Deleted = 0
ORDER BY RN.NoteStyle ASC, RN.TransactionKey DESC
"""


def _extract_plain_text_from_wp_xml(raw: bytes) -> str:
    """Extract plain text from Kestral WordProcessor XML format."""
    import re as _re
    try:
        try:
            xml_text = raw.decode("utf-8")
        except UnicodeDecodeError:
            xml_text = raw.decode("utf-16-le")

        texts = _re.findall(r"<Text[^>]*>([^<]+)</Text>", xml_text)
        if texts:
            parts = _re.split(r"</Paragraph>\s*<Paragraph[^>]*>", xml_text)
            if len(parts) > 1:
                paragraphs = []
                for part in parts:
                    part_texts = _re.findall(r"<Text[^>]*>([^<]+)</Text>", part)
                    if part_texts:
                        paragraphs.append(" ".join(part_texts))
                return "\n".join(p for p in paragraphs if p.strip())
            return " ".join(texts)

        plain = _re.sub(r"<[^>]+>", " ", xml_text)
        plain = _re.sub(r"\s+", " ", plain).strip()
        return plain
    except Exception as e:
        logger.warning("Failed to extract text from WP XML: %s", e)
        return ""


def fetch_all_request_notes(site: SiteConfig, request_key: int) -> dict[str, str]:
    """Fetch all note types for a request.

    Returns dict with keys: 'clinical', 'order', 'worksheet', 'private'.
    NoteStyle: 0=order, 1=clinical, 2=worksheet/focus, 3=private.
    """
    if not request_key:
        return {}
    conn = _get_connection(site)
    style_map = {0: "order", 1: "clinical", 2: "worksheet", 3: "private"}
    try:
        result = {}
        with conn.cursor(as_dict=False) as cur:
            cur.execute(ALL_NOTES_QUERY, (request_key,))
            for row in cur:
                style = style_map.get(row[0], "other")
                if style in result:
                    continue  # Keep first (most recent) per style
                raw = bytes(row[1]) if row[1] else None
                if raw:
                    text = _extract_plain_text_from_wp_xml(raw)
                    if text:
                        result[style] = text
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Prior reports for the same patient
# ---------------------------------------------------------------------------

PRIOR_REPORTS_QUERY = """\
SELECT TOP {limit}
    RR.InternalIdentifier AS AccessionNumber,
    SD.[Name] AS ServiceName,
    SD.Code AS ServiceCode,
    SM.[Name] AS ModalityName,
    RI.ProcessStatus,
    RR.RequestedDate,
    RI.[Key] AS ReportInstanceKey
FROM [Version].[Karisma.Report.Instance] RI
JOIN [Version].[Karisma.Request.Record] RR
    ON RI.RequestRecordKey = RR.[Key]
LEFT JOIN [Version].[Karisma.Request.Service] RS
    ON RS.RequestRecordKey = RR.[Key]
    AND RS.ReportInstanceKey = RI.[Key]
    AND RS.IsDiscarded = 0
LEFT JOIN [Version].[Karisma.Service.Definition] SD
    ON RS.PerformedServiceDefinitionKey = SD.[Key]
LEFT JOIN [Version].[Karisma.Service.Modality] SM
    ON SD.ServiceModalityKey = SM.[Key]
WHERE RR.PatientKey = %d
  AND RI.IsDiscarded = 0
  AND RI.[Key] != %d
  AND RI.ProcessStatus > 0
ORDER BY RR.RequestedDate DESC
"""


def fetch_prior_reports(
    site: SiteConfig, patient_key: int, exclude_report_key: int = 0, limit: int = 5
) -> list[dict[str, Any]]:
    """Fetch recent prior reports for the same patient."""
    if not patient_key:
        return []
    conn = _get_connection(site)
    try:
        result = []
        with conn.cursor() as cur:
            query = PRIOR_REPORTS_QUERY.format(limit=limit)
            cur.execute(query, (patient_key, exclude_report_key))
            for row in cur:
                result.append(dict(row))
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Report templates (per-doctor, per-exam type)
# ---------------------------------------------------------------------------

REPORT_TEMPLATES_QUERY = """\
SELECT TOP 5
    RT.[Name] AS TemplateName,
    RT.Code AS TemplateCode,
    RT.Description AS TemplateDescription,
    E.Buffer AS TemplateBuffer
FROM [Version].[Karisma.Report.Template] RT
JOIN [System].[Extent] E ON E.[Key] = RT.BufferKey
JOIN [Version].[Karisma.Report.Template-ProfileUser] TPU
    ON TPU.ChildKey = RT.[Key]
WHERE TPU.ParentKey = %d
  AND RT.Key_Deleted = 0
  AND RT.[Name] LIKE %s
ORDER BY RT.TransactionKey DESC
"""

REPORT_TEMPLATES_BY_USER_QUERY = """\
SELECT TOP 10
    RT.[Name] AS TemplateName,
    RT.Code AS TemplateCode,
    RT.Description AS TemplateDescription,
    E.Buffer AS TemplateBuffer
FROM [Version].[Karisma.Report.Template] RT
JOIN [System].[Extent] E ON E.[Key] = RT.BufferKey
JOIN [Version].[Karisma.Report.Template-ProfileUser] TPU
    ON TPU.ChildKey = RT.[Key]
WHERE TPU.ParentKey = %d
  AND RT.Key_Deleted = 0
ORDER BY RT.TransactionKey DESC
"""


def fetch_report_templates(
    site: SiteConfig, dictator_user_key: int, service_name: str | None = None
) -> list[dict[str, str]]:
    """Fetch report templates for a doctor, optionally filtered by service name."""
    if not dictator_user_key:
        return []
    conn = _get_connection(site)
    try:
        result = []
        with conn.cursor(as_dict=True) as cur:
            if service_name:
                pattern = f"%{service_name.split()[0] if service_name else ''}%"
                cur.execute(REPORT_TEMPLATES_QUERY, (dictator_user_key, pattern))
            else:
                cur.execute(REPORT_TEMPLATES_BY_USER_QUERY, (dictator_user_key,))
            for row in cur:
                entry = {
                    "name": row["TemplateName"],
                    "code": row["TemplateCode"],
                    "description": row["TemplateDescription"],
                }
                raw = bytes(row["TemplateBuffer"]) if row["TemplateBuffer"] else None
                if raw:
                    entry["text"] = _extract_plain_text_from_wp_xml(raw)
                else:
                    entry["text"] = ""
                result.append(entry)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Medical dictionary from Karisma
# ---------------------------------------------------------------------------

DICTIONARY_QUERY = """\
SELECT DISTINCT [Word]
FROM [Version].[Karisma.Document.Dictionary]
WHERE Key_Deleted = 0
  AND LEN([Word]) > 3
ORDER BY [Word]
"""


def fetch_medical_dictionary(site: SiteConfig) -> list[str]:
    """Fetch custom medical dictionary words from Karisma (for Deepgram keyterms)."""
    conn = _get_connection(site)
    try:
        with conn.cursor(as_dict=False) as cur:
            cur.execute(DICTIONARY_QUERY)
            return [row[0] for row in cur if row[0]]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Practitioner qualifications (for footer rendering)
# ---------------------------------------------------------------------------

PRACTITIONER_DETAILS_QUERY = """\
SELECT TOP 1
    P.Code, P.Title, P.FirstName, P.Surname, P.Qualifications,
    P.IsReportingProvider, P.AssociatedUserKey
FROM [Version].[Karisma.Practitioner.Record] P
WHERE P.Code = %s
  AND P.Key_Deleted = 0
ORDER BY P.TransactionKey DESC
"""


def fetch_practitioner_details(site: SiteConfig, practitioner_code: str) -> dict[str, Any] | None:
    """Fetch practitioner details including qualifications."""
    if not practitioner_code:
        return None
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            cur.execute(PRACTITIONER_DETAILS_QUERY, (practitioner_code,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_min_transaction_key_for_date(site: SiteConfig, date_str: str) -> int:
    """Get the minimum TransactionKey for dictations on or after the given date (YYYY-MM-DD)."""
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ISNULL(MIN(TransactionKey), 0) AS min_tk
                FROM [Version].[Karisma.Dictation.Instance]
                WHERE Key_Deleted = 0
                  AND Key_Owner = 0
                  AND ContentKey IS NOT NULL
                  AND CAST(LastDictationCompleteDateTime AS DATE) >= %s
            """, (date_str,))
            row = cur.fetchone()
            return max(0, row["min_tk"] - 1)
    finally:
        conn.close()
