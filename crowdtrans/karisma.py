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
