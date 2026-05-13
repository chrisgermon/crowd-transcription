"""Read-only MSSQL client for Karisma RIS dictation data."""

import logging
from typing import Any

import pymssql

from crowdtrans.config import SiteConfig

logger = logging.getLogger(__name__)

DICTATION_QUERY = """\
SELECT TOP {limit}
    DI.TransactionKey,
    DI.[Key] AS DictationInstanceKey,
    DI.CompletionStatus,
    DI.LastDictationCompleteDateTime AS CreatedTime,
    DI.ContentKey,
    DI.LengthSeconds,

    PR.[Key] AS PatientKey,
    PN.Title AS PatientTitle,
    PN.FirstName AS PatientFirstName,
    PN.Surname AS PatientLastName,
    PR.BirthDate AS PatientDateOfBirth,
    PTID.Value AS PatientId,

    RR.[Key] AS RequestKey,
    RR.InternalIdentifier,
    RR.ExternalIdentifier,
    RR.RequestedDate,

    RS.[Key] AS ServiceKey,
    SD.[Name] AS ServiceName,
    SD.Code AS ServiceCode,
    SM.Code AS ModalityCode,
    SM.[Name] AS ModalityName,
    SDEPT.[Name] AS DepartmentName,
    SDEPT.Code AS DepartmentCode,

    WS.[Key] AS WorkSiteKey,
    WS.[Name] AS WorkSiteName,
    WS.Code AS WorkSiteCode,

    PRAC.Title AS DictatingPractitionerTitle,
    PRAC.FirstName AS DictatingPractitionerFirstName,
    PRAC.Surname AS DictatingPractitionerSurname,
    PRAC.Code AS DictatingPractitionerCode,

    REFPRAC.[Key] AS ReferringPractitionerKey,
    REFPRAC.FirstName AS ReferringPractitionerFirstName,
    REFPRAC.Surname AS ReferringPractitionerSurname,

    COALESCE(RI.[Key], RI2.[Key]) AS ReportInstanceKey,
    COALESCE(RI.ClinicalAvailability, RI2.ClinicalAvailability) AS ClinicalAvailability,
    COALESCE(RI.ProcessStatus, RI2.ProcessStatus) AS ReportProcessStatus

FROM [Version].[Karisma.Dictation.Instance] DI

-- Path 1: Dictation -> ReportInstanceChange -> Report.Instance (traditional)
LEFT JOIN [Version].[Karisma.Dictation.Instance-ReportInstanceChange] DIRIC
    ON DIRIC.ParentKey = DI.[Key] AND DIRIC.DeletedTransactionKey IS NULL
LEFT JOIN [Version].[Karisma.Report.InstanceChange] RIC
    ON DIRIC.ChildKey = RIC.[Key]
LEFT JOIN [Version].[Karisma.Report.Instance] RI
    ON RIC.ReportInstanceKey = RI.[Key] AND RI.IsDiscarded = 0

-- Path 2: Dictation -> Document -> Report.Instance (catches 91% vs 58%)
LEFT JOIN [Version].[Karisma.Dictation.Instance-Document] DID
    ON DID.ParentKey = DI.[Key] AND DID.DeletedTransactionKey IS NULL
LEFT JOIN [Version].[Karisma.Report.Instance] RI2
    ON RI2.DocumentKey = DID.ChildKey AND RI2.IsDiscarded = 0
    AND RI.[Key] IS NULL  -- only use Path 2 if Path 1 didn't find anything

-- Request from whichever Report path succeeded
LEFT JOIN [Version].[Karisma.Request.Record] RR
    ON RR.[Key] = COALESCE(RI.RequestRecordKey, RI2.RequestRecordKey)

-- Service linked to this report instance
LEFT JOIN [Version].[Karisma.Request.Service] RS
    ON RS.RequestRecordKey = RR.[Key]
    AND RS.ReportInstanceKey = COALESCE(RI.[Key], RI2.[Key])
    AND RS.IsDiscarded = 0

LEFT JOIN [Version].[Karisma.Service.Definition] SD
    ON RS.PerformedServiceDefinitionKey = SD.[Key]
LEFT JOIN [Version].[Karisma.Service.Modality] SM
    ON SD.ServiceModalityKey = SM.[Key]
LEFT JOIN [Version].[Karisma.Service.Department] SDEPT
    ON SD.ServiceDepartmentKey = SDEPT.[Key]

-- Patient from Request
LEFT JOIN [Version].[Karisma.Patient.Record] PR
    ON RR.PatientKey = PR.[Key]
LEFT JOIN [Version].[Karisma.Patient.Name] PN
    ON PN.[Key] = PR.PreferredNameKey

OUTER APPLY (
    SELECT TOP 1 PI.Value
    FROM [Version].[Karisma.Patient.Identifier] PI
    WHERE PI.PatientRecordKey = PR.[Key]
      AND PI.Preferred = 1
      AND PI.IsDiscarded = 0
      AND PI.Key_Deleted = 0
    ORDER BY PI.TransactionKey DESC
) PTID

LEFT JOIN [Version].[Karisma.Work.Site] WS
    ON RR.WorkSiteKey = WS.[Key]

OUTER APPLY (
    SELECT TOP 1 P.Code, P.Title, P.FirstName, P.Surname
    FROM [Version].[Karisma.Practitioner.Record] P
    WHERE P.AssociatedUserKey = DI.DictatorKey
      AND P.Key_Deleted = 0
    ORDER BY P.TransactionKey DESC
) PRAC

LEFT JOIN [Version].[Karisma.Practitioner.Assignment] PA
    ON RR.RequestingPractitionerAssignmentKey = PA.[Key]
LEFT JOIN [Version].[Karisma.Practitioner.Record] REFPRAC
    ON PA.PractitionerRecordKey = REFPRAC.[Key]

WHERE DI.TransactionKey > %d
  AND DI.Key_Deleted = 0
  AND DI.Key_Owner = 0
  AND DI.ContentKey IS NOT NULL

ORDER BY DI.TransactionKey ASC
"""

FETCH_AUDIO_BLOB_QUERY = """\
SELECT Buffer
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
            cur.execute(DICTATION_QUERY.format(limit=int(limit)), (after_id,))
            rows = [dict(row) for row in cur]
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


PRE_DICTATION_REPORT_QUERY = """\
SELECT TOP 1 E.Buffer
FROM [Version].[Karisma.Report.InstanceChange] RIC
JOIN [Version].[Karisma.Report.InstanceValue] RIV
    ON RIV.ReportInstanceChangeKey = RIC.[Key]
    AND RIV.ReportDefinitionFieldKey = 1
JOIN [System].[Extent] E
    ON E.[Key] = RIV.BlobKey
WHERE RIC.ReportInstanceKey = %d
  AND RIC.TransactionKey < %d
  AND RIV.BlobKey IS NOT NULL
  AND E.Buffer IS NOT NULL
ORDER BY RIC.TransactionKey DESC
"""


def fetch_existing_report_content(
    site: SiteConfig,
    report_instance_key: int,
    dictation_transaction_key: int,
) -> str | None:
    """Fetch report content that existed before a dictation was recorded.

    For ultrasound studies, the sonographer or typist often pre-populates a
    report template with measurements and findings before the radiologist
    dictates. This function retrieves that pre-existing content so the
    formatter can merge dictation instructions with the template.

    Returns plain text or None if no pre-existing content was found.
    """
    if not report_instance_key or not dictation_transaction_key:
        return None
    conn = _get_connection(site)
    try:
        with conn.cursor(as_dict=False) as cur:
            cur.execute(
                PRE_DICTATION_REPORT_QUERY,
                (report_instance_key, dictation_transaction_key),
            )
            row = cur.fetchone()
            if row and row[0]:
                xml_bytes = bytes(row[0])
                text = _parse_report_xml(xml_bytes)
                # Only return if there's meaningful content (not just empty template shell)
                if text and len(text) > 30:
                    return text
        return None
    except Exception as e:
        logger.warning(
            "Could not fetch existing report for instance %d: %s",
            report_instance_key, e,
        )
        return None
    finally:
        conn.close()


def fetch_existing_report_batch(
    site: SiteConfig,
    items: list[tuple[int, int, int]],
) -> dict[int, str]:
    """Batch-fetch pre-dictation report content for multiple transcriptions.

    items: list of (transcription_id, report_instance_key, dictation_transaction_key)
    Returns {transcription_id: plain_text} for items that have pre-existing content.
    """
    if not items:
        return {}
    conn = _get_connection(site)
    results = {}
    try:
        with conn.cursor(as_dict=False) as cur:
            for txn_id, rik, dtk in items:
                if not rik or not dtk:
                    continue
                try:
                    cur.execute(PRE_DICTATION_REPORT_QUERY, (rik, dtk))
                    row = cur.fetchone()
                    if row and row[0]:
                        xml_bytes = bytes(row[0])
                        text = _parse_report_xml(xml_bytes)
                        if text and len(text) > 30:
                            results[txn_id] = text
                except Exception as e:
                    logger.debug(
                        "Skipped existing report for txn %d (rik=%d): %s",
                        txn_id, rik, e,
                    )
        return results
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

REPORT_TEMPLATES_SEARCH_QUERY = """\
SELECT TOP 15
    sub.TemplateName,
    sub.TemplateCode,
    sub.TemplateDescription,
    sub.TemplateBuffer
FROM (
    SELECT RT.[Name] AS TemplateName,
           RT.Code AS TemplateCode,
           RT.Description AS TemplateDescription,
           E.Buffer AS TemplateBuffer,
           ROW_NUMBER() OVER (PARTITION BY RT.[Name] ORDER BY RT.TransactionKey DESC) AS rn
    FROM [Version].[Karisma.Report.Template] RT
    JOIN [System].[Extent] E ON E.[Key] = RT.BufferKey
    WHERE RT.Key_Deleted = 0
      AND ({conditions})
) sub
WHERE sub.rn = 1
ORDER BY sub.TemplateName
"""


REPORT_TEMPLATES_ALL_QUERY = """\
SELECT sub.TemplateName, sub.TemplateCode, sub.TemplateDescription, sub.TemplateBuffer
FROM (
    SELECT RT.[Name] AS TemplateName,
           RT.Code AS TemplateCode,
           RT.Description AS TemplateDescription,
           E.Buffer AS TemplateBuffer,
           ROW_NUMBER() OVER (PARTITION BY RT.[Name] ORDER BY RT.TransactionKey DESC) AS rn
    FROM [Version].[Karisma.Report.Template] RT
    JOIN [System].[Extent] E ON E.[Key] = RT.BufferKey
    WHERE RT.Key_Deleted = 0
      AND E.Buffer IS NOT NULL
) sub
WHERE sub.rn = 1
ORDER BY sub.TemplateName
"""


def fetch_all_report_templates(site: SiteConfig) -> list[dict[str, str]]:
    """Fetch the entire Karisma report template library (latest version of each).

    Returns one entry per unique template name, with the most recent revision's
    contents converted to plain text.
    """
    conn = _get_connection(site)
    try:
        result = []
        with conn.cursor(as_dict=True) as cur:
            cur.execute(REPORT_TEMPLATES_ALL_QUERY)
            for row in cur:
                entry = {
                    "name": row["TemplateName"],
                    "code": row["TemplateCode"],
                    "description": row["TemplateDescription"],
                }
                raw = bytes(row["TemplateBuffer"]) if row["TemplateBuffer"] else None
                entry["text"] = _extract_plain_text_from_wp_xml(raw) if raw else ""
                result.append(entry)
        return result
    finally:
        conn.close()


def fetch_report_templates(
    site: SiteConfig,
    doctor_surname: str | None = None,
    procedure_desc: str | None = None,
    **_kwargs,
) -> list[dict[str, str]]:
    """Search report templates by doctor surname and/or procedure keywords."""
    conditions = []
    params: list[str] = []

    if doctor_surname:
        # Match templates whose name or description contains the doctor's surname
        conditions.append(
            "(RT.[Name] LIKE %s OR RT.Description LIKE %s)"
        )
        pat = f"%{doctor_surname}%"
        params.extend([pat, pat])

    if procedure_desc:
        # Extract meaningful keywords from procedure (e.g. "US SHOULDER RIGHT" -> shoulder)
        stop = {"us", "ct", "mr", "mri", "xr", "xray", "left", "right", "bilateral",
                "with", "without", "contrast", "and", "of", "the"}
        keywords = [
            w for w in procedure_desc.split()
            if len(w) > 2 and w.lower() not in stop
        ]
        for kw in keywords[:3]:
            conditions.append(
                "(RT.[Name] LIKE %s OR RT.Description LIKE %s)"
            )
            pat = f"%{kw}%"
            params.extend([pat, pat])

    if not conditions:
        return []

    query = REPORT_TEMPLATES_SEARCH_QUERY.format(
        conditions=" AND ".join(conditions)
    )

    conn = _get_connection(site)
    try:
        result = []
        with conn.cursor(as_dict=True) as cur:
            cur.execute(query, tuple(params))
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
SELECT DISTINCT TOP 5000 [Word]
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


# ---------------------------------------------------------------------------
# Undictated studies (registered/imaged but no report yet)
# ---------------------------------------------------------------------------

UNDICTATED_STUDIES_QUERY = """\
SELECT TOP {limit}
    RS.[Key] AS ServiceKey,
    RS.AccessionNumber,
    RS.RequestRecordKey,

    RR.InternalIdentifier,
    RR.AuditRegisteredDate AS RegisteredDate,
    RR.AuditScheduledDate AS ScheduledDate,
    RR.DefaultReportingPractitionerKey,

    PN.Title AS PatientTitle,
    PN.FirstName AS PatientFirstName,
    PN.Surname AS PatientLastName,
    PR.BirthDate AS PatientDOB,

    PTID.Value AS PatientId,

    SD.[Name] AS ServiceName,
    SD.Code AS ServiceCode,
    SM.Code AS ModalityCode,
    SM.[Name] AS ModalityName,

    WS.[Key] AS WorkSiteKey,
    WS.[Name] AS WorkSiteName,
    WS.Code AS WorkSiteCode,

    PRAC.Title AS PractitionerTitle,
    PRAC.FirstName AS PractitionerFirstName,
    PRAC.Surname AS PractitionerSurname,
    PRAC.Code AS PractitionerCode

FROM [Version].[Karisma.Request.Service] RS

JOIN [Version].[Karisma.Request.Record] RR
    ON RR.[Key] = RS.RequestRecordKey
    AND RR.Key_Deleted = 0
    AND RR.IsDiscarded = 0
    AND RR.Registered = 1

LEFT JOIN [Version].[Karisma.Service.Definition] SD
    ON RS.PerformedServiceDefinitionKey = SD.[Key]
LEFT JOIN [Version].[Karisma.Service.Modality] SM
    ON SD.ServiceModalityKey = SM.[Key]

LEFT JOIN [Version].[Karisma.Patient.Record] PR
    ON RR.PatientKey = PR.[Key]
LEFT JOIN [Version].[Karisma.Patient.Name] PN
    ON PN.[Key] = PR.PreferredNameKey

OUTER APPLY (
    SELECT TOP 1 PI.Value
    FROM [Version].[Karisma.Patient.Identifier] PI
    WHERE PI.PatientRecordKey = PR.[Key]
      AND PI.Preferred = 1
      AND PI.IsDiscarded = 0
      AND PI.Key_Deleted = 0
    ORDER BY PI.TransactionKey DESC
) PTID

LEFT JOIN [Version].[Karisma.Work.Site] WS
    ON RR.WorkSiteKey = WS.[Key]

LEFT JOIN [Version].[Karisma.Practitioner.Record] PRAC
    ON PRAC.[Key] = RR.DefaultReportingPractitionerKey
    AND PRAC.Key_Deleted = 0

WHERE RS.IsDiscarded = 0
  AND RS.Key_Deleted = 0
  AND RS.ReportInstanceKey = 0

  AND RR.AuditScheduledDate >= DATEADD(day, -7, GETDATE())

ORDER BY RR.AuditRegisteredDate DESC
"""


# ---------------------------------------------------------------------------
# Attachments: referral PDFs (Request) and worksheet images (Report)
# ---------------------------------------------------------------------------

REFERRAL_ATTACHMENTS_QUERY = """\
SELECT RA.Name, E.Buffer, E.[Format], E.[Length], E.[Handle]
FROM [Version].[Karisma.Request.Attachment] RA
JOIN [System].[Extent] E ON E.[Key] = RA.ContentKey
WHERE RA.RequestRecordKey = %d
  AND RA.Key_Deleted = 0
ORDER BY RA.TransactionKey DESC
"""

WORKSHEET_ATTACHMENTS_QUERY = """\
SELECT RIA.Name, E.Buffer, E.[Format], E.[Length]
FROM [Version].[Karisma.Report.InstanceAttachment] RIA
JOIN [Version].[Karisma.Report.InstanceChange] RIC ON RIC.[Key] = RIA.ReportInstanceChangeKey
JOIN [System].[Extent] E ON E.[Key] = RIA.BlobKey
WHERE RIC.ReportInstanceKey = %d
  AND RIA.Key_Deleted = 0
  AND RIA.[Current] = 1
  AND E.Buffer IS NOT NULL
ORDER BY RIA.TransactionKey DESC
"""


def _fetch_external_extent(handle: bytes) -> bytes | None:
    """Resolve a Kestral external extent by Handle (16-byte GUID).

    Karisma stores large PDFs/images outside the DB. The Handle is a binary
    GUID — typically used as a filename under a configured share. This
    function looks for the file under env-configured roots and returns its
    bytes if found.

    Configure via env: KARISMA_EXTENT_ROOTS = colon-separated absolute
    paths to search. The file is expected to be named after the GUID
    (with or without dashes, with or without the original format extension).
    Returns None when not found.
    """
    import os
    import uuid as _uuid
    from pathlib import Path

    roots_env = os.environ.get("KARISMA_EXTENT_ROOTS", "")
    if not roots_env:
        return None
    try:
        guid_le = _uuid.UUID(bytes_le=handle)
        guid_be = _uuid.UUID(bytes=handle)
    except Exception:
        return None

    candidates = {
        str(guid_le), str(guid_le).replace("-", ""), str(guid_le).upper(),
        str(guid_be), str(guid_be).replace("-", ""), str(guid_be).upper(),
    }
    for root in roots_env.split(":"):
        root = root.strip()
        if not root:
            continue
        rp = Path(root)
        if not rp.exists():
            continue
        for cand in candidates:
            for suffix in ("", ".pdf", ".jpeg", ".jpg", ".png", ".bin"):
                p = rp / f"{cand}{suffix}"
                if p.is_file():
                    try:
                        return p.read_bytes()
                    except OSError:
                        pass
    return None


def fetch_referral_attachments(site: SiteConfig, request_key: int) -> list[dict[str, Any]]:
    """Fetch referral PDF attachments for a request.

    Returns list of dicts with keys: name, data, format, length, external.
    When external=True, data is empty unless KARISMA_EXTENT_ROOTS is configured
    and the file is found there.
    """
    if not request_key:
        return []
    conn = _get_connection(site)
    try:
        with conn.cursor(as_dict=False) as cur:
            cur.execute(REFERRAL_ATTACHMENTS_QUERY, (request_key,))
            results = []
            for row in cur:
                name, buf, fmt, length, handle = row
                if buf:
                    data = bytes(buf)
                    external = False
                elif handle:
                    fetched = _fetch_external_extent(bytes(handle))
                    if fetched:
                        data = fetched
                        external = False
                    else:
                        data = b""
                        external = True
                else:
                    data = b""
                    external = False
                results.append({
                    "name": name or "referral",
                    "data": data,
                    "format": (fmt or "").lstrip(".") or "pdf",
                    "length": length or 0,
                    "external": external,
                })
            return results
    finally:
        conn.close()


def fetch_worksheet_attachments(site: SiteConfig, report_instance_key: int) -> list[dict[str, Any]]:
    """Fetch worksheet/SonoReview image attachments for a report instance."""
    if not report_instance_key:
        return []
    conn = _get_connection(site)
    try:
        with conn.cursor(as_dict=False) as cur:
            cur.execute(WORKSHEET_ATTACHMENTS_QUERY, (report_instance_key,))
            results = []
            for row in cur:
                name, buf, fmt, length = row
                results.append({
                    "name": name or "worksheet",
                    "data": bytes(buf) if buf else b"",
                    "format": (fmt or "").lstrip(".") or "png",
                    "length": length or 0,
                })
            return results
    finally:
        conn.close()


def fetch_undictated_studies(
    site: SiteConfig,
    limit: int = 500,
    excluded_worksites: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch registered studies that have no report yet (ready for dictation).

    Limited to studies scheduled in the last 7 days for performance.
    """
    conn = _get_connection(site)
    try:
        with conn.cursor() as cur:
            cur.execute(UNDICTATED_STUDIES_QUERY.format(limit=int(limit)))
            rows = []
            for row in cur:
                r = dict(row)
                if excluded_worksites and r.get("WorkSiteName") in excluded_worksites:
                    continue
                rows.append(r)
            return rows
    finally:
        conn.close()


def sync_pending_studies(site_id: str, site: SiteConfig, excluded_worksites: set[str] | None = None) -> int:
    """Sync undictated studies from Karisma into local PendingStudy cache.

    Replaces all cached studies for this site with fresh data.
    Returns the number of studies synced.
    """
    import datetime
    from crowdtrans.database import SessionLocal
    from crowdtrans.models import PendingStudy

    studies = fetch_undictated_studies(site, limit=1000, excluded_worksites=excluded_worksites)

    with SessionLocal() as session:
        # Delete existing entries for this site
        session.query(PendingStudy).filter(PendingStudy.site_id == site_id).delete()

        now = datetime.datetime.utcnow()
        for s in studies:
            ps = PendingStudy(
                site_id=site_id,
                service_key=s.get("ServiceKey"),
                request_key=s.get("RequestRecordKey"),
                accession_number=s.get("AccessionNumber") or s.get("InternalIdentifier"),
                internal_identifier=s.get("InternalIdentifier"),
                patient_title=s.get("PatientTitle"),
                patient_first_name=s.get("PatientFirstName"),
                patient_last_name=s.get("PatientLastName"),
                patient_id=s.get("PatientId"),
                patient_dob=str(s["PatientDOB"]) if s.get("PatientDOB") else None,
                service_name=s.get("ServiceName"),
                service_code=s.get("ServiceCode"),
                modality_code=s.get("ModalityCode"),
                modality_name=s.get("ModalityName"),
                doctor_code=s.get("PractitionerCode"),
                doctor_title=s.get("PractitionerTitle"),
                doctor_first_name=s.get("PractitionerFirstName"),
                doctor_surname=s.get("PractitionerSurname"),
                facility_name=s.get("WorkSiteName"),
                facility_code=s.get("WorkSiteCode"),
                registered_date=s.get("RegisteredDate"),
                scheduled_date=s.get("ScheduledDate"),
                synced_at=now,
            )
            session.add(ps)
        session.commit()

    logger.info("Synced %d pending studies for site %s", len(studies), site_id)
    return len(studies)


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
