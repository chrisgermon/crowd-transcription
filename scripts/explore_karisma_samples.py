#!/usr/bin/env python3
"""Sample data from key Karisma tables for transcription system improvement."""

import pymssql

CONN = {
    'server': '10.100.50.5',
    'port': 1433,
    'database': 'karisma_rvc_live',
    'user': 'Crowditreader',
    'password': 'Crowdbot1@',
}

def get_conn():
    return pymssql.connect(**CONN)

def sample(conn, table_name, top=3, where=None):
    """Print TOP N rows from a Version schema table."""
    cur = conn.cursor()
    q = f"SELECT TOP {top} * FROM [Version].[{table_name}]"
    if where:
        q += f" WHERE {where}"
    try:
        cur.execute(q)
        rows = cur.fetchall()
        if not rows:
            print(f"  (empty)")
            return
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:200] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

def section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

def main():
    conn = get_conn()

    # 1. Patient Record
    section("Karisma.Patient.Record (4.8M rows)")
    sample(conn, "Karisma.Patient.Record", 3)

    # 2. Patient Name
    section("Karisma.Patient.Name (1.7M rows)")
    sample(conn, "Karisma.Patient.Name", 3)

    # 3. Patient Allergy
    section("Karisma.Patient.Allergy (6 rows)")
    sample(conn, "Karisma.Patient.Allergy", 6)

    # 4. Patient Note
    section("Karisma.Patient.Note (51K rows)")
    sample(conn, "Karisma.Patient.Note", 3)

    # 5. Patient Observation (pregnancy/weight)
    section("Karisma.Patient.Observation (303 rows)")
    sample(conn, "Karisma.Patient.Observation", 3)

    # 6. Patient Condition Instance
    section("Karisma.Patient.ConditionInstance (165K rows)")
    sample(conn, "Karisma.Patient.ConditionInstance", 3)

    # 7. Patient Condition Definition
    section("Karisma.Patient.ConditionDefinition (18 rows)")
    sample(conn, "Karisma.Patient.ConditionDefinition", 18)

    # 8. Request Record
    section("Karisma.Request.Record (7.1M rows)")
    sample(conn, "Karisma.Request.Record", 3)

    # 9. Request Note
    section("Karisma.Request.Note (1.2M rows)")
    sample(conn, "Karisma.Request.Note", 3)

    # 10. Request Service
    section("Karisma.Request.Service (6.4M rows)")
    sample(conn, "Karisma.Request.Service", 3)

    # 11. Request Letter (referral letters?)
    section("Karisma.Request.Letter (153 rows)")
    sample(conn, "Karisma.Request.Letter", 5)

    # 12. Request Attachment
    section("Karisma.Request.Attachment (1186 rows)")
    sample(conn, "Karisma.Request.Attachment", 5)

    # 13. Request Referral Reason
    section("Karisma.Request.ReferralReason (1 row)")
    sample(conn, "Karisma.Request.ReferralReason", 5)

    # 14. Request Condition Definition (flags/conditions on requests)
    section("Karisma.Request.ConditionDefinition (273 rows)")
    sample(conn, "Karisma.Request.ConditionDefinition", 10)

    # 15. Report Instance
    section("Karisma.Report.Instance (11.7M rows)")
    sample(conn, "Karisma.Report.Instance", 3)

    # 16. Report Instance Value (THE REPORT TEXT!)
    section("Karisma.Report.InstanceValue (9.2M rows)")
    sample(conn, "Karisma.Report.InstanceValue", 5)

    # 17. Report Instance Change
    section("Karisma.Report.InstanceChange (12.2M rows)")
    sample(conn, "Karisma.Report.InstanceChange", 3)

    # 18. Report Definition
    section("Karisma.Report.Definition (2 rows)")
    sample(conn, "Karisma.Report.Definition", 2)

    # 19. Report Definition Field
    section("Karisma.Report.DefinitionField (2 rows)")
    sample(conn, "Karisma.Report.DefinitionField", 2)

    # 20. Report Template
    section("Karisma.Report.Template (19547 rows)")
    sample(conn, "Karisma.Report.Template", 5)

    # 21. Report Template Category
    section("Karisma.Report.TemplateCategory (28 rows)")
    sample(conn, "Karisma.Report.TemplateCategory", 28)

    # 22. Note Template
    section("Karisma.Note.Template (255 rows)")
    sample(conn, "Karisma.Note.Template", 5)

    # 23. Practitioner Record
    section("Karisma.Practitioner.Record (115K rows)")
    sample(conn, "Karisma.Practitioner.Record", 3)

    # 24. Service Definition
    section("Karisma.Service.Definition (5114 rows)")
    sample(conn, "Karisma.Service.Definition", 3)

    # 25. Service Department
    section("Karisma.Service.Department (17 rows)")
    sample(conn, "Karisma.Service.Department", 17)

    # 26. Dictation Instance
    section("Karisma.Dictation.Instance (6.8M rows)")
    sample(conn, "Karisma.Dictation.Instance", 3)

    # 27. Order Record
    section("Karisma.Order.Record (2033 rows)")
    sample(conn, "Karisma.Order.Record", 3)

    # 28. Document Record
    section("Karisma.Document.Record (13.9M rows)")
    sample(conn, "Karisma.Document.Record", 3)

    # 29. Contact Instance
    section("Karisma.Contact.Instance (2.2M rows)")
    sample(conn, "Karisma.Contact.Instance", 3)

    # 30. Contact Address
    section("Karisma.Contact.Address (1.3M rows)")
    sample(conn, "Karisma.Contact.Address", 3)

    # 31. Contact Phone
    section("Karisma.Contact.Phone (3.7M rows)")
    sample(conn, "Karisma.Contact.Phone", 3)

    # 32. Report Instance Actor
    section("Karisma.Report.InstanceActor (9.7M rows)")
    sample(conn, "Karisma.Report.InstanceActor", 3)

    # 33. Request Service ImageUrl
    section("Karisma.Request.Service-ImageUrl (540K rows)")
    sample(conn, "Karisma.Request.Service-ImageUrl", 3)

    # 34. Health Fund Record
    section("Karisma.HealthFund.Record (8 rows)")
    sample(conn, "Karisma.HealthFund.Record", 8)

    # 35. Report Problem Definition (for report flags)
    section("Karisma.Report.ProblemDefinition (123 rows)")
    sample(conn, "Karisma.Report.ProblemDefinition", 10)

    # 36. Document Dictionary
    section("Karisma.Document.Dictionary (8357 rows)")
    sample(conn, "Karisma.Document.Dictionary", 5)

    # 37. Patient Surgery
    section("Karisma.Patient.Surgery (0 rows)")
    sample(conn, "Karisma.Patient.Surgery", 3)

    # 38. Report Instance Attachment
    section("Karisma.Report.InstanceAttachment (4290 rows)")
    sample(conn, "Karisma.Report.InstanceAttachment", 3)

    # 39. Let's look at a full request+report join
    section("JOINED: Request + Report Instance + Service (recent)")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT TOP 5
                rq.Key AS RequestKey,
                rq.InternalIdentifier,
                rq.Context,
                rq.RequestedDate,
                rq.DefaultServicedDate,
                rq.ReceivedDate,
                ri.Key AS ReportInstanceKey,
                ri.InternalIdentifier AS ReportIdentifier,
                ri.ClinicalAvailability,
                ri.ProcessStatus,
                ri.Abnormal,
                ri.Source,
                rs.AccessionNumber,
                rs.OrderedServiceDefinitionKey,
                rs.PerformedServiceDefinitionKey
            FROM [Version].[Karisma.Request.Record] rq
            JOIN [Version].[Karisma.Report.Instance] ri ON ri.RequestRecordKey = rq.Key
            JOIN [Version].[Karisma.Request.Service] rs ON rs.RequestRecordKey = rq.Key
            WHERE rq.Key_Deleted = 0 AND ri.Key_Deleted = 0 AND rs.Key_Deleted = 0
            ORDER BY rq.Key DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:200] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 40. Report InstanceValue with actual text
    section("REPORT TEXT: InstanceValue with long text values")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT TOP 5
                riv.Key,
                riv.ReportInstanceChangeKey,
                riv.ReportDefinitionFieldKey,
                riv.Current,
                riv.DataType,
                riv.Value,
                riv.BlobKey
            FROM [Version].[Karisma.Report.InstanceValue] riv
            WHERE riv.Key_Deleted = 0
              AND riv.Value IS NOT NULL
              AND LEN(riv.Value) > 50
            ORDER BY riv.Key DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:500] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 41. Look at the Blob/Buffer storage - where actual report content might be
    section("BUFFER EXPLORATION: Looking for report text content storage")
    cur = conn.cursor()

    # Check for Blob/Buffer tables
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (TABLE_NAME LIKE '%Blob%'
               OR TABLE_NAME LIKE '%Buffer%'
               OR TABLE_NAME LIKE '%Content%'
               OR TABLE_NAME LIKE '%Body%')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    rows = cur.fetchall()
    print("  Blob/Buffer/Content/Body tables:")
    for schema, table in rows:
        cur2 = conn.cursor()
        try:
            cur2.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            count = cur2.fetchone()[0]
        except:
            count = "Error"
        print(f"    [{schema}].[{table}]  ({count} rows)")

    # 42. Look at Request.Note content (BufferKey reference)
    section("REQUEST NOTE: Trying to find note text content")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT TOP 5
                rn.Key,
                rn.NoteStyle,
                rn.RequestRecordKey,
                rn.IsDiscarded,
                rn.BufferKey
            FROM [Version].[Karisma.Request.Note] rn
            WHERE rn.Key_Deleted = 0
              AND rn.IsDiscarded = 0
              AND rn.BufferKey IS NOT NULL
            ORDER BY rn.Key DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:200] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 43. Look at System.Extent (where Blobs likely live)
    section("System.Extent structure (blob storage)")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'System' AND TABLE_NAME = 'Extent'
            ORDER BY ORDINAL_POSITION
        """)
        for col_name, dtype, max_len in cur.fetchall():
            len_str = f"({max_len})" if max_len else ""
            print(f"    {col_name:<40} {dtype}{len_str}")
    except Exception as e:
        print(f"  Error: {e}")

    # 44. Try to read actual note content via Extent
    section("ACTUAL NOTE CONTENT via System.Extent")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT TOP 3
                rn.Key AS NoteKey,
                rn.NoteStyle,
                rn.RequestRecordKey,
                CAST(e.Value AS NVARCHAR(4000)) AS NoteText
            FROM [Version].[Karisma.Request.Note] rn
            JOIN [System].[Extent] e ON e.Key = rn.BufferKey
            WHERE rn.Key_Deleted = 0
              AND rn.IsDiscarded = 0
              AND rn.BufferKey IS NOT NULL
            ORDER BY rn.Key DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:500] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 45. Service Definition lookup
    section("SERVICE DEFINITIONS (exam types)")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Version' AND TABLE_NAME = 'Karisma.Service.Definition'
            ORDER BY ORDINAL_POSITION
        """)
        print("  Columns:")
        for col_name, dtype, max_len in cur.fetchall():
            len_str = f"({max_len})" if max_len else ""
            print(f"    {col_name:<40} {dtype}{len_str}")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n\nDone.")
    conn.close()

if __name__ == '__main__':
    main()
