#!/usr/bin/env python3
"""Get actual content from Karisma - report text, note text, service defs."""

import pymssql

CONN = {
    'server': '10.100.50.5',
    'port': 1433,
    'database': 'karisma_rvc_live',
    'user': 'Crowditreader',
    'password': 'Crowdbot1@',
}

def section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

def main():
    conn = pymssql.connect(**CONN)
    cur = conn.cursor()

    # 1. Joined Request + Report + Service (recent, using quoted Key)
    section("JOINED: Recent Request + Report Instance + Service")
    try:
        cur.execute("""
            SELECT TOP 5
                rq.[Key] AS RequestKey,
                rq.InternalIdentifier,
                rq.Context,
                rq.RequestedDate,
                rq.DefaultServicedDate,
                ri.[Key] AS ReportInstanceKey,
                ri.InternalIdentifier AS ReportIdentifier,
                ri.ClinicalAvailability,
                ri.ProcessStatus,
                ri.Abnormal,
                ri.Source,
                rs.AccessionNumber,
                rs.OrderedServiceDefinitionKey,
                rs.PerformedServiceDefinitionKey
            FROM [Version].[Karisma.Request.Record] rq
            JOIN [Version].[Karisma.Report.Instance] ri ON ri.RequestRecordKey = rq.[Key]
            JOIN [Version].[Karisma.Request.Service] rs ON rs.RequestRecordKey = rq.[Key]
            WHERE rq.Key_Deleted = 0 AND ri.Key_Deleted = 0 AND rs.Key_Deleted = 0
            ORDER BY rq.[Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 2. Report InstanceValue with actual text
    section("REPORT TEXT: InstanceValue with long text values")
    try:
        cur.execute("""
            SELECT TOP 5
                riv.[Key],
                riv.ReportInstanceChangeKey,
                riv.ReportDefinitionFieldKey,
                riv.[Current],
                riv.DataType,
                riv.Value,
                riv.BlobKey
            FROM [Version].[Karisma.Report.InstanceValue] riv
            WHERE riv.Key_Deleted = 0
              AND riv.Value IS NOT NULL
              AND LEN(riv.Value) > 50
            ORDER BY riv.[Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:500] if val is not None else 'NULL'}")
            print()
        if not rows:
            print("  (no rows with Value > 50 chars)")
    except Exception as e:
        print(f"  Error: {e}")

    # 3. Try to read report content from System.Extent (blob storage)
    section("REPORT CONTENT via System.Extent (blob storage)")
    try:
        cur.execute("""
            SELECT TOP 3
                riv.[Key] AS InstanceValueKey,
                riv.ReportDefinitionFieldKey,
                riv.BlobKey,
                e.Length AS BlobLength,
                e.Format AS BlobFormat,
                CAST(e.Buffer AS NVARCHAR(MAX)) AS BlobContent
            FROM [Version].[Karisma.Report.InstanceValue] riv
            JOIN [System].[Extent] e ON e.[Key] = riv.BlobKey
            WHERE riv.Key_Deleted = 0
              AND riv.[Current] = 1
              AND riv.BlobKey IS NOT NULL
            ORDER BY riv.[Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:1000] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 4. Request Note content via System.Extent
    section("REQUEST NOTE CONTENT via System.Extent")
    try:
        cur.execute("""
            SELECT TOP 5
                rn.[Key] AS NoteKey,
                rn.NoteStyle,
                rn.RequestRecordKey,
                rn.BufferKey,
                e.Length AS BlobLength,
                e.Format AS BlobFormat,
                CAST(e.Buffer AS NVARCHAR(MAX)) AS NoteText
            FROM [Version].[Karisma.Request.Note] rn
            JOIN [System].[Extent] e ON e.[Key] = rn.BufferKey
            WHERE rn.Key_Deleted = 0
              AND rn.IsDiscarded = 0
              AND rn.BufferKey IS NOT NULL
            ORDER BY rn.[Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:1000] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 5. Note Template content
    section("NOTE TEMPLATE CONTENT via System.Extent")
    try:
        cur.execute("""
            SELECT TOP 5
                nt.[Key],
                nt.Code,
                nt.Name,
                nt.Description,
                nt.Type,
                nt.BufferKey,
                e.Length AS BlobLength,
                e.Format AS BlobFormat,
                CAST(e.Buffer AS NVARCHAR(MAX)) AS TemplateText
            FROM [Version].[Karisma.Note.Template] nt
            JOIN [System].[Extent] e ON e.[Key] = nt.BufferKey
            WHERE nt.Key_Deleted = 0
              AND nt.IsDiscarded = 0
              AND nt.BufferKey IS NOT NULL
            ORDER BY nt.[Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:1000] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 6. Service Definition samples
    section("SERVICE DEFINITION SAMPLES (exam types)")
    try:
        cur.execute("""
            SELECT TOP 20
                sd.[Key],
                sd.Code,
                sd.Name,
                sd.Description,
                sd.ServiceDepartmentKey,
                sd.ServiceModalityKey,
                dep.Name AS DepartmentName,
                mod.Name AS ModalityName
            FROM [Version].[Karisma.Service.Definition] sd
            LEFT JOIN [Version].[Karisma.Service.Department] dep ON dep.[Key] = sd.ServiceDepartmentKey
            LEFT JOIN [Version].[Karisma.Service.Modality] mod ON mod.[Key] = sd.ServiceModalityKey
            WHERE sd.Key_Deleted = 0
            ORDER BY sd.[Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 7. Service Department
    section("SERVICE DEPARTMENTS")
    try:
        cur.execute("""
            SELECT [Key], Code, Name, Description
            FROM [Version].[Karisma.Service.Department]
            WHERE Key_Deleted = 0
            ORDER BY [Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  {row[0]:>4}  {row[1]:<15} {row[2]:<30} {row[3]}")
    except Exception as e:
        print(f"  Error: {e}")

    # 8. Service Modality
    section("SERVICE MODALITIES")
    try:
        cur.execute("""
            SELECT [Key], Code, Name, Description
            FROM [Version].[Karisma.Service.Modality]
            WHERE Key_Deleted = 0
            ORDER BY [Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  {row[0]:>4}  {row[1]:<15} {row[2]:<30} {row[3]}")
    except Exception as e:
        print(f"  Error: {e}")

    # 9. Report Template content (the actual template text)
    section("REPORT TEMPLATE CONTENT (first 5)")
    try:
        cur.execute("""
            SELECT TOP 5
                rt.[Key],
                rt.Code,
                rt.Name,
                rt.Description,
                rt.BufferKey,
                e.Length AS BlobLength,
                e.Format AS BlobFormat,
                CAST(e.Buffer AS NVARCHAR(MAX)) AS TemplateContent
            FROM [Version].[Karisma.Report.Template] rt
            JOIN [System].[Extent] e ON e.[Key] = rt.BufferKey
            WHERE rt.Key_Deleted = 0
              AND rt.IsDiscarded = 0
              AND rt.BufferKey IS NOT NULL
            ORDER BY rt.[Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:1000] if val is not None else "NULL"
                print(f"    {cn:<45} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 10. Practitioner Record with name info
    section("PRACTITIONER RECORDS (first 10)")
    try:
        cur.execute("""
            SELECT TOP 10
                pr.[Key],
                pr.PractitionerNumber,
                pr.Title,
                ci.[Key] AS ContactInstanceKey,
                pn.FirstName,
                pn.OtherNames,
                pn.Surname
            FROM [Version].[Karisma.Practitioner.Record] pr
            LEFT JOIN [Version].[Karisma.Contact.Instance] ci ON ci.[Key] = pr.ContactInstanceKey
            LEFT JOIN [Version].[Karisma.Patient.Name] pn ON pn.[Key] = pr.PreferredNameKey
            WHERE pr.Key_Deleted = 0
            ORDER BY pr.[Key]
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 11. Full Practitioner Record columns
    section("PRACTITIONER RECORD COLUMNS")
    try:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Version' AND TABLE_NAME = 'Karisma.Practitioner.Record'
            ORDER BY ORDINAL_POSITION
        """)
        for col_name, dtype, max_len in cur.fetchall():
            len_str = f"({max_len})" if max_len else ""
            print(f"    {col_name:<40} {dtype}{len_str}")
    except Exception as e:
        print(f"  Error: {e}")

    # 12. Dictation Instance structure and samples
    section("DICTATION INSTANCE (6.8M rows)")
    try:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Version' AND TABLE_NAME = 'Karisma.Dictation.Instance'
            ORDER BY ORDINAL_POSITION
        """)
        print("  Columns:")
        for col_name, dtype, max_len in cur.fetchall():
            len_str = f"({max_len})" if max_len else ""
            print(f"    {col_name:<40} {dtype}{len_str}")
    except Exception as e:
        print(f"  Error: {e}")

    print()
    try:
        cur.execute("""
            SELECT TOP 3 *
            FROM [Version].[Karisma.Dictation.Instance]
            WHERE Key_Deleted = 0
            ORDER BY [Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 13. Full enriched query - patient + request + service + report
    section("FULL ENRICHED: Patient + Request + Service Def + Report (recent 5)")
    try:
        cur.execute("""
            SELECT TOP 5
                pn.Surname,
                pn.FirstName,
                pr.BirthDate,
                pr.SexAssignedAtBirth,
                rq.InternalIdentifier AS RequestID,
                rq.RequestedDate,
                rq.DefaultServicedDate,
                rq.Context AS RequestContext,
                sd.Code AS ServiceCode,
                sd.Name AS ServiceName,
                dep.Name AS Department,
                md.Name AS Modality,
                ri.ClinicalAvailability,
                ri.ProcessStatus AS ReportStatus,
                ri.Abnormal
            FROM [Version].[Karisma.Request.Record] rq
            JOIN [Version].[Karisma.Patient.Record] pr ON pr.[Key] = rq.PatientKey
            JOIN [Version].[Karisma.Patient.Name] pn ON pn.[Key] = pr.PreferredNameKey
            JOIN [Version].[Karisma.Request.Service] rs ON rs.RequestRecordKey = rq.[Key]
            JOIN [Version].[Karisma.Service.Definition] sd ON sd.[Key] = rs.PerformedServiceDefinitionKey
            LEFT JOIN [Version].[Karisma.Service.Department] dep ON dep.[Key] = sd.ServiceDepartmentKey
            LEFT JOIN [Version].[Karisma.Service.Modality] md ON md.[Key] = sd.ServiceModalityKey
            LEFT JOIN [Version].[Karisma.Report.Instance] ri ON ri.RequestRecordKey = rq.[Key] AND ri.Key_Deleted = 0
            WHERE rq.Key_Deleted = 0
              AND pr.Key_Deleted = 0
              AND rs.Key_Deleted = 0
              AND sd.Key_Deleted = 0
              AND rq.DefaultServicedDate IS NOT NULL
            ORDER BY rq.DefaultServicedDate DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    # 14. Check NoteStyle values
    section("NOTE STYLE DISTRIBUTION in Request.Note")
    try:
        cur.execute("""
            SELECT NoteStyle, COUNT(*) AS cnt
            FROM [Version].[Karisma.Request.Note]
            WHERE Key_Deleted = 0 AND IsDiscarded = 0
            GROUP BY NoteStyle
            ORDER BY cnt DESC
        """)
        for style, cnt in cur.fetchall():
            print(f"    NoteStyle {style}: {cnt:>10} notes")
    except Exception as e:
        print(f"  Error: {e}")

    # 15. Request.Letter samples
    section("REQUEST LETTER SAMPLES")
    try:
        cur.execute("""
            SELECT TOP 3
                rl.[Key],
                rl.RequestKey,
                rl.DocumentKey,
                rl.ContentKey,
                e.Length AS ContentLength,
                e.Format AS ContentFormat
            FROM [Version].[Karisma.Request.Letter] rl
            LEFT JOIN [System].[Extent] e ON e.[Key] = rl.ContentKey
            WHERE rl.Key_Deleted = 0
            ORDER BY rl.[Key] DESC
        """)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                print(f"    {cn:<45} = {str(val)[:200] if val is not None else 'NULL'}")
            print()
    except Exception as e:
        print(f"  Error: {e}")

    conn.close()
    print("\n\nDone.")

if __name__ == '__main__':
    main()
