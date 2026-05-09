#!/usr/bin/env python3
"""Deep exploration of Karisma MSSQL database for radiology transcription improvements."""

import pymssql
import sys

CONN = {
    'server': '10.100.50.5',
    'port': 1433,
    'database': 'karisma_rvc_live',
    'user': 'Crowditreader',
    'password': 'Crowdbot1@',
}

KEYWORDS = [
    'Patient', 'Request', 'Report', 'Practitioner', 'Service', 'Document',
    'Note', 'Template', 'Body', 'Study', 'Exam', 'Clinical', 'Allergy',
    'Medication', 'Prior', 'History', 'Schedule', 'Appointment', 'Order',
    'Referral', 'Insurance', 'Fund', 'Address', 'Phone', 'Contact',
]

def get_conn():
    return pymssql.connect(**CONN)

def section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

def subsection(title):
    print(f"\n--- {title} ---")

def print_columns(conn, schema, table):
    """Print all columns for a given schema.table"""
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    rows = cur.fetchall()
    if not rows:
        print(f"  (no columns found)")
        return rows
    for col_name, dtype, max_len, nullable in rows:
        len_str = f"({max_len})" if max_len else ""
        null_str = "NULL" if nullable == 'YES' else "NOT NULL"
        print(f"    {col_name:<40} {dtype}{len_str:<20} {null_str}")
    return rows

def sample_data(conn, schema, table, top=3):
    """Print TOP N rows from a table."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT TOP {top} * FROM [{schema}].[{table}]")
        rows = cur.fetchall()
        if not rows:
            print(f"  (empty table)")
            return
        # Get column names
        col_names = [desc[0] for desc in cur.description]
        for i, row in enumerate(rows):
            print(f"  Row {i+1}:")
            for cn, val in zip(col_names, row):
                val_str = str(val)[:120] if val is not None else "NULL"
                print(f"    {cn:<40} = {val_str}")
            print()
    except Exception as e:
        print(f"  Error sampling: {e}")

def count_rows(conn, schema, table):
    """Count rows in a table."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        count = cur.fetchone()[0]
        return count
    except Exception as e:
        return f"Error: {e}"

def main():
    conn = get_conn()
    cur = conn.cursor()

    # =========================================================================
    # 1. Find all tables matching keywords in [Version] schema
    # =========================================================================
    section("1. TABLES MATCHING KEYWORDS (all schemas)")

    # First, let's see what schemas exist
    subsection("Available schemas")
    cur.execute("""
        SELECT DISTINCT TABLE_SCHEMA
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA
    """)
    schemas = [r[0] for r in cur.fetchall()]
    for s in schemas:
        print(f"  {s}")

    # Build keyword filter
    keyword_conditions = " OR ".join([f"TABLE_NAME LIKE '%{kw}%'" for kw in KEYWORDS])

    subsection("Tables matching keywords (all schemas)")
    cur.execute(f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND ({keyword_conditions})
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    matched_tables = cur.fetchall()
    print(f"  Found {len(matched_tables)} matching tables\n")

    for schema, table in matched_tables:
        fqn = f"[{schema}].[{table}]"
        count = count_rows(conn, schema, table)
        print(f"\n  TABLE: {fqn}  ({count} rows)")
        print_columns(conn, schema, table)

    # =========================================================================
    # 2. Patient-related tables
    # =========================================================================
    section("2. PATIENT-RELATED TABLES")

    # Find all tables in Patient schema
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (TABLE_SCHEMA = 'Patient' OR TABLE_NAME LIKE '%Patient%')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    patient_tables = cur.fetchall()
    print(f"  Found {len(patient_tables)} patient-related tables\n")

    for schema, table in patient_tables:
        fqn = f"[{schema}].[{table}]"
        count = count_rows(conn, schema, table)
        print(f"\n  TABLE: {fqn}  ({count} rows)")
        print_columns(conn, schema, table)

    # =========================================================================
    # 3. Request-related tables
    # =========================================================================
    section("3. REQUEST-RELATED TABLES")

    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (TABLE_SCHEMA = 'Request' OR TABLE_NAME LIKE '%Request%')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    request_tables = cur.fetchall()
    print(f"  Found {len(request_tables)} request-related tables\n")

    for schema, table in request_tables:
        fqn = f"[{schema}].[{table}]"
        count = count_rows(conn, schema, table)
        print(f"\n  TABLE: {fqn}  ({count} rows)")
        print_columns(conn, schema, table)

    # =========================================================================
    # 4. Report template/definition tables
    # =========================================================================
    section("4. REPORT & TEMPLATE TABLES")

    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (TABLE_SCHEMA = 'Report'
               OR TABLE_NAME LIKE '%Report%'
               OR TABLE_NAME LIKE '%Template%'
               OR TABLE_SCHEMA = 'Template')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    report_tables = cur.fetchall()
    print(f"  Found {len(report_tables)} report/template tables\n")

    for schema, table in report_tables:
        fqn = f"[{schema}].[{table}]"
        count = count_rows(conn, schema, table)
        print(f"\n  TABLE: {fqn}  ({count} rows)")
        print_columns(conn, schema, table)

    # =========================================================================
    # 5. Referral/prior study/clinical tables
    # =========================================================================
    section("5. REFERRAL / PRIOR STUDY / CLINICAL TABLES")

    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND (TABLE_NAME LIKE '%Referral%'
               OR TABLE_NAME LIKE '%Prior%'
               OR TABLE_NAME LIKE '%Clinical%'
               OR TABLE_NAME LIKE '%Study%'
               OR TABLE_NAME LIKE '%Exam%'
               OR TABLE_SCHEMA = 'Referral'
               OR TABLE_SCHEMA = 'Clinical'
               OR TABLE_SCHEMA = 'Study'
               OR TABLE_SCHEMA = 'Exam')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    ref_tables = cur.fetchall()
    print(f"  Found {len(ref_tables)} referral/clinical tables\n")

    for schema, table in ref_tables:
        fqn = f"[{schema}].[{table}]"
        count = count_rows(conn, schema, table)
        print(f"\n  TABLE: {fqn}  ({count} rows)")
        print_columns(conn, schema, table)

    # =========================================================================
    # 6. Sample data from key tables
    # =========================================================================
    section("6. SAMPLE DATA FROM KEY TABLES")

    # Tables to sample - we'll build this list from what we found
    sample_targets = []

    # Key tables we expect
    key_tables = [
        ('Patient', 'Record'),
        ('Patient', 'Address'),
        ('Patient', 'Phone'),
        ('Patient', 'Contact'),
        ('Patient', 'Allergy'),
        ('Patient', 'Alert'),
        ('Patient', 'History'),
        ('Request', 'Record'),
        ('Request', 'Note'),
        ('Request', 'Service'),
        ('Request', 'ClinicalQuestion'),
        ('Request', 'History'),
        ('Report', 'Record'),
        ('Report', 'Definition'),
        ('Report', 'Template'),
        ('Report', 'Section'),
        ('Report', 'Body'),
        ('Practitioner', 'Record'),
        ('Document', 'Record'),
        ('Service', 'Record'),
    ]

    for schema, table in key_tables:
        # Check if table exists
        cur.execute("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND TABLE_TYPE = 'BASE TABLE'
        """, (schema, table))
        if cur.fetchone()[0] > 0:
            sample_targets.append((schema, table))

    for schema, table in sample_targets:
        fqn = f"[{schema}].[{table}]"
        subsection(f"Sample data: {fqn}")
        sample_data(conn, schema, table, top=3)

    # =========================================================================
    # 7. Row counts for key tables
    # =========================================================================
    section("7. ROW COUNTS FOR ALL MATCHED TABLES")

    # Combine all found tables
    all_tables = set()
    for schema, table in matched_tables:
        all_tables.add((schema, table))
    for schema, table in patient_tables:
        all_tables.add((schema, table))
    for schema, table in request_tables:
        all_tables.add((schema, table))
    for schema, table in report_tables:
        all_tables.add((schema, table))
    for schema, table in ref_tables:
        all_tables.add((schema, table))

    counts = []
    for schema, table in sorted(all_tables):
        c = count_rows(conn, schema, table)
        counts.append((schema, table, c))

    # Sort by count descending
    counts.sort(key=lambda x: x[2] if isinstance(x[2], int) else 0, reverse=True)

    print(f"  {'Schema':<20} {'Table':<40} {'Rows':>12}")
    print(f"  {'-'*20} {'-'*40} {'-'*12}")
    for schema, table, c in counts:
        print(f"  {schema:<20} {table:<40} {c:>12}")

    # =========================================================================
    # 8. Bonus: Find foreign key relationships to Patient.Record and Request.Record
    # =========================================================================
    section("8. FOREIGN KEY RELATIONSHIPS")

    subsection("FKs referencing Patient.Record")
    cur.execute("""
        SELECT
            fk.name AS FK_Name,
            tp.name AS Parent_Table,
            sp.name AS Parent_Schema,
            cp.name AS Parent_Column,
            tr.name AS Referenced_Table,
            sr.name AS Referenced_Schema,
            cr.name AS Referenced_Column
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        INNER JOIN sys.tables tp ON fkc.parent_object_id = tp.object_id
        INNER JOIN sys.schemas sp ON tp.schema_id = sp.schema_id
        INNER JOIN sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
        INNER JOIN sys.tables tr ON fkc.referenced_object_id = tr.object_id
        INNER JOIN sys.schemas sr ON tr.schema_id = sr.schema_id
        INNER JOIN sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        WHERE (sr.name = 'Patient' AND tr.name = 'Record')
           OR (sr.name = 'Request' AND tr.name = 'Record')
           OR (sr.name = 'Report' AND tr.name = 'Record')
        ORDER BY sr.name, tr.name, sp.name, tp.name
    """)
    fk_rows = cur.fetchall()
    for fk_name, ptable, pschema, pcol, rtable, rschema, rcol in fk_rows:
        print(f"  [{pschema}].[{ptable}].{pcol}  -->  [{rschema}].[{rtable}].{rcol}   (FK: {fk_name})")

    # =========================================================================
    # 9. Look at all schemas and all tables for complete picture
    # =========================================================================
    section("9. COMPLETE TABLE LIST BY SCHEMA (for reference)")

    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    all_db_tables = cur.fetchall()

    current_schema = None
    for schema, table in all_db_tables:
        if schema != current_schema:
            print(f"\n  Schema: [{schema}]")
            current_schema = schema
        c = count_rows(conn, schema, table)
        print(f"    {table:<50} ({c} rows)")

    conn.close()
    print("\n\nDone.")

if __name__ == '__main__':
    main()
