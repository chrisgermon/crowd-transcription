#!/usr/bin/env python3
"""Poll the local SQLite DB every 30s and print newly-arrived transcriptions.

Reads /opt/crowdtrans/data/crowdtrans.db directly (no Karisma traffic).
Tracks the highest transcription id seen and reports anything newer.

Run: python3 scripts/watch_new_transcriptions.py [--interval 30]
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime

DB_PATH = "/opt/crowdtrans/data/crowdtrans.db"

COLS = [
    "id",
    "discovered_at",
    "status",
    "site_id",
    "accession_number",
    "modality_code",
    "doctor_family_name",
    "patient_family_name",
    "facility_name",
]


def fetch_new(conn: sqlite3.Connection, last_id: int) -> list[sqlite3.Row]:
    cur = conn.execute(
        f"SELECT {', '.join(COLS)} FROM transcriptions "
        "WHERE id > ? ORDER BY id ASC LIMIT 200",
        (last_id,),
    )
    return cur.fetchall()


def fetch_max_id(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM transcriptions").fetchone()
    return int(row[0])


def fmt_row(r: sqlite3.Row) -> str:
    return (
        f"#{r['id']:>6}  {r['discovered_at'] or '-':<19}  "
        f"{(r['status'] or '-'):<10}  "
        f"{(r['accession_number'] or '-'):<15}  "
        f"{(r['modality_code'] or '-'):<4}  "
        f"Dr {r['doctor_family_name'] or '-'}  /  "
        f"{r['patient_family_name'] or '-'}  "
        f"@ {r['facility_name'] or '-'}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=30, help="poll seconds (default 30)")
    ap.add_argument("--db", default=DB_PATH)
    ap.add_argument("--from-id", type=int, default=None,
                    help="start watching from this id (default: current max)")
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row

    last_id = args.from_id if args.from_id is not None else fetch_max_id(conn)
    print(f"[{datetime.now():%H:%M:%S}] watching {args.db} from id > {last_id} "
          f"(every {args.interval}s)", flush=True)

    try:
        while True:
            rows = fetch_new(conn, last_id)
            if rows:
                print(f"\n[{datetime.now():%H:%M:%S}] {len(rows)} new:", flush=True)
                for r in rows:
                    print("  " + fmt_row(r), flush=True)
                last_id = rows[-1]["id"]
            else:
                print(f"[{datetime.now():%H:%M:%S}] no new (last_id={last_id})",
                      flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped.", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
