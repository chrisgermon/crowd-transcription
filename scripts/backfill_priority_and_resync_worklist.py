"""One-off backfill: clear stale 'ready' worklist + populate priority on history.

Two passes:

1. Worklist sweep — for every Transcription where worklist_status='ready' (and
   site uses Karisma), query the current Report.Instance state via the dual-path
   sync helper and mark verified when Karisma already has typed text.

2. Priority backfill — for every Transcription where priority_name IS NULL,
   look up ReportCompletionPriorityType via Request.Record. We match by
   InternalIdentifier (accession) to avoid the Path-1/Path-2 split.
"""

from __future__ import annotations

import datetime
import logging
import sys
import time

import pymssql

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal, init_db
from crowdtrans.karisma import (
    _get_connection,
    fetch_worklist_sync_state,
    fetch_worklist_sync_state_by_accession,
)
from crowdtrans.models import Transcription
from crowdtrans.transcriber.service import _VERIFIED_PROCESS_STATUSES

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def sweep_ready(site_id: str, batch_size: int = 2000) -> tuple[int, int]:
    """Run the new dual-path sync over all 'ready' items.

    Returns (verified, refreshed).
    """
    store = get_config_store()
    site = store.get_site(site_id)
    if not site:
        log.error("Unknown site %s", site_id)
        return 0, 0
    if site.ris_type != "karisma":
        return 0, 0

    total_verified = 0
    total_refreshed = 0
    page = 0
    last_id = 0
    while True:
        with SessionLocal() as session:
            ready = (
                session.query(Transcription)
                .filter(
                    Transcription.site_id == site_id,
                    Transcription.status == "complete",
                    Transcription.worklist_status == "ready",
                    Transcription.source_dictation_id.isnot(None),
                    Transcription.id > last_id,
                )
                .order_by(Transcription.id.asc())
                .limit(batch_size)
                .all()
            )
            if not ready:
                break
            last_id = ready[-1].id

            accessions = list({t.internal_identifier for t in ready if t.internal_identifier})
            tk_orphans = [int(t.source_dictation_id) for t in ready if not t.internal_identifier]
            t0 = time.time()
            by_acc = fetch_worklist_sync_state_by_accession(site, accessions)
            by_tk = fetch_worklist_sync_state(site, tk_orphans) if tk_orphans else {}

            now = datetime.datetime.utcnow()
            verified_here = 0
            refreshed_here = 0
            for t in ready:
                entry = None
                if t.internal_identifier:
                    entry = by_acc.get(t.internal_identifier)
                if entry is None:
                    entry = by_tk.get(int(t.source_dictation_id))
                if not entry:
                    continue
                status = entry.get("process_status")
                rik = entry.get("report_instance_key")
                pname = entry.get("priority_name")
                prank = entry.get("priority_rank")

                changed = False
                if rik and t.report_instance_key != rik:
                    t.report_instance_key = rik
                    changed = True
                if status is not None and t.report_process_status != status:
                    t.report_process_status = status
                    changed = True
                if pname and t.priority_name != pname:
                    t.priority_name = pname
                    changed = True
                if prank is not None and t.priority_rank != prank:
                    t.priority_rank = prank
                    changed = True
                if changed:
                    refreshed_here += 1

                if status not in _VERIFIED_PROCESS_STATUSES:
                    continue
                t.worklist_status = "verified"
                t.verified_at = now
                t.verified_by = "karisma"
                verified_here += 1
            session.commit()
            total_verified += verified_here
            total_refreshed += refreshed_here
            page += 1
            log.info(
                "page %d  batch=%d  verified=%d  refreshed=%d  (%.1fs)  cumulative: verified=%d refreshed=%d  last_id=%d",
                page, len(ready), verified_here, refreshed_here,
                time.time() - t0, total_verified, total_refreshed, last_id,
            )
    return total_verified, total_refreshed


PRIORITY_LOOKUP_BY_ACCESSION = """\
SELECT RR.InternalIdentifier AS Accession,
       PT.[Name] AS PriorityName,
       PT.[Rank] AS PriorityRank
FROM [Version].[Karisma.Request.Record] RR
LEFT JOIN [Version].[Karisma.Request.PriorityType] PT
    ON PT.[Key] = RR.ReportCompletionPriorityTypeKey
WHERE RR.InternalIdentifier IN ({placeholders})
  AND RR.Key_Deleted = 0
"""


def backfill_priority(site_id: str, batch_size: int = 500) -> int:
    """Populate priority_name/priority_rank on rows where it is currently NULL."""
    store = get_config_store()
    site = store.get_site(site_id)
    if not site or site.ris_type != "karisma":
        return 0

    updated = 0
    page = 0
    last_id = 0
    while True:
        with SessionLocal() as session:
            rows = (
                session.query(Transcription.id, Transcription.internal_identifier)
                .filter(
                    Transcription.site_id == site_id,
                    Transcription.priority_name.is_(None),
                    Transcription.internal_identifier.isnot(None),
                    Transcription.id > last_id,
                )
                .order_by(Transcription.id.asc())
                .limit(batch_size)
                .all()
            )
            if not rows:
                break
            last_id = rows[-1][0]
            accessions = list({r[1] for r in rows if r[1]})
            if not accessions:
                continue

            conn = _get_connection(site)
            try:
                placeholders = ",".join(["%s"] * len(accessions))
                with conn.cursor(as_dict=True) as cur:
                    cur.execute(
                        PRIORITY_LOOKUP_BY_ACCESSION.format(placeholders=placeholders),
                        tuple(accessions),
                    )
                    by_acc: dict[str, tuple[str, int]] = {}
                    for r in cur:
                        if r["PriorityName"] is None:
                            continue
                        by_acc[r["Accession"]] = (r["PriorityName"], r["PriorityRank"])
            finally:
                conn.close()

            if not by_acc:
                log.info("page %d  batch=%d  no priorities matched — continuing", page, len(rows))
                continue

            local_updated = 0
            for row_id, accession in rows:
                hit = by_acc.get(accession)
                if not hit:
                    continue
                pname, prank = hit
                t = session.get(Transcription, row_id)
                if t and t.priority_name is None:
                    t.priority_name = pname
                    t.priority_rank = prank
                    local_updated += 1
            session.commit()
            updated += local_updated
            page += 1
            log.info(
                "priority page %d  batch=%d  updated=%d  cumulative=%d  last_id=%d",
                page, len(rows), local_updated, updated, last_id,
            )
    return updated


def main():
    init_db()  # ensures the new columns exist
    site_id = sys.argv[1] if len(sys.argv) > 1 else "karisma"

    log.info("=== Phase 1: Sweep stale 'ready' items through dual-path sync ===")
    verified, refreshed = sweep_ready(site_id)
    log.info("Phase 1 complete: %d verified, %d field refreshes", verified, refreshed)

    log.info("=== Phase 2: Backfill priority for rows where it's NULL ===")
    populated = backfill_priority(site_id)
    log.info("Phase 2 complete: %d rows received priority", populated)


if __name__ == "__main__":
    main()
