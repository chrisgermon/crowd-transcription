"""Click CLI for CrowdScription."""

import logging
import sys

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@click.group()
def cli():
    """CrowdScription: Radiology Dictation Transcription Platform."""


@cli.command()
def init_db():
    """Create SQLite schema and initialize watermarks for all enabled sites."""
    from crowdtrans.database import init_db
    init_db()
    click.echo("Database initialized successfully.")


@cli.command()
@click.option("--site", type=click.Choice(["visage", "karisma"]), default=None, help="Test a specific site (default: all enabled)")
def check_ris(site):
    """Test RIS database connectivity and show table counts."""
    from crowdtrans.config import settings

    sites = settings.get_site_configs()
    if site:
        sites = [s for s in sites if s.site_id == site]

    if not sites:
        click.echo("No matching sites enabled.", err=True)
        sys.exit(1)

    for s in sites:
        click.echo(f"\n=== {s.site_name} ({s.ris_type}) ===")
        try:
            if s.ris_type == "visage":
                from crowdtrans.visage import check_connection
                result = check_connection(s)
            elif s.ris_type == "karisma":
                from crowdtrans.karisma import check_connection
                result = check_connection(s)
            else:
                click.echo(f"  Unknown RIS type: {s.ris_type}", err=True)
                continue

            click.echo("  Connection: OK")
            for table, count in result["counts"].items():
                click.echo(f"  {table}: {count:,}")
        except Exception as e:
            click.echo(f"  Connection FAILED: {e}", err=True)


@cli.command()
@click.option("--site", type=click.Choice(["visage", "karisma"]), default=None, help="Process only this site")
def run_service(site):
    """Start the transcription polling daemon."""
    from crowdtrans.database import init_db
    from crowdtrans.transcriber.service import run
    init_db()
    run(site_id=site)


@cli.command()
def run_web():
    """Start the FastAPI web interface."""
    import uvicorn
    from crowdtrans.config import settings
    from crowdtrans.database import init_db
    init_db()
    uvicorn.run(
        "crowdtrans.web.app:app",
        host=settings.web_host,
        port=settings.web_port,
        reload=False,
    )


@cli.command()
@click.option("--site", type=click.Choice(["visage", "karisma"]), required=True, help="Site to backfill")
@click.option("--from-id", default=0, type=int, help="Reset watermark to this ID")
def backfill(site, from_id):
    """Reset watermark for reprocessing dictations on a specific site."""
    from crowdtrans.database import SessionLocal, init_db
    from crowdtrans.models import Watermark
    init_db()
    with SessionLocal() as session:
        wm = session.query(Watermark).filter_by(site_id=site).first()
        if not wm:
            click.echo(f"No watermark found for site '{site}'", err=True)
            sys.exit(1)
        old_id = wm.last_dictation_id
        wm.last_dictation_id = from_id
        session.commit()
        click.echo(f"[{site}] Watermark reset from {old_id} to {from_id}")
        click.echo("Run 'crowdtrans run-service' to begin reprocessing.")


@cli.command()
def sites():
    """List all configured sites and their status."""
    from crowdtrans.config import settings
    for s in settings.get_site_configs():
        status = "enabled" if s.enabled else "disabled"
        click.echo(f"  {s.site_id:12s}  {s.site_name:40s}  {s.ris_type:8s}  {s.db_host}:{s.db_port}  [{status}]")


@cli.command()
def reformat():
    """Re-format all completed transcriptions using the latest formatter."""
    from crowdtrans.database import SessionLocal, init_db
    from crowdtrans.models import Transcription
    from crowdtrans.transcriber.formatter import format_transcript
    init_db()
    with SessionLocal() as session:
        txns = (
            session.query(Transcription)
            .filter(Transcription.status == "complete", Transcription.transcript_text.isnot(None))
            .all()
        )
        click.echo(f"Re-formatting {len(txns)} completed transcriptions...")
        for i, txn in enumerate(txns, 1):
            txn.formatted_text = format_transcript(
                txn.transcript_text,
                modality_code=txn.modality_code,
                procedure_description=txn.procedure_description,
                clinical_history=txn.complaint,
                doctor_id=txn.doctor_id,
            )
            if i % 50 == 0:
                session.commit()
                click.echo(f"  {i}/{len(txns)} done")
        session.commit()
        click.echo(f"Done. Re-formatted {len(txns)} transcriptions.")


@cli.command()
@click.option("--limit", default=5000, type=int, help="Max reports to analyze")
def learn(limit):
    """Analyze Visage reports to show formatting patterns and validate classifier."""
    import re
    from collections import Counter, defaultdict

    from crowdtrans.config_store import get_config_store
    from crowdtrans.database import SessionLocal, init_db
    from crowdtrans.models import Transcription
    from crowdtrans.transcriber.formatter import format_transcript

    init_db()
    store = get_config_store()
    sites = store.get_enabled_site_configs()
    visage_site = next((s for s in sites if s.ris_type == "visage"), None)
    if not visage_site:
        click.echo("No enabled Visage site found.", err=True)
        sys.exit(1)

    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(
        host=visage_site.db_host, port=visage_site.db_port,
        dbname=visage_site.db_name, user=visage_site.db_user,
        password=visage_site.db_password,
        options="-c default_transaction_read_only=on",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    cur = conn.cursor()

    # Fetch matched report pairs
    cur.execute("""
        SELECT d.id AS dictation_id, cd.report_body,
               m.code AS modality_code, p.description AS procedure_description,
               doc.family_name AS doctor_family_name, dr.title AS report_title
        FROM dictation d
        JOIN clinical_document cd ON cd.id = d.clinical_document_id
        JOIN diagnostic_report dr ON dr.id = cd.diagnostic_report_id
        JOIN clinical_document_procedure cdp ON cdp.clinical_document_id = cd.id
        JOIN procedure_ p ON p.id = cdp.procedure_id
        LEFT JOIN procedure_type pt2 ON pt2.id = p.procedure_type_id
        LEFT JOIN modality m ON m.id = pt2.modality_id
        LEFT JOIN doctor doc ON doc.id = d.doctor_id
        WHERE cd.status = 'FINAL' AND d.duration > 0
        AND cd.report_body IS NOT NULL AND length(cd.report_body) > 50
        ORDER BY d.id DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    conn.close()

    click.echo(f"Analyzed {len(rows)} reports from Visage")

    # Extract heading sequences
    heading_seqs = defaultdict(Counter)
    for row in rows:
        html = row['report_body']
        mod = row['modality_code'] or 'UNKNOWN'
        headings = []
        plain = re.sub(r'<[^>]+>', ' ', html)
        for m in re.finditer(
            r'\b(CLINICAL HISTORY|CLINICAL INDICATION|CLINICAL DETAILS|FINDINGS|CONCLUSION|PROCEDURE|TECHNIQUE|IMPRESSION|COMMENT|REPORT)\b',
            plain,
        ):
            headings.append(m.group(1))
        seq = ' > '.join(headings) if headings else '(none)'
        heading_seqs[mod][seq] += 1

    click.echo("\nHeading patterns by modality (top 3 each):")
    for mod in sorted(heading_seqs.keys()):
        click.echo(f"  [{mod}]")
        for seq, c in heading_seqs[mod].most_common(3):
            click.echo(f"    {c:4d}x  {seq}")

    # Compare with our transcriptions if available
    with SessionLocal() as session:
        our_txns = (
            session.query(Transcription)
            .filter(Transcription.status == "complete", Transcription.formatted_text.isnot(None))
            .limit(10)
            .all()
        )
        if our_txns:
            click.echo(f"\nSample formatted output (latest {len(our_txns)}):")
            for txn in our_txns[:3]:
                click.echo(f"\n  --- Dictation {txn.source_dictation_id} | {txn.modality_code} ---")
                for line in txn.formatted_text.split('\n')[:8]:
                    click.echo(f"  {line}")
                if txn.formatted_text.count('\n') > 8:
                    click.echo(f"  ... ({txn.formatted_text.count(chr(10)) + 1} lines total)")

    click.echo("\nDone.")


if __name__ == "__main__":
    cli()
