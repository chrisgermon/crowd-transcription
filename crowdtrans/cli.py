"""Click CLI for CrowdTrans."""

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
    """CrowdTrans: Radiology Dictation Transcription Platform."""


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


if __name__ == "__main__":
    cli()
