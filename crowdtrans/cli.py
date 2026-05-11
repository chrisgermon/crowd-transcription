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
    ssl_kwargs = {}
    if settings.ssl_certfile and settings.ssl_keyfile:
        ssl_kwargs["ssl_certfile"] = settings.ssl_certfile
        ssl_kwargs["ssl_keyfile"] = settings.ssl_keyfile
    uvicorn.run(
        "crowdtrans.web.app:app",
        host=settings.web_host,
        port=settings.web_port,
        reload=False,
        **ssl_kwargs,
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
            _pn = " ".join(p for p in [txn.patient_given_names, txn.patient_family_name] if p) or None
            txn.formatted_text = format_transcript(
                txn.transcript_text,
                modality_code=txn.modality_code,
                procedure_description=txn.procedure_description,
                clinical_history=txn.complaint,
                doctor_id=txn.doctor_id,
                patient_name=_pn,
                patient_ur=txn.patient_ur,
            )
            if i % 50 == 0:
                session.commit()
                click.echo(f"  {i}/{len(txns)} done")
        session.commit()
        click.echo(f"Done. Re-formatted {len(txns)} transcriptions.")


@cli.command()
@click.option("--no-reformat", is_flag=True, help="Skip reformatting transcriptions after learning")
def learn(no_reformat):
    """Analyze transcript-report pairs, update doctor profiles, and discover new rules.

    Compares all completed Visage transcriptions against their final reports to:
    - Build/update per-doctor formatting profiles (section structures, word corrections)
    - Discover candidate global corrections (Deepgram mishears, spelling patterns)
    - Generate a suggestions report at /opt/crowdtrans/data/learning_suggestions.json

    By default also reformats all transcriptions with the updated profiles.
    """
    from crowdtrans.database import init_db
    from crowdtrans.transcriber.learner import run_learning

    init_db()
    results = run_learning(reformat=not no_reformat)

    stats = results["stats"]
    click.echo(f"\nLearning complete:")
    click.echo(f"  Pairs analyzed:       {stats['pairs']:,}")
    click.echo(f"  Avg similarity:       {stats['avg_similarity']:.1f}%")
    click.echo(f"  Doctor profiles:      {stats['doctors']}")
    click.echo(f"  Correction candidates: {stats['correction_candidates']}")

    if results["global_corrections"]:
        click.echo(f"\nTop correction candidates (add to formatter if validated):")
        for c in results["global_corrections"][:15]:
            click.echo(f"  {c['transcript']:20s} -> {c['report']:20s}  ({c['count']}x)")

    if results["transcript_only_words"]:
        click.echo(f"\nTranscript-only words (possible fillers/mishears):")
        for w in results["transcript_only_words"][:10]:
            click.echo(f"  '{w['word']}' ({w['transcript_count']}x transcript, {w['report_count']}x report)")

    click.echo(f"\nSuggestions saved to data/learning_suggestions.json")


@cli.command("llm-test")
@click.option("--id", "txn_id", type=int, required=True, help="Transcription ID to format")
def llm_test(txn_id):
    """Test LLM formatting on a single transcription (prints regex vs LLM output)."""
    from crowdtrans.database import SessionLocal, init_db
    from crowdtrans.models import Transcription
    from crowdtrans.transcriber.formatter import format_transcript
    from crowdtrans.transcriber.llm_client import llm_format

    init_db()
    with SessionLocal() as session:
        txn = session.query(Transcription).filter_by(id=txn_id).first()
        if not txn:
            click.echo(f"Transcription {txn_id} not found.", err=True)
            sys.exit(1)
        if not txn.transcript_text:
            click.echo(f"Transcription {txn_id} has no transcript text.", err=True)
            sys.exit(1)

        click.echo(f"=== Transcription {txn_id} ({txn.accession_number or 'no accession'}) ===")
        click.echo(f"Doctor: {txn.doctor_family_name or '?'} | Modality: {txn.modality_code or '?'}")
        click.echo(f"Procedure: {txn.procedure_description or '?'}")
        click.echo()

        # Regex formatting
        _pn = " ".join(p for p in [txn.patient_given_names, txn.patient_family_name] if p) or None
        regex_result = format_transcript(
            txn.transcript_text,
            modality_code=txn.modality_code,
            procedure_description=txn.procedure_description,
            clinical_history=txn.complaint,
            doctor_id=txn.doctor_id,
            patient_name=_pn,
            patient_ur=txn.patient_ur,
        )
        click.echo("--- REGEX FORMATTED ---")
        click.echo(regex_result)
        click.echo()

        # LLM formatting
        try:
            llm_result = llm_format(
                txn.transcript_text,
                modality_code=txn.modality_code,
                procedure_description=txn.procedure_description,
                clinical_history=txn.complaint,
                doctor_id=txn.doctor_id,
            )
            click.echo("--- LLM FORMATTED ---")
            click.echo(llm_result.formatted_text)
            click.echo()
            click.echo(
                f"LLM stats: {llm_result.model} | {llm_result.duration_ms}ms | "
                f"{llm_result.input_tokens} in / {llm_result.output_tokens} out tokens"
            )
        except Exception as e:
            click.echo(f"LLM formatting failed: {e}", err=True)


@cli.command("llm-reformat")
@click.option("--limit", default=0, type=int, help="Max transcriptions to process (0 = all)")
def llm_reformat(limit):
    """Backfill LLM formatting on all completed transcriptions."""
    from crowdtrans.database import SessionLocal, init_db
    from crowdtrans.models import Transcription
    from crowdtrans.transcriber.llm_client import llm_format

    init_db()
    with SessionLocal() as session:
        query = (
            session.query(Transcription)
            .filter(
                Transcription.status == "complete",
                Transcription.transcript_text.isnot(None),
            )
        )
        if limit > 0:
            query = query.limit(limit)
        txns = query.all()

        click.echo(f"LLM-reformatting {len(txns)} transcriptions...")
        success = 0
        failed = 0
        for i, txn in enumerate(txns, 1):
            try:
                result = llm_format(
                    txn.transcript_text,
                    modality_code=txn.modality_code,
                    procedure_description=txn.procedure_description,
                    clinical_history=txn.complaint,
                    doctor_id=txn.doctor_id,
                )
                txn.llm_formatted_text = result.formatted_text
                txn.llm_model_used = result.model
                txn.llm_format_duration_ms = result.duration_ms
                txn.llm_input_tokens = result.input_tokens
                txn.llm_output_tokens = result.output_tokens
                txn.formatting_method = "hybrid"
                success += 1
            except Exception as e:
                click.echo(f"  [{i}] Failed ({txn.accession_number}): {e}", err=True)
                failed += 1

            if i % 10 == 0:
                session.commit()
                click.echo(f"  {i}/{len(txns)} done ({success} ok, {failed} failed)")

        session.commit()
        click.echo(f"\nDone. {success} formatted, {failed} failed out of {len(txns)} total.")


if __name__ == "__main__":
    cli()
