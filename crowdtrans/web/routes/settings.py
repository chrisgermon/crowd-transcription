"""Settings routes — view/edit global config and site configs."""

import logging
import subprocess
from typing import List

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Radiologist, Transcription
from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings")


@router.get("/")
def settings_page(request: Request):
    store = get_config_store()
    globals_ = store.get_all_globals()
    sites = store.get_all_site_rows()

    # Build worksite list with exclusion status
    excluded_raw = store.get_global("excluded_worksites") or ""
    excluded_set = {s.strip() for s in excluded_raw.split(",") if s.strip()}

    worksites = []
    with SessionLocal() as session:
        from sqlalchemy import func
        rows = (
            session.query(
                Transcription.facility_name,
                func.count(Transcription.id).label("cnt"),
            )
            .filter(Transcription.facility_name.isnot(None))
            .group_by(Transcription.facility_name)
            .order_by(Transcription.facility_name)
            .all()
        )
        for name, cnt in rows:
            worksites.append({
                "name": name,
                "count": cnt,
                "excluded": name in excluded_set,
            })

    # Load radiologists
    with SessionLocal() as session:
        radiologists = session.query(Radiologist).order_by(Radiologist.surname).all()
        # Detach from session for template use
        radiologists = [
            {
                "id": r.id,
                "doctor_code": r.doctor_code or "",
                "title": r.title or "",
                "first_name": r.first_name,
                "surname": r.surname,
                "qualifications": r.qualifications or "",
                "role": r.role or "",
                "signature_text": r.signature_text or "",
                "enabled": r.enabled,
            }
            for r in radiologists
        ]

    return templates.TemplateResponse("settings/index.html", {
        "request": request,
        "globals": globals_,
        "sites": sites,
        "worksites": worksites,
        "radiologists": radiologists,
    })


@router.post("/worksites")
async def save_worksites(request: Request):
    """Save excluded worksites from checkbox form."""
    form = await request.form()
    excluded = form.getlist("excluded")
    store = get_config_store()
    store.set_global("excluded_worksites", ",".join(excluded))
    logger.info("Updated excluded worksites: %d sites excluded", len(excluded))
    return RedirectResponse("/settings/", status_code=303)


@router.post("/auth")
def save_auth(
    request: Request,
    ad_server: str = Form("10.17.10.10"),
    ad_domain: str = Form("images.local"),
):
    store = get_config_store()
    store.set_global("ad_server", ad_server.strip())
    store.set_global("ad_domain", ad_domain.strip())
    logger.info("Updated AD settings: server=%s domain=%s", ad_server, ad_domain)
    return RedirectResponse("/settings/", status_code=303)


@router.post("/auth/test")
def test_ad_connection(request: Request):
    """Test connectivity to the Active Directory server."""
    store = get_config_store()
    ad_server = store.get_global("ad_server") or "10.17.10.10"
    ad_domain = store.get_global("ad_domain") or "images.local"

    import socket
    try:
        sock = socket.create_connection((ad_server, 389), timeout=5)
        sock.close()
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": True,
            "message": f"Connected to LDAP at {ad_server}:389 (domain: {ad_domain})",
        })
    except Exception as e:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": f"Cannot reach {ad_server}:389 — {e}",
        })


@router.post("/global")
def save_global(
    request: Request,
    ris_type: str = Form("visage"),
    deepgram_api_key: str = Form(""),
    deepgram_model: str = Form("nova-3-medical"),
    deepgram_language: str = Form("en-AU"),
    poll_interval_seconds: str = Form("30"),
    batch_size: str = Form("10"),
    anthropic_api_key: str = Form(""),
    llm_model: str = Form("claude-sonnet-4-20250514"),
    llm_mode: str = Form("off"),
    llm_ab_test_pct: str = Form("50"),
):
    store = get_config_store()
    data = {
        "ris_type": ris_type,
        "deepgram_api_key": deepgram_api_key,
        "deepgram_model": deepgram_model,
        "deepgram_language": deepgram_language,
        "poll_interval_seconds": poll_interval_seconds,
        "batch_size": batch_size,
        "anthropic_api_key": anthropic_api_key,
        "llm_model": llm_model,
        "llm_mode": llm_mode,
        "llm_ab_test_pct": llm_ab_test_pct,
    }
    store.save_globals(data)

    # Reset LLM client if API key changed so it picks up the new key
    try:
        from crowdtrans.transcriber.llm_client import reset_client
        reset_client()
    except Exception:
        pass

    logger.info("Global settings updated (LLM mode: %s)", llm_mode)
    return RedirectResponse("/settings/", status_code=303)


@router.post("/llm/test")
def test_llm(request: Request):
    """Test LLM formatting with a sample transcript."""
    store = get_config_store()
    api_key = store.get_global("anthropic_api_key")
    if not api_key:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": "Anthropic API key not configured",
        })

    sample_text = (
        "Clinical history is right shoulder pain. "
        "The findings are. There is no full thickness retrotter cuff tear. "
        "There is mild subacromial subdeltoid bursitis with bugling. "
        "The glenohumeral joint shows no fusion. "
        "Inclusion. Mild subacromial bursitis and impingement. "
        "No full thickness rotator cuff tear. Thank you."
    )

    try:
        from crowdtrans.transcriber.llm_client import llm_format, reset_client
        reset_client()  # Ensure fresh client with current key
        result = llm_format(
            sample_text,
            modality_code="US",
            procedure_description="US SHOULDER RIGHT",
            clinical_history="Right shoulder pain",
        )
        message = (
            f"Model: {result.model} | {result.duration_ms}ms | "
            f"{result.input_tokens} in / {result.output_tokens} out tokens\n\n"
            f"{result.formatted_text}"
        )
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": True,
            "message": message,
        })
    except Exception as e:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": str(e)[:500],
        })


@router.post("/sites/new")
def add_site(
    request: Request,
    site_id: str = Form(...),
    site_name: str = Form(...),
    ris_type: str = Form(...),
    db_host: str = Form(...),
    db_port: int = Form(...),
    db_name: str = Form(...),
    db_user: str = Form(...),
    db_password: str = Form(...),
    audio_source: str = Form("nfs"),
    audio_mount_path: str = Form(""),
    poll_interval_seconds: int = Form(30),
    batch_size: int = Form(10),
):
    store = get_config_store()
    store.save_site(
        site_id=site_id,
        site_name=site_name,
        ris_type=ris_type,
        enabled=True,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        audio_source=audio_source,
        audio_mount_path=audio_mount_path or None,
        poll_interval_seconds=poll_interval_seconds,
        batch_size=batch_size,
    )
    logger.info("Added new site '%s'", site_id)
    return RedirectResponse("/settings/", status_code=303)


@router.post("/sites/{site_id}")
def update_site(
    request: Request,
    site_id: str,
    site_name: str = Form(...),
    ris_type: str = Form(...),
    db_host: str = Form(...),
    db_port: int = Form(...),
    db_name: str = Form(...),
    db_user: str = Form(...),
    db_password: str = Form(""),
    audio_source: str = Form("nfs"),
    audio_mount_path: str = Form(""),
    poll_interval_seconds: int = Form(30),
    batch_size: int = Form(10),
):
    store = get_config_store()
    # If password is blank, keep existing
    if not db_password:
        existing = store.get_site_row(site_id)
        db_password = existing.db_password if existing else ""

    existing = store.get_site_row(site_id)
    enabled = existing.enabled if existing else True

    store.save_site(
        site_id=site_id,
        site_name=site_name,
        ris_type=ris_type,
        enabled=enabled,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
        audio_source=audio_source,
        audio_mount_path=audio_mount_path or None,
        poll_interval_seconds=poll_interval_seconds,
        batch_size=batch_size,
    )
    logger.info("Updated site '%s'", site_id)
    return RedirectResponse("/settings/", status_code=303)


@router.post("/sites/{site_id}/toggle")
def toggle_site(request: Request, site_id: str):
    store = get_config_store()
    new_state = store.toggle_site(site_id)
    site = store.get_site_row(site_id)
    label = "Enabled" if new_state else "Disabled"
    color = "green" if new_state else "gray"
    return HTMLResponse(
        f'<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-{color}-100 text-{color}-800">'
        f'{label}</span>'
    )


@router.post("/sites/{site_id}/delete")
def delete_site(request: Request, site_id: str):
    store = get_config_store()
    store.delete_site(site_id)
    return RedirectResponse("/settings/", status_code=303)


@router.post("/sites/{site_id}/test")
def test_connection(request: Request, site_id: str):
    store = get_config_store()
    site = store.get_site(site_id)
    if not site:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": f"Site '{site_id}' not found",
        })

    success = False
    message = ""

    try:
        if site.ris_type == "visage":
            import psycopg2
            conn = psycopg2.connect(
                host=site.db_host,
                port=site.db_port,
                dbname=site.db_name,
                user=site.db_user,
                password=site.db_password,
                connect_timeout=5,
            )
            conn.close()
            success = True
            message = f"Connected to PostgreSQL at {site.db_host}:{site.db_port}/{site.db_name}"
        elif site.ris_type == "karisma":
            import pymssql
            conn = pymssql.connect(
                server=site.db_host,
                port=str(site.db_port),
                database=site.db_name,
                user=site.db_user,
                password=site.db_password,
                login_timeout=5,
            )
            conn.close()
            success = True
            message = f"Connected to MSSQL at {site.db_host}:{site.db_port}/{site.db_name}"
        else:
            message = f"Unknown RIS type: {site.ris_type}"
    except Exception as e:
        message = str(e)[:300]

    return templates.TemplateResponse("settings/_test_result.html", {
        "request": request,
        "success": success,
        "message": message,
    })


# ------------------------------------------------------------------
# Radiologist management
# ------------------------------------------------------------------


@router.post("/radiologists/add")
async def add_radiologist(request: Request):
    form = await request.form()
    with SessionLocal() as session:
        rad = Radiologist(
            doctor_code=form.get("doctor_code", "").strip() or None,
            title=form.get("title", "").strip() or None,
            first_name=form.get("first_name", "").strip(),
            surname=form.get("surname", "").strip(),
            qualifications=form.get("qualifications", "").strip() or None,
            role=form.get("role", "").strip() or None,
            signature_text=form.get("signature_text", "").strip() or None,
            enabled=True,
        )
        session.add(rad)
        session.commit()
    logger.info("Added radiologist: %s %s", form.get("first_name"), form.get("surname"))
    # Clear cached radiologist signatures
    _clear_signature_cache()
    return RedirectResponse("/settings/#radiologists", status_code=303)


@router.post("/radiologists/{rad_id}")
async def update_radiologist(request: Request, rad_id: int):
    form = await request.form()
    with SessionLocal() as session:
        rad = session.query(Radiologist).filter_by(id=rad_id).first()
        if not rad:
            return RedirectResponse("/settings/", status_code=303)
        rad.doctor_code = form.get("doctor_code", "").strip() or None
        rad.title = form.get("title", "").strip() or None
        rad.first_name = form.get("first_name", "").strip()
        rad.surname = form.get("surname", "").strip()
        rad.qualifications = form.get("qualifications", "").strip() or None
        rad.role = form.get("role", "").strip() or None
        rad.signature_text = form.get("signature_text", "").strip() or None
        session.commit()
    logger.info("Updated radiologist #%d", rad_id)
    _clear_signature_cache()
    return RedirectResponse("/settings/#radiologists", status_code=303)


@router.post("/radiologists/{rad_id}/delete")
def delete_radiologist(request: Request, rad_id: int):
    with SessionLocal() as session:
        rad = session.query(Radiologist).filter_by(id=rad_id).first()
        if rad:
            session.delete(rad)
            session.commit()
            logger.info("Deleted radiologist #%d", rad_id)
    _clear_signature_cache()
    return RedirectResponse("/settings/#radiologists", status_code=303)


def _clear_signature_cache():
    """Clear the cached radiologist signatures in the formatter."""
    try:
        from crowdtrans.transcriber.formatter import _clear_radiologist_cache
        _clear_radiologist_cache()
    except Exception:
        pass


# ------------------------------------------------------------------
# Service control (systemd)
# ------------------------------------------------------------------

SERVICE_NAME = "crowdtrans-service.service"


@router.get("/service/status")
def service_status(request: Request):
    """Return the current status of the transcription service."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", SERVICE_NAME],
            capture_output=True, text=True, timeout=5,
        )
        active = result.stdout.strip() == "active"
    except Exception:
        active = False

    color = "green" if active else "gray"
    label = "Running" if active else "Stopped"
    return HTMLResponse(
        f'<span id="service-badge" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-{color}-100 text-{color}-800">'
        f'{label}</span>'
    )


@router.post("/service/stop")
def service_stop(request: Request):
    """Stop the transcription polling service."""
    try:
        subprocess.run(
            ["sudo", "systemctl", "stop", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        logger.info("Transcription service stopped via UI")
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": True,
            "message": "Transcription service stopped.",
        })
    except Exception as e:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": f"Failed to stop service: {e}",
        })


@router.post("/service/start")
def service_start(request: Request):
    """Start the transcription polling service."""
    try:
        subprocess.run(
            ["sudo", "systemctl", "start", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        logger.info("Transcription service started via UI")
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": True,
            "message": "Transcription service started.",
        })
    except Exception as e:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": f"Failed to start service: {e}",
        })


@router.post("/service/restart")
def service_restart(request: Request):
    """Restart the transcription polling service."""
    try:
        subprocess.run(
            ["sudo", "systemctl", "restart", SERVICE_NAME],
            capture_output=True, text=True, timeout=10,
        )
        logger.info("Transcription service restarted via UI")
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": True,
            "message": "Transcription service restarted.",
        })
    except Exception as e:
        return templates.TemplateResponse("settings/_test_result.html", {
            "request": request,
            "success": False,
            "message": f"Failed to restart service: {e}",
        })
