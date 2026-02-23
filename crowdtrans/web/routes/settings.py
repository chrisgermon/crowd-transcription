"""Settings routes — view/edit global config and site configs."""

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from crowdtrans.config_store import get_config_store
from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings")


@router.get("/")
def settings_page(request: Request):
    store = get_config_store()
    globals_ = store.get_all_globals()
    sites = store.get_all_site_rows()
    return templates.TemplateResponse("settings/index.html", {
        "request": request,
        "globals": globals_,
        "sites": sites,
    })


@router.post("/global")
def save_global(
    request: Request,
    deepgram_api_key: str = Form(""),
    deepgram_model: str = Form("nova-3-medical"),
    deepgram_language: str = Form("en-AU"),
    poll_interval_seconds: str = Form("30"),
    batch_size: str = Form("10"),
):
    store = get_config_store()
    store.save_globals({
        "deepgram_api_key": deepgram_api_key,
        "deepgram_model": deepgram_model,
        "deepgram_language": deepgram_language,
        "poll_interval_seconds": poll_interval_seconds,
        "batch_size": batch_size,
    })
    logger.info("Global settings updated")
    return RedirectResponse("/settings/", status_code=303)


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
