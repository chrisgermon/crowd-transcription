"""FastAPI application factory."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

WEB_DIR = Path(__file__).parent

app = FastAPI(title="CrowdScription", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory=WEB_DIR / "static"), name="static")

templates = Jinja2Templates(directory=WEB_DIR / "templates")


def _get_ris_name() -> str:
    """Return the human-readable name of the configured RIS system."""
    try:
        from crowdtrans.config_store import get_config_store
        ris_type = get_config_store().get_global("ris_type") or "visage"
        return ris_type.capitalize()
    except Exception:
        return "Visage"


templates.env.globals["ris_name"] = _get_ris_name


def _from_json(value):
    """Jinja2 filter: parse a JSON string into a Python object."""
    import json
    if not value:
        return []
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []


templates.env.filters["from_json"] = _from_json

# Import routes after templates is defined to avoid circular import
from crowdtrans.web.routes import api, compare, dashboard, learning, settings, transcriptions, worklist  # noqa: E402

app.include_router(dashboard.router)
app.include_router(worklist.router)
app.include_router(transcriptions.router)
app.include_router(compare.router)
app.include_router(learning.router)
app.include_router(settings.router)
app.include_router(api.router)
