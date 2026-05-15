"""FastAPI application factory."""

import secrets
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

WEB_DIR = Path(__file__).parent

# Persist session secret so restarts don't invalidate sessions.
# Try the data dir first (when running as crowdtrans service), fall back to home dir.
_SECRET_CANDIDATES = [
    Path("/opt/crowdtrans/data/.session_secret"),
    Path.home() / ".crowdscription_session_secret",
]
_SESSION_SECRET = ""
for _sf in _SECRET_CANDIDATES:
    if _sf.exists():
        _SESSION_SECRET = _sf.read_text().strip()
        break
if not _SESSION_SECRET:
    _SESSION_SECRET = secrets.token_hex(32)
    for _sf in _SECRET_CANDIDATES:
        try:
            _sf.parent.mkdir(parents=True, exist_ok=True)
            _sf.write_text(_SESSION_SECRET)
            break
        except OSError:
            continue

app = FastAPI(title="CrowdScription", docs_url=None, redoc_url=None)


class _AuthMiddleware:
    """Redirect unauthenticated users to /login.

    Must be added *after* SessionMiddleware via add_middleware so that
    SessionMiddleware wraps us (runs first) and populates request.session.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path = scope["path"]
        if path.startswith("/static") or path in ("/login", "/logout") or path.startswith("/api/"):
            return await self.app(scope, receive, send)

        request = Request(scope, receive, send)
        if not request.session.get("user"):
            response = RedirectResponse(url="/login", status_code=302)
            return await response(scope, receive, send)

        return await self.app(scope, receive, send)


# Order matters: add_middleware wraps in reverse order, so SessionMiddleware
# is added last but runs first (outermost), then AuthMiddleware checks session.
app.add_middleware(_AuthMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=_SESSION_SECRET,
    session_cookie="crowdscription_session",
    max_age=28800,  # 8 hours
)


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


# All UI timestamps are rendered in Australian Eastern Time (AEST/AEDT — DST
# transitions handled automatically by Australia/Sydney). Times stored via
# datetime.utcnow() in our DB are naive UTC; times pulled from Karisma's
# LastDictationCompleteDateTime are naive AEST (the Karisma SQL Server runs
# at UTC+10), so we expose two filters.
import datetime as _datetime
try:
    from zoneinfo import ZoneInfo as _ZoneInfo
    _AEST = _ZoneInfo("Australia/Sydney")
except Exception:  # pragma: no cover — Python <3.9 fallback
    _AEST = _datetime.timezone(_datetime.timedelta(hours=10))


def _aest_from_utc(dt, fmt: str = "%Y-%m-%d %H:%M"):
    """Render a naive-UTC datetime as Australia/Sydney local time."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_datetime.timezone.utc)
    return dt.astimezone(_AEST).strftime(fmt)


def _aest_passthrough(dt, fmt: str = "%Y-%m-%d %H:%M"):
    """Render a datetime already in AEST (e.g. from Karisma) without conversion."""
    if dt is None:
        return ""
    return dt.strftime(fmt)


templates.env.filters["aest"] = _aest_from_utc
templates.env.filters["aest_local"] = _aest_passthrough

# Import routes after templates is defined to avoid circular import
from crowdtrans.web.routes import api, compare, dashboard, learning, login, settings, transcriptions, voice, worklist  # noqa: E402

app.include_router(login.router)
app.include_router(dashboard.router)
app.include_router(worklist.router)
app.include_router(voice.router)
app.include_router(transcriptions.router)
app.include_router(compare.router)
app.include_router(learning.router)
app.include_router(settings.router)
app.include_router(api.router)
