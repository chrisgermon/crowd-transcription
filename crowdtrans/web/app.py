"""FastAPI application factory."""

import secrets
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

WEB_DIR = Path(__file__).parent

app = FastAPI(title="CrowdScription", docs_url=None, redoc_url=None)

# --- HTTP Basic Auth middleware (protects all routes) ---
AUTH_USERNAME = "admin"
AUTH_PASSWORD = "admin"

SETTINGS_USERNAME = "admin"
SETTINGS_PASSWORD = "Crowdbot1@"


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Allow static files through without auth
        if request.url.path.startswith("/static"):
            return await call_next(request)

        # Settings routes require separate credentials
        is_settings = request.url.path.startswith("/settings")
        required_user = SETTINGS_USERNAME if is_settings else AUTH_USERNAME
        required_pass = SETTINGS_PASSWORD if is_settings else AUTH_PASSWORD
        realm = "CrowdScription Settings" if is_settings else "CrowdScription"

        auth = request.headers.get("authorization")
        if auth:
            import base64
            try:
                scheme, credentials = auth.split(" ", 1)
                if scheme.lower() == "basic":
                    decoded = base64.b64decode(credentials).decode("utf-8")
                    username, password = decoded.split(":", 1)
                    if (
                        secrets.compare_digest(username, required_user)
                        and secrets.compare_digest(password, required_pass)
                    ):
                        return await call_next(request)
            except Exception:
                pass

        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": f'Basic realm="{realm}"'},
        )


app.add_middleware(BasicAuthMiddleware)

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

# Import routes after templates is defined to avoid circular import
from crowdtrans.web.routes import api, compare, dashboard, learning, settings, transcriptions  # noqa: E402

app.include_router(dashboard.router)
app.include_router(transcriptions.router)
app.include_router(compare.router)
app.include_router(learning.router)
app.include_router(settings.router)
app.include_router(api.router)
