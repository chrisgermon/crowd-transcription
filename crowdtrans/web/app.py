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


class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Allow static files through without auth
        if request.url.path.startswith("/static"):
            return await call_next(request)

        auth = request.headers.get("authorization")
        if auth:
            import base64
            try:
                scheme, credentials = auth.split(" ", 1)
                if scheme.lower() == "basic":
                    decoded = base64.b64decode(credentials).decode("utf-8")
                    username, password = decoded.split(":", 1)
                    if (
                        secrets.compare_digest(username, AUTH_USERNAME)
                        and secrets.compare_digest(password, AUTH_PASSWORD)
                    ):
                        return await call_next(request)
            except Exception:
                pass

        return Response(
            content="Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="CrowdScription"'},
        )


app.add_middleware(BasicAuthMiddleware)

app.mount("/static", StaticFiles(directory=WEB_DIR / "static"), name="static")

templates = Jinja2Templates(directory=WEB_DIR / "templates")

# Import routes after templates is defined to avoid circular import
from crowdtrans.web.routes import api, compare, dashboard, learning, settings, transcriptions  # noqa: E402

app.include_router(dashboard.router)
app.include_router(transcriptions.router)
app.include_router(compare.router)
app.include_router(learning.router)
app.include_router(settings.router)
app.include_router(api.router)
