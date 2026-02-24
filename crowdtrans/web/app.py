"""FastAPI application factory."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

WEB_DIR = Path(__file__).parent

app = FastAPI(title="CrowdScription", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory=WEB_DIR / "static"), name="static")

templates = Jinja2Templates(directory=WEB_DIR / "templates")

# Import routes after templates is defined to avoid circular import
from crowdtrans.web.routes import api, compare, dashboard, settings, transcriptions  # noqa: E402

app.include_router(dashboard.router)
app.include_router(transcriptions.router)
app.include_router(compare.router)
app.include_router(settings.router)
app.include_router(api.router)
