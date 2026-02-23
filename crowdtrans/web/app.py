"""FastAPI application factory."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from crowdtrans.web.routes import api, dashboard, settings, transcriptions

WEB_DIR = Path(__file__).parent

app = FastAPI(title="CrowdTrans", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory=WEB_DIR / "static"), name="static")

templates = Jinja2Templates(directory=WEB_DIR / "templates")

app.include_router(dashboard.router)
app.include_router(transcriptions.router)
app.include_router(settings.router)
app.include_router(api.router)
