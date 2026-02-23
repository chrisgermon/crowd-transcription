"""Dashboard route — GET /."""

import datetime

from fastapi import APIRouter, Query, Request
from sqlalchemy import func

from crowdtrans.config_store import get_config_store
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription, Watermark
from crowdtrans.web.app import templates

router = APIRouter()


@router.get("/")
def dashboard(request: Request, site: str = Query("", description="Filter by site")):
    with SessionLocal() as session:
        base = session.query(Transcription)
        if site:
            base = base.filter(Transcription.site_id == site)

        # Status counts
        status_rows = (
            base.with_entities(Transcription.status, func.count())
            .group_by(Transcription.status)
            .all()
        )
        status_counts = dict(status_rows)
        total = sum(status_counts.values())

        # Today's transcriptions
        today = datetime.date.today()
        today_q = base.filter(
            Transcription.transcription_completed_at >= datetime.datetime.combine(today, datetime.time.min),
            Transcription.status == "complete",
        )
        today_count = today_q.count()

        # Average confidence
        avg_confidence = (
            base.filter(Transcription.status == "complete")
            .with_entities(func.avg(Transcription.confidence))
            .scalar()
        )

        # Recent 10 completed
        recent = (
            base.filter(Transcription.status == "complete")
            .order_by(Transcription.transcription_completed_at.desc())
            .limit(10)
            .all()
        )

        # Watermarks for all sites
        watermarks = session.query(Watermark).all()

        # Available sites for filter
        site_configs = get_config_store().get_site_configs()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "status_counts": status_counts,
        "total": total,
        "today_count": today_count or 0,
        "avg_confidence": avg_confidence,
        "recent": recent,
        "watermarks": watermarks,
        "site_configs": site_configs,
        "selected_site": site,
    })
