"""Learning dashboard — view and trigger the continuous learning agent."""

import json
import logging
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from crowdtrans.web.app import templates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/learning")

_DATA_PATHS = [
    Path("/opt/crowdtrans/data"),
    Path(__file__).resolve().parent.parent.parent.parent / "data",
]


def _load_json(filename: str) -> dict | None:
    for p in _DATA_PATHS:
        f = p / filename
        if f.exists():
            try:
                return json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                pass
    return None


@router.get("/")
def learning_dashboard(request: Request):
    """Show learning results — profiles, suggestions, correction candidates."""
    profiles = _load_json("doctor_profiles.json") or {}
    suggestions = _load_json("learning_suggestions.json") or {}

    # Summarise profiles
    profile_summaries = []
    for doc_id, profile in profiles.items():
        modalities = profile.get("modalities", {})
        total_count = sum(m.get("count", 0) for m in modalities.values())
        avg_sim = 0
        if modalities:
            sims = [m.get("avg_similarity", 0) for m in modalities.values() if m.get("count", 0) > 0]
            avg_sim = sum(sims) / len(sims) if sims else 0
        corrections_count = sum(
            len(m.get("word_corrections", [])) for m in modalities.values()
        )
        profile_summaries.append({
            "doctor_id": doc_id,
            "name": profile.get("doctor_name", "Unknown"),
            "modalities": list(modalities.keys()),
            "total_count": total_count,
            "avg_similarity": round(avg_sim, 1),
            "corrections_count": corrections_count,
        })
    profile_summaries.sort(key=lambda x: x["total_count"], reverse=True)

    return templates.TemplateResponse("learning/dashboard.html", {
        "request": request,
        "stats": suggestions.get("stats", {}),
        "profile_summaries": profile_summaries,
        "global_corrections": suggestions.get("global_corrections", [])[:30],
        "transcript_only": suggestions.get("transcript_only_words", [])[:20],
        "report_only": suggestions.get("report_only_words", [])[:20],
    })


@router.get("/profile/{doctor_id}")
def doctor_profile_detail(request: Request, doctor_id: str):
    """Detailed view of a single doctor's learned profile."""
    profiles = _load_json("doctor_profiles.json") or {}
    profile = profiles.get(doctor_id)
    if not profile:
        return JSONResponse({"error": "Doctor profile not found"}, status_code=404)

    return templates.TemplateResponse("learning/profile.html", {
        "request": request,
        "doctor_id": doctor_id,
        "profile": profile,
    })


def _run_learning_task():
    """Background task to run the learning pipeline."""
    try:
        from crowdtrans.database import init_db
        from crowdtrans.transcriber.learner import run_learning
        init_db()
        run_learning(reformat=True)
    except Exception:
        logger.exception("Learning task failed")


@router.post("/run")
def trigger_learning(background_tasks: BackgroundTasks):
    """Trigger the learning pipeline as a background task."""
    background_tasks.add_task(_run_learning_task)
    return JSONResponse({"status": "started", "message": "Learning pipeline started in background"})
