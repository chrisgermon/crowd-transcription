"""Local cache for Karisma referral PDFs and worksheet images.

The Karisma SILO + MSSQL lookups are heavyweight per request, so we cache
attachments locally as soon as a transcription is complete. Cache lives at
``settings.attachments_cache_dir / <txn_id>/`` with a ``metadata.json``
describing each file. Bytes that are stored "external" in Karisma but
unreachable through our SILO mount get recorded with ``filename = null`` so
the API can still render the "open in Karisma" pill.

Lifecycle:
  - cache_attachments(site, txn)   — called after a successful transcribe
  - read_cache(txn_id)             — used by the API instead of hitting Karisma
  - drop_cache(txn_id)             — called when the txn leaves the 'ready' bucket
  - maintain(session, site, limit) — periodic sweep run by the service loop
"""

from __future__ import annotations

import datetime
import json
import logging
import re
from pathlib import Path
from typing import Any

from crowdtrans.config import SiteConfig, settings

logger = logging.getLogger(__name__)

_SAFE_NAME_RX = re.compile(r"[^A-Za-z0-9._-]+")


def _cache_root() -> Path:
    return Path(settings.attachments_cache_dir)


def _txn_dir(txn_id: int) -> Path:
    return _cache_root() / str(int(txn_id))


def _safe_name(name: str | None) -> str:
    if not name:
        return "file"
    base = _SAFE_NAME_RX.sub("_", name).strip("._-")
    return base[:80] or "file"


def has_cache(txn_id: int) -> bool:
    return (_txn_dir(txn_id) / "metadata.json").is_file()


def drop_cache(txn_id: int) -> bool:
    """Remove the cached attachments for a transcription. Idempotent."""
    d = _txn_dir(txn_id)
    if not d.exists():
        return False
    try:
        for p in d.iterdir():
            try:
                p.unlink()
            except OSError:
                pass
        d.rmdir()
        return True
    except OSError as e:
        logger.warning("Failed to drop attachment cache for %s: %s", txn_id, e)
        return False


def read_cache(txn_id: int) -> dict[str, Any] | None:
    """Return the cached payload for a transcription, or None if absent."""
    meta_path = _txn_dir(txn_id) / "metadata.json"
    if not meta_path.is_file():
        return None
    try:
        with meta_path.open() as f:
            meta = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Bad metadata.json for %s: %s — dropping cache", txn_id, e)
        drop_cache(txn_id)
        return None

    def _hydrate(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for e in entries:
            filename = e.get("filename")
            data = b""
            if filename:
                fp = _txn_dir(txn_id) / filename
                try:
                    data = fp.read_bytes()
                except OSError:
                    data = b""
            out.append({
                "name": e.get("name") or "",
                "format": e.get("format") or "",
                "length": e.get("length") or 0,
                "external": bool(e.get("external")),
                "data": data,
            })
        return out

    return {
        "referrals": _hydrate(meta.get("referrals", [])),
        "worksheets": _hydrate(meta.get("worksheets", [])),
        "cached_at": meta.get("cached_at"),
    }


def cache_attachments(site: SiteConfig, txn) -> dict[str, int]:
    """Fetch referrals + worksheets for ``txn`` and persist them under the cache dir.

    Returns ``{"referrals": n_inline_bytes_saved, "worksheets": m_inline, ...}``
    for log readability. Safe to call multiple times; overwrites previous cache.
    Only meaningful for site.ris_type == 'karisma'.
    """
    if site.ris_type != "karisma":
        return {"referrals": 0, "worksheets": 0, "external_only": 0}

    from crowdtrans.karisma import (
        fetch_referral_attachments,
        fetch_worksheet_attachments,
        fetch_form_attachments,
    )

    request_key = None
    try:
        request_key = int(txn.order_id) if txn.order_id else None
    except (TypeError, ValueError):
        request_key = None
    rik = getattr(txn, "report_instance_key", None)

    referrals: list[dict[str, Any]] = []
    worksheets: list[dict[str, Any]] = []
    if request_key:
        try:
            referrals = fetch_referral_attachments(site, request_key)
        except Exception:
            logger.exception("[%s] cache_attachments: referrals fetch failed for %s",
                             site.site_id, txn.id)
        # Karisma also stores referrals + sono-review worksheets via Form.Image —
        # this is the high-volume path (~8M rows vs ~1k in Request.Attachment).
        try:
            forms = fetch_form_attachments(site, request_key)
            referrals.extend(forms.get("documents", []))
            worksheets.extend(forms.get("sono_review", []))
        except Exception:
            logger.exception("[%s] cache_attachments: form fetch failed for %s",
                             site.site_id, txn.id)
    if rik:
        try:
            worksheets.extend(fetch_worksheet_attachments(site, rik))
        except Exception:
            logger.exception("[%s] cache_attachments: worksheets fetch failed for %s",
                             site.site_id, txn.id)

    # Filter to actionable entries only (have bytes OR are flagged external so
    # the UI can render the "open in Karisma" pill).
    def _keep(att: dict[str, Any]) -> bool:
        return bool(att.get("data")) or bool(att.get("external"))

    referrals = [a for a in referrals if _keep(a)]
    worksheets = [a for a in worksheets if _keep(a)]

    if not referrals and not worksheets:
        # Nothing to cache — drop any stale dir, then record an empty marker.
        drop_cache(txn.id)
        d = _txn_dir(txn.id)
        d.mkdir(parents=True, exist_ok=True)
        meta_path = d / "metadata.json"
        meta_path.write_text(json.dumps({
            "cached_at": datetime.datetime.utcnow().isoformat() + "Z",
            "referrals": [],
            "worksheets": [],
        }))
        return {"referrals": 0, "worksheets": 0, "external_only": 0}

    d = _txn_dir(txn.id)
    d.mkdir(parents=True, exist_ok=True)
    inline = 0
    external_only = 0

    def _persist(kind_prefix: str, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        nonlocal inline, external_only
        out = []
        for idx, att in enumerate(entries):
            data = att.get("data") or b""
            fmt = (att.get("format") or "").lstrip(".") or "bin"
            name = att.get("name") or f"{kind_prefix}-{idx}"
            entry: dict[str, Any] = {
                "name": name,
                "format": fmt,
                "length": att.get("length") or len(data),
                "external": bool(att.get("external")),
                "filename": None,
            }
            if data:
                fname = f"{kind_prefix}-{idx}-{_safe_name(name)}.{fmt}"
                (d / fname).write_bytes(data)
                entry["filename"] = fname
                inline += 1
            else:
                external_only += 1
            out.append(entry)
        return out

    meta = {
        "cached_at": datetime.datetime.utcnow().isoformat() + "Z",
        "referrals": _persist("ref", referrals),
        "worksheets": _persist("ws", worksheets),
    }
    tmp = d / "metadata.json.tmp"
    tmp.write_text(json.dumps(meta))
    tmp.replace(d / "metadata.json")
    return {
        "referrals": len(meta["referrals"]),
        "worksheets": len(meta["worksheets"]),
        "external_only": external_only,
    }


def maintain(session, site: SiteConfig, limit: int = 50) -> dict[str, int]:
    """Service-loop maintenance: pre-cache ready items missing a cache, prune the rest.

    Two passes:
      1. Pre-cache up to ``limit`` Transcription rows where worklist_status='ready'
         and no cache exists yet.
      2. Drop any cache directory whose owner is not 'ready' (or no longer exists).

    Cheap to call on every poll cycle: directory probing only, plus at most
    ``limit`` SILO fetches.
    """
    if site.ris_type != "karisma":
        return {"cached": 0, "pruned": 0}

    from crowdtrans.models import Transcription  # local import: avoid cycle

    cached = 0
    pruned = 0

    # --- Pre-cache ready-but-uncached items ---
    candidates = (
        session.query(Transcription)
        .filter(
            Transcription.site_id == site.site_id,
            Transcription.status == "complete",
            Transcription.worklist_status == "ready",
        )
        .order_by(Transcription.priority_rank.asc().nullslast(),
                  Transcription.dictation_date.desc().nullslast())
        .limit(limit * 4)  # over-fetch so we can skip ones already cached
        .all()
    )
    for txn in candidates:
        if cached >= limit:
            break
        if has_cache(txn.id):
            continue
        try:
            cache_attachments(site, txn)
            cached += 1
        except Exception:
            logger.exception("[%s] maintain: cache_attachments failed for %s",
                             site.site_id, txn.id)

    # --- Prune cache dirs whose owners aren't 'ready' ---
    root = _cache_root()
    if root.is_dir():
        # Collect on-disk txn ids
        on_disk: list[int] = []
        for d in root.iterdir():
            if not d.is_dir():
                continue
            try:
                on_disk.append(int(d.name))
            except ValueError:
                continue
        if on_disk:
            # Find which ones are still 'ready' on this site
            ready_rows = (
                session.query(Transcription.id)
                .filter(
                    Transcription.id.in_(on_disk),
                    Transcription.worklist_status == "ready",
                )
                .all()
            )
            ready_set = {r[0] for r in ready_rows}
            for txn_id in on_disk:
                if txn_id not in ready_set:
                    if drop_cache(txn_id):
                        pruned += 1

    if cached or pruned:
        logger.info("[%s] attachment cache: pre-cached %d ready, pruned %d done",
                    site.site_id, cached, pruned)
    return {"cached": cached, "pruned": pruned}
