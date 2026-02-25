"""Layered config: SQLite → .env → defaults.

Usage:
    from crowdtrans.config_store import config_store

    sites = config_store.get_site_configs()
    api_key = config_store.get_global("deepgram_api_key")
"""

import logging

from crowdtrans.config import SiteConfig, settings
from crowdtrans.models import GlobalSetting, SiteConfigRow, Watermark

logger = logging.getLogger(__name__)

# Keys stored in global_settings, with their .env fallback attribute names
_GLOBAL_KEYS = {
    "ris_type": "ris_type",
    "deepgram_api_key": "deepgram_api_key",
    "deepgram_model": "deepgram_model",
    "deepgram_language": "deepgram_language",
    "poll_interval_seconds": "poll_interval_seconds",
    "batch_size": "batch_size",
}


class ConfigStore:
    def __init__(self, session_factory):
        self._session_factory = session_factory

    # ------------------------------------------------------------------
    # Global settings
    # ------------------------------------------------------------------

    def get_global(self, key: str) -> str | None:
        with self._session_factory() as session:
            row = session.query(GlobalSetting).filter_by(key=key).first()
            if row and row.value is not None:
                return row.value
        # Fall back to .env / defaults
        return str(getattr(settings, key, "")) or None

    def get_all_globals(self) -> dict[str, str]:
        result = {}
        for key in _GLOBAL_KEYS:
            result[key] = self.get_global(key) or ""
        return result

    def set_global(self, key: str, value: str):
        with self._session_factory() as session:
            row = session.query(GlobalSetting).filter_by(key=key).first()
            if row:
                row.value = value
            else:
                session.add(GlobalSetting(key=key, value=value))
            session.commit()

    def save_globals(self, data: dict[str, str]):
        with self._session_factory() as session:
            for key, value in data.items():
                if key not in _GLOBAL_KEYS:
                    continue
                row = session.query(GlobalSetting).filter_by(key=key).first()
                if row:
                    row.value = value
                else:
                    session.add(GlobalSetting(key=key, value=value))
            session.commit()

    # ------------------------------------------------------------------
    # Site configs
    # ------------------------------------------------------------------

    def get_site_configs(self) -> list[SiteConfig]:
        with self._session_factory() as session:
            rows = session.query(SiteConfigRow).all()
            if rows:
                return [self._row_to_site_config(r) for r in rows]
        # Fallback to .env
        return settings.get_site_configs()

    def get_enabled_site_configs(self) -> list[SiteConfig]:
        with self._session_factory() as session:
            rows = session.query(SiteConfigRow).filter_by(enabled=True).all()
            if rows:
                return [self._row_to_site_config(r) for r in rows]
        # Fallback to .env (which only returns enabled sites)
        return settings.get_site_configs()

    def get_site(self, site_id: str) -> SiteConfig | None:
        with self._session_factory() as session:
            row = session.query(SiteConfigRow).filter_by(site_id=site_id).first()
            if row:
                return self._row_to_site_config(row)
        return settings.get_site(site_id)

    def get_site_row(self, site_id: str) -> SiteConfigRow | None:
        with self._session_factory() as session:
            row = session.query(SiteConfigRow).filter_by(site_id=site_id).first()
            if row:
                session.expunge(row)
                return row
        return None

    def get_all_site_rows(self) -> list[SiteConfigRow]:
        with self._session_factory() as session:
            rows = session.query(SiteConfigRow).all()
            for r in rows:
                session.expunge(r)
            return rows

    def save_site(
        self,
        site_id: str,
        site_name: str,
        ris_type: str,
        enabled: bool,
        db_host: str,
        db_port: int,
        db_name: str,
        db_user: str,
        db_password: str,
        audio_source: str,
        audio_mount_path: str | None,
        poll_interval_seconds: int,
        batch_size: int,
    ):
        with self._session_factory() as session:
            row = session.query(SiteConfigRow).filter_by(site_id=site_id).first()
            if row:
                row.site_name = site_name
                row.ris_type = ris_type
                row.enabled = enabled
                row.db_host = db_host
                row.db_port = db_port
                row.db_name = db_name
                row.db_user = db_user
                row.db_password = db_password
                row.audio_source = audio_source
                row.audio_mount_path = audio_mount_path
                row.poll_interval_seconds = poll_interval_seconds
                row.batch_size = batch_size
            else:
                session.add(SiteConfigRow(
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
                    audio_mount_path=audio_mount_path,
                    poll_interval_seconds=poll_interval_seconds,
                    batch_size=batch_size,
                ))
                # Create watermark for new site
                wm = session.query(Watermark).filter_by(site_id=site_id).first()
                if not wm:
                    session.add(Watermark(site_id=site_id, last_dictation_id=0))
                    logger.info("Initialized watermark for new site '%s'", site_id)
            session.commit()

    def delete_site(self, site_id: str):
        with self._session_factory() as session:
            row = session.query(SiteConfigRow).filter_by(site_id=site_id).first()
            if row:
                session.delete(row)
                session.commit()
                logger.info("Deleted site config '%s' (transcription history preserved)", site_id)

    def toggle_site(self, site_id: str) -> bool:
        with self._session_factory() as session:
            row = session.query(SiteConfigRow).filter_by(site_id=site_id).first()
            if row:
                row.enabled = not row.enabled
                session.commit()
                return row.enabled
        return False

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed_from_env(self):
        with self._session_factory() as session:
            existing = session.query(SiteConfigRow).count()
            if existing > 0:
                return  # Already seeded

            logger.info("Seeding site_configs from .env")
            for site in settings.get_site_configs():
                session.add(SiteConfigRow(
                    site_id=site.site_id,
                    site_name=site.site_name,
                    ris_type=site.ris_type,
                    enabled=site.enabled,
                    db_host=site.db_host,
                    db_port=site.db_port,
                    db_name=site.db_name,
                    db_user=site.db_user,
                    db_password=site.db_password,
                    audio_source=site.audio_source,
                    audio_mount_path=site.audio_mount_path,
                    poll_interval_seconds=site.poll_interval_seconds,
                    batch_size=site.batch_size,
                ))

            # Seed global settings from .env
            for key, attr in _GLOBAL_KEYS.items():
                val = str(getattr(settings, attr, ""))
                if val:
                    session.add(GlobalSetting(key=key, value=val))

            session.commit()
            logger.info("Seeded %d site(s) and %d global setting(s) from .env",
                        len(settings.get_site_configs()), len(_GLOBAL_KEYS))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_site_config(row: SiteConfigRow) -> SiteConfig:
        return SiteConfig(
            site_id=row.site_id,
            site_name=row.site_name,
            ris_type=row.ris_type,
            enabled=row.enabled,
            db_host=row.db_host,
            db_port=row.db_port,
            db_name=row.db_name,
            db_user=row.db_user,
            db_password=row.db_password,
            audio_source=row.audio_source,
            audio_mount_path=row.audio_mount_path,
            poll_interval_seconds=row.poll_interval_seconds,
            batch_size=row.batch_size,
        )


# Module-level singleton — initialized lazily after database.py sets up SessionLocal
_config_store: ConfigStore | None = None


def get_config_store() -> ConfigStore:
    global _config_store
    if _config_store is None:
        from crowdtrans.database import SessionLocal
        _config_store = ConfigStore(SessionLocal)
    return _config_store
