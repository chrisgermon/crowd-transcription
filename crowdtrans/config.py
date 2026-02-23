from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class SiteConfig(BaseModel):
    """Configuration for a single RIS site."""
    site_id: str
    site_name: str
    ris_type: str  # "visage" or "karisma"
    enabled: bool = True

    # Database connection (varies by ris_type)
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Audio source
    audio_source: str  # "nfs" or "sql_blob"
    audio_mount_path: str | None = None  # For NFS-based audio (Visage)

    # Processing
    poll_interval_seconds: int = 30
    batch_size: int = 10


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    # --- Visage PostgreSQL (read-only) ---
    visage_db_host: str = "unitedpdcor01"
    visage_db_port: int = 5432
    visage_db_name: str = "visage_ris"
    visage_db_user: str = "crowdit"
    visage_db_password: str = "Crowdbot1@"
    visage_audio_mount_path: str = "/mnt/visage-audio"
    visage_enabled: bool = True

    # --- Karisma MSSQL (read-only) ---
    karisma_db_host: str = "10.100.50.5"
    karisma_db_port: int = 1433
    karisma_db_name: str = "karisma_rvc_live"
    karisma_db_user: str = "Crowditreader"
    karisma_db_password: str = "Crowdbot1@"
    karisma_enabled: bool = True

    # --- Deepgram ---
    deepgram_api_key: str = "413a9095e1114cb967eb77dac023ac8cbc7bc9b2"
    deepgram_model: str = "nova-3-medical"
    deepgram_language: str = "en-AU"

    # --- Local SQLite ---
    sqlite_db_path: Path = Path("/opt/crowdtrans/data/crowdtrans.db")

    # --- Transcription service defaults ---
    poll_interval_seconds: int = 30
    batch_size: int = 10

    # --- Web interface ---
    web_host: str = "0.0.0.0"
    web_port: int = 8080

    @property
    def sqlite_url(self) -> str:
        return f"sqlite:///{self.sqlite_db_path}"

    def get_site_configs(self) -> list[SiteConfig]:
        """Build site configs from flat env vars."""
        sites = []
        if self.visage_enabled:
            sites.append(SiteConfig(
                site_id="visage",
                site_name="United Radiology (Visage)",
                ris_type="visage",
                db_host=self.visage_db_host,
                db_port=self.visage_db_port,
                db_name=self.visage_db_name,
                db_user=self.visage_db_user,
                db_password=self.visage_db_password,
                audio_source="nfs",
                audio_mount_path=self.visage_audio_mount_path,
                poll_interval_seconds=self.poll_interval_seconds,
                batch_size=self.batch_size,
            ))
        if self.karisma_enabled:
            sites.append(SiteConfig(
                site_id="karisma",
                site_name="Vision Radiology (Karisma)",
                ris_type="karisma",
                db_host=self.karisma_db_host,
                db_port=self.karisma_db_port,
                db_name=self.karisma_db_name,
                db_user=self.karisma_db_user,
                db_password=self.karisma_db_password,
                audio_source="sql_blob",
                poll_interval_seconds=self.poll_interval_seconds,
                batch_size=self.batch_size,
            ))
        return sites

    def get_site(self, site_id: str) -> SiteConfig | None:
        for s in self.get_site_configs():
            if s.site_id == site_id:
                return s
        return None


settings = Settings()
