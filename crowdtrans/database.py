import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from crowdtrans.config import settings
from crowdtrans.config_store import get_config_store
from crowdtrans.models import Base, Watermark

logger = logging.getLogger(__name__)

engine = create_engine(
    settings.sqlite_url,
    connect_args={"check_same_thread": False},
    echo=False,
    pool_size=5,
    pool_recycle=300,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.execute("PRAGMA mmap_size=0")
    cursor.close()


def _migrate_add_columns(engine_):
    """Add new columns to existing tables if they don't exist yet.

    SQLite doesn't support ALTER TABLE ADD COLUMN IF NOT EXISTS,
    so we check column existence first via PRAGMA table_info.
    """
    import sqlalchemy

    new_columns = {
        "transcriptions": [
            ("llm_formatted_text", "TEXT"),
            ("formatting_method", "TEXT DEFAULT 'regex'"),
            ("llm_model_used", "TEXT"),
            ("llm_format_duration_ms", "INTEGER"),
            ("llm_input_tokens", "INTEGER"),
            ("llm_output_tokens", "INTEGER"),
            ("patient_conditions", "TEXT"),
            ("worksheet_notes", "TEXT"),
            ("order_notes", "TEXT"),
            ("doctor_qualifications", "TEXT"),
            ("doctor_user_key", "BIGINT"),
            ("verified_at", "DATETIME"),
            ("verified_by", "TEXT"),
            ("final_text", "TEXT"),
        ],
    }

    with engine_.connect() as conn:
        for table, columns in new_columns.items():
            result = conn.execute(sqlalchemy.text(f"PRAGMA table_info({table})"))
            existing = {row[1] for row in result.fetchall()}
            for col_name, col_type in columns:
                if col_name not in existing:
                    conn.execute(sqlalchemy.text(
                        f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                    ))
                    logger.info("Added column %s.%s", table, col_name)
            conn.commit()


def init_db():
    settings.sqlite_db_path.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)

    # Migrate: add new columns to existing tables
    _migrate_add_columns(engine)

    # Seed site_configs + global_settings from .env on first run
    store = get_config_store()
    store.seed_from_env()

    # Ensure a watermark row exists for each enabled site
    with SessionLocal() as session:
        for site in store.get_site_configs():
            wm = session.query(Watermark).filter_by(site_id=site.site_id).first()
            if wm is None:
                session.add(Watermark(site_id=site.site_id, last_dictation_id=0))
                logger.info("Initialized watermark for site '%s' at 0", site.site_id)
        session.commit()

    logger.info("Database initialized at %s", settings.sqlite_db_path)


@contextmanager
def get_db():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
