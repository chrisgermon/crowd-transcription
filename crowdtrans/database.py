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


def init_db():
    settings.sqlite_db_path.parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)

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
