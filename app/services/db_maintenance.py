import asyncio
import logging

from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine
from app.services.runtime_control import services_enabled, services_killed

logger = logging.getLogger(__name__)

_ANALYZE_TABLES = (
    "remnawave_accounts",
    "remnawave_dns_queries",
    "remnawave_dns_unique",
    "remnawave_nodes",
)


async def setup_postgres_performance_objects() -> None:
    if not settings.DB_MAINTENANCE_INDEX_SETUP_ON_STARTUP:
        return
    if not settings.DATABASE_URL.startswith("postgresql"):
        return

    statements = (
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
        "CREATE INDEX IF NOT EXISTS ix_rnw_queries_account_requested_id ON remnawave_dns_queries (account_login, requested_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_rnw_queries_dns_requested_id ON remnawave_dns_queries (dns, requested_at DESC, id DESC)",
        "CREATE INDEX IF NOT EXISTS ix_rnw_queries_requested_at_brin ON remnawave_dns_queries USING BRIN (requested_at)",
        "CREATE INDEX IF NOT EXISTS ix_rnw_accounts_login_trgm ON remnawave_accounts USING GIN (account_login gin_trgm_ops)",
        "CREATE INDEX IF NOT EXISTS ix_rnw_queries_dns_trgm ON remnawave_dns_queries USING GIN (dns gin_trgm_ops)",
    )

    for stmt in statements:
        try:
            async with engine.begin() as conn:
                await conn.execute(text(stmt))
        except Exception as exc:
            logger.warning("db maintenance setup skipped for statement '%s': %s", stmt, exc)


async def analyze_hot_tables_once() -> None:
    if not settings.DATABASE_URL.startswith("postgresql"):
        return
    for table_name in _ANALYZE_TABLES:
        try:
            async with engine.connect() as conn:
                await conn.execute(text(f"ANALYZE {table_name}"))
                await conn.commit()
        except Exception as exc:
            logger.warning("db maintenance analyze failed for %s: %s", table_name, exc)


async def background_db_maintenance(stop_event: asyncio.Event) -> None:
    if not settings.DB_MAINTENANCE_ENABLED:
        return
    interval = max(300, int(settings.DB_MAINTENANCE_ANALYZE_INTERVAL_SECONDS))

    while not stop_event.is_set():
        try:
            for _ in range(interval):
                if stop_event.is_set() or services_killed():
                    return
                await asyncio.sleep(1)
            if not services_enabled():
                continue
            await analyze_hot_tables_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("background db maintenance loop error: %s", exc)
