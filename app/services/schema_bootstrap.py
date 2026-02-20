import asyncio
import logging

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import Base, engine

logger = logging.getLogger(__name__)

_SCHEMA_INIT_LOCK = asyncio.Lock()
_SCHEMA_READY = False


def _normalize_schema_name(schema_name: str | None) -> str | None:
    if not schema_name:
        return None
    return schema_name


def _table_exists(inspector, table) -> bool:
    schema = _normalize_schema_name(table.schema)
    tables = set(inspector.get_table_names(schema=schema))
    return table.name in tables


def _create_missing_tables_sync(connection) -> None:
    metadata = Base.metadata
    inspector = inspect(connection)

    for table in metadata.sorted_tables:
        if _table_exists(inspector, table):
            continue
        table.create(bind=connection, checkfirst=False)


async def ensure_base_schema_ready(force: bool = False) -> bool:
    """Ensure all ORM tables are initialized.

    Returns True when schema creation succeeded or already available.
    """
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return True

    async with _SCHEMA_INIT_LOCK:
        if _SCHEMA_READY and not force:
            return True

        try:
            async with engine.begin() as conn:
                await conn.run_sync(_create_missing_tables_sync)

            # Validate that core tables are now visible for current metadata.
            async with engine.connect() as conn:
                existing = await conn.run_sync(
                    lambda c: set(inspect(c).get_table_names())
                )
            required = {t.name for t in Base.metadata.sorted_tables}
            if not required.issubset(existing):
                missing = sorted(required.difference(existing))
                logger.warning("schema bootstrap incomplete, missing tables: %s", ",".join(missing))
                _SCHEMA_READY = False
                return False

            _SCHEMA_READY = True
            return True
        except SQLAlchemyError:
            logger.exception("schema bootstrap failed")
            return False
        except Exception:
            logger.exception("schema bootstrap failed")
            return False


def reset_schema_bootstrap_state() -> None:
    global _SCHEMA_READY
    _SCHEMA_READY = False
