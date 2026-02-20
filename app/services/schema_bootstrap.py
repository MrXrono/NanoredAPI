import asyncio
import logging

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import Base, engine

logger = logging.getLogger(__name__)

_SCHEMA_INIT_LOCK = asyncio.Lock()
_SCHEMA_READY = False


def _table_ensure_error_is_ignorable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "already exists" in msg
        or "duplicate" in msg
        or "relation already exists" in msg
        or "constraint" in msg and "already exists" in msg
    )


def _create_tables_sync(connection) -> None:
    metadata = Base.metadata
    for table in metadata.sorted_tables:
        try:
            table.create(bind=connection, checkfirst=True)
        except Exception as exc:
            if _table_ensure_error_is_ignorable(exc):
                logger.warning("Schema bootstrap: table/index already exists while creating %s: %s", table.name, exc)
                try:
                    connection.rollback()
                except Exception:
                    pass
                continue
            raise


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
                await conn.run_sync(_create_tables_sync)
            # Validate that core tables are now visible for current metadata.
            async with engine.connect() as conn:
                existing = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
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
