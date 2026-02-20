import asyncio
import logging

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import Base, engine

logger = logging.getLogger(__name__)

_SCHEMA_INIT_LOCK = asyncio.Lock()
_SCHEMA_READY = False


def _get_existing_tables(connection) -> set[tuple[str | None, str]]:
    inspector = inspect(connection)
    tables: set[tuple[str | None, str]] = set()

    schemas = {table.schema for table in Base.metadata.sorted_tables}
    for schema in schemas:
        schema_name = schema or None
        for table_name in inspector.get_table_names(schema=schema_name):
            tables.add((schema_name, table_name))

    return tables


def _create_missing_tables_sync(connection) -> None:
    """Create ORM schema idempotently."""
    Base.metadata.create_all(bind=connection, checkfirst=True)


async def ensure_base_schema_ready(force: bool = False) -> bool:
    """Ensure all ORM tables are initialized and visible."""
    global _SCHEMA_READY
    if _SCHEMA_READY and not force:
        return True

    async with _SCHEMA_INIT_LOCK:
        if _SCHEMA_READY and not force:
            return True

        try:
            async with engine.begin() as conn:
                await conn.run_sync(_create_missing_tables_sync)

            async with engine.connect() as conn:
                existing = await conn.run_sync(_get_existing_tables)

            required = {(table.schema or None, table.name) for table in Base.metadata.sorted_tables}
            missing = required - existing
            if missing:
                logger.warning(
                    "schema bootstrap incomplete, missing tables: %s",
                    ",".join(sorted(f"{schema or 'public'}.{name}" for schema, name in missing)),
                )
                _SCHEMA_READY = False
                return False

            _SCHEMA_READY = True
            return True
        except SQLAlchemyError:
            logger.exception("schema bootstrap failed")
            _SCHEMA_READY = False
            return False
        except Exception:
            logger.exception("schema bootstrap failed")
            _SCHEMA_READY = False
            return False


def reset_schema_bootstrap_state() -> None:
    global _SCHEMA_READY
    _SCHEMA_READY = False
