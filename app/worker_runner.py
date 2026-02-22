from __future__ import annotations

import asyncio
import logging
import signal

from sqlalchemy import text

from app.core.config import settings
from app.core.database import engine
from app.core.logging_setup import setup_logging, setup_function_call_logging
from app.services.db_maintenance import background_db_maintenance, setup_postgres_performance_objects
from app.services.remnawave_adult import background_remnawave_adult_tasks
from app.services.remnawave_adult_task_queue import (
    background_manual_adult_sync_worker,
    background_manual_adult_txt_worker,
)
from app.services.remnawave_ingest_queue import background_remnawave_ingest_worker
from app.services.schema_bootstrap import ensure_base_schema_ready

setup_logging()
setup_function_call_logging()
logger = logging.getLogger(__name__)
SCHEMA_SETUP_LOCK_ID = 1


async def _bootstrap_schema() -> None:
    async with engine.connect() as conn:
        await conn.execute(text(f"SELECT pg_advisory_lock({SCHEMA_SETUP_LOCK_ID})"))
        try:
            created = await ensure_base_schema_ready()
            if not created:
                raise RuntimeError("Schema bootstrap failed")
            await setup_postgres_performance_objects()
        finally:
            await conn.execute(text(f"SELECT pg_advisory_unlock({SCHEMA_SETUP_LOCK_ID})"))


async def _run_worker(role: str, stop_event: asyncio.Event) -> None:
    if role == "adult_sync":
        logger.info("Starting worker role=adult_sync")
        await asyncio.gather(
            background_remnawave_adult_tasks(stop_event),
            background_manual_adult_sync_worker(stop_event),
        )
        return
    if role == "txt_db":
        logger.info("Starting worker role=txt_db")
        await asyncio.gather(
            background_remnawave_ingest_worker(stop_event),
            background_manual_adult_txt_worker(stop_event),
        )
        return
    if role == "db_maintenance":
        logger.info("Starting worker role=db_maintenance")
        await background_db_maintenance(stop_event)
        return
    raise RuntimeError(f"Unknown WORKER_ROLE={role}")


async def _amain() -> None:
    role = (settings.WORKER_ROLE or "").strip().lower()
    if not role:
        raise RuntimeError("WORKER_ROLE is required for worker container")

    await _bootstrap_schema()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    task = asyncio.create_task(_run_worker(role, stop_event))
    try:
        await task
    except asyncio.CancelledError:
        raise
    finally:
        stop_event.set()
        task.cancel()
        await engine.dispose()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
