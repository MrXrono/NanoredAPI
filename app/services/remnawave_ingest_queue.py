from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.database import async_session
from app.core.redis import get_redis
from app.core.logging_buffer import logging_buffer
from app.services.ingest_metrics import (
    record_rsyslog_enqueue,
    record_rsyslog_failed,
    record_rsyslog_result,
    record_rsyslog_retried,
)
from app.services.remnawave_ingest_processor import process_remnawave_ingest_entries

logger = logging.getLogger(__name__)


def _stream() -> str:
    return settings.REMNAWAVE_INGEST_STREAM


def _group() -> str:
    return settings.REMNAWAVE_INGEST_GROUP


def _dead_stream() -> str:
    return settings.REMNAWAVE_INGEST_DEAD_STREAM


async def enqueue_remnawave_entries(entries: list[dict[str, Any]]) -> str:
    redis = await get_redis()
    payload = json.dumps(entries, ensure_ascii=False)
    msg_id = await redis.xadd(
        _stream(),
        {
            "payload": payload,
            "attempts": "0",
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        },
        maxlen=settings.REMNAWAVE_INGEST_STREAM_MAXLEN,
        approximate=True,
    )
    record_rsyslog_enqueue(len(entries))
    return str(msg_id)


async def _ensure_group() -> None:
    redis = await get_redis()
    try:
        await redis.xgroup_create(_stream(), _group(), id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def get_remnawave_queue_stats() -> dict[str, int]:
    redis = await get_redis()
    pending = 0
    group_lag = 0
    retry_pending = 0
    try:
        pend = await redis.xpending(_stream(), _group())
        pending = int((pend or {}).get("pending", 0) or 0)
    except Exception:
        pending = 0
    try:
        groups = await redis.xinfo_groups(_stream())
        for g in groups or []:
            name = g.get("name")
            if isinstance(name, bytes):
                name = name.decode("utf-8", "ignore")
            if str(name) == _group():
                group_lag = int(g.get("lag", 0) or 0)
                break
    except Exception:
        group_lag = 0
    try:
        stream_len = int(await redis.xlen(_stream()) or 0)
    except Exception:
        stream_len = 0
    try:
        dead_len = int(await redis.xlen(_dead_stream()) or 0)
    except Exception:
        dead_len = 0
    try:
        sample = await redis.xrange(_stream(), min="-", max="+", count=200)
        for _, fields in sample or []:
            attempts_raw = fields.get("attempts") if isinstance(fields, dict) else None
            if isinstance(attempts_raw, bytes):
                attempts_raw = attempts_raw.decode("utf-8", "ignore")
            attempts = int(attempts_raw or 0)
            if attempts > 0:
                retry_pending += 1
    except Exception:
        retry_pending = 0
    lag_estimate = max(stream_len, group_lag) + pending
    return {
        "stream_len": stream_len,
        "pending": pending,
        "dead_len": dead_len,
        "group_lag": group_lag,
        "lag_estimate": lag_estimate,
        "retry_pending_sample": retry_pending,
    }


async def _handle_message(msg_id: str, fields: dict[str, Any]) -> None:
    redis = await get_redis()
    payload_raw = fields.get("payload") or "[]"
    attempts = int(fields.get("attempts") or 0)
    entries: list[dict[str, Any]] = []
    try:
        data = json.loads(payload_raw)
        if isinstance(data, list):
            entries = [x for x in data if isinstance(x, dict)]
    except Exception:
        entries = []

    if not entries:
        await redis.xack(_stream(), _group(), msg_id)
        await redis.xdel(_stream(), msg_id)
        return

    try:
        async with async_session() as db:
            result = await process_remnawave_ingest_entries(db, entries)
            await db.commit()

        deduplicated = max(0, int(result.validated_ok) - int(result.unique_inserted))
        record_rsyslog_result(
            validated_ok=result.validated_ok,
            processed_ok=result.processed,
            inserted_new=result.unique_inserted,
            deduplicated=deduplicated,
            rejected=result.rejected,
            reject_reasons=result.rejected_reasons,
        )
        logging_buffer.add(
            "processing",
            (
                "Remnawave queued ingest processed: "
                f"entries={result.received}, valid={result.validated_ok}, processed={result.processed}, "
                f"inserted_new={result.unique_inserted}, dedup={deduplicated}, rejected={result.rejected}, "
                f"accounts={result.accounts_upserted}, reject_reasons={result.rejected_reasons}"
            ),
        )
        await redis.xack(_stream(), _group(), msg_id)
        await redis.xdel(_stream(), msg_id)
    except Exception as exc:
        logger.exception("Queued remnawave ingest failed for message %s: %s", msg_id, exc)
        retry = attempts + 1
        if retry <= settings.REMNAWAVE_INGEST_MAX_RETRIES:
            await redis.xadd(
                _stream(),
                {
                    "payload": payload_raw,
                    "attempts": str(retry),
                    "enqueued_at": datetime.now(timezone.utc).isoformat(),
                },
                maxlen=settings.REMNAWAVE_INGEST_STREAM_MAXLEN,
                approximate=True,
            )
            record_rsyslog_retried(len(entries))
        else:
            await redis.xadd(
                _dead_stream(),
                {
                    "payload": payload_raw,
                    "attempts": str(retry),
                    "error": str(exc)[:500],
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                },
                maxlen=settings.REMNAWAVE_INGEST_DEAD_MAXLEN,
                approximate=True,
            )
            record_rsyslog_failed(len(entries))

        await redis.xack(_stream(), _group(), msg_id)
        await redis.xdel(_stream(), msg_id)


async def background_remnawave_ingest_worker(stop_event: asyncio.Event) -> None:
    if not settings.REMNAWAVE_INGEST_QUEUE_ENABLED:
        return
    await _ensure_group()

    redis = await get_redis()
    consumer = settings.REMNAWAVE_INGEST_CONSUMER

    while not stop_event.is_set():
        try:
            data = await redis.xreadgroup(
                groupname=_group(),
                consumername=consumer,
                streams={_stream(): ">"},
                count=settings.REMNAWAVE_INGEST_READ_COUNT,
                block=1000,
            )
            if not data:
                continue
            for _, messages in data:
                for msg_id, fields in messages:
                    await _handle_message(str(msg_id), fields)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Remnawave ingest queue worker error: %s", exc)
            await asyncio.sleep(1.0)
