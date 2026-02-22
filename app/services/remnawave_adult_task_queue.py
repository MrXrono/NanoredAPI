from __future__ import annotations

import asyncio
import logging
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.database import async_session
from app.core.redis import get_redis
from app.models.remnawave_log import AdultTaskRun
from app.services.remnawave_adult import sync_adult_catalog, sync_adult_catalog_from_txt
from app.services.runtime_control import services_enabled, services_killed

logger = logging.getLogger(__name__)


def _decode(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "ignore")
    return str(raw or "")


def _normalize_msg_id(raw: Any) -> str:
    return _decode(raw).strip()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def _create_task_run(*, task_key: str, label: str, message_id: str, worker_name: str) -> uuid.UUID:
    run_id = uuid.uuid4()
    started = _now_utc()
    payload = {
        "message_id": message_id,
        "worker": worker_name,
    }
    row = AdultTaskRun(
        id=run_id,
        task_key=task_key,
        label=label,
        status="running",
        running=True,
        phase="start",
        message=f"Worker {worker_name} started message_id={message_id}",
        progress_current=0,
        progress_total=0,
        progress_percent=0.0,
        result_json=payload,
        started_at=started,
        finished_at=None,
    )
    async with async_session() as db:
        db.add(row)
        await db.commit()
    return run_id


async def _update_task_run(
    run_id: uuid.UUID,
    *,
    status: str | None = None,
    running: bool | None = None,
    phase: str | None = None,
    message: str | None = None,
    progress_current: int | None = None,
    progress_total: int | None = None,
    progress_percent: float | None = None,
    result_json: dict[str, Any] | None = None,
    error_short: str | None = None,
    error_full: str | None = None,
    finished: bool = False,
) -> None:
    async with async_session() as db:
        row = await db.get(AdultTaskRun, run_id)
        if row is None:
            return
        if status is not None:
            row.status = status
        if running is not None:
            row.running = running
        if phase is not None:
            row.phase = phase
        if message is not None:
            row.message = message[:16000]
        if progress_current is not None:
            row.progress_current = int(progress_current)
        if progress_total is not None:
            row.progress_total = int(progress_total)
        if progress_percent is not None:
            row.progress_percent = float(progress_percent)
        if result_json is not None:
            row.result_json = result_json
        if error_short is not None:
            row.error_short = error_short[:512]
        if error_full is not None:
            row.error_full = error_full[:200000]
        if finished:
            row.finished_at = _now_utc()
        await db.commit()


async def enqueue_manual_adult_sync() -> str:
    redis = await get_redis()
    msg_id = await redis.xadd(
        settings.ADULT_MANUAL_SYNC_STREAM,
        {
            "task": "sync",
            "requested_at": datetime.now(timezone.utc).isoformat(),
        },
        maxlen=settings.ADULT_MANUAL_TASK_STREAM_MAXLEN,
        approximate=True,
    )
    return str(msg_id)


async def enqueue_manual_adult_txt_sync(path: str | None = None) -> str:
    redis = await get_redis()
    fields = {
        "task": "txt_sync",
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    if path:
        fields["path"] = path
    msg_id = await redis.xadd(
        settings.ADULT_MANUAL_TXT_STREAM,
        fields,
        maxlen=settings.ADULT_MANUAL_TASK_STREAM_MAXLEN,
        approximate=True,
    )
    return str(msg_id)


async def _ensure_group(stream: str, group: str) -> None:
    redis = await get_redis()
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _claim_stale_messages(
    *,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    count: int,
) -> list[tuple[str, dict[str, Any]]]:
    redis = await get_redis()
    try:
        claimed_raw = await redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
    except Exception:
        return []

    messages: list[tuple[str, dict[str, Any]]] = []
    if isinstance(claimed_raw, (list, tuple)) and len(claimed_raw) >= 2:
        raw_messages = claimed_raw[1] or []
        for msg_id, fields in raw_messages:
            normalized_id = _normalize_msg_id(msg_id)
            if not normalized_id:
                continue
            messages.append((normalized_id, fields or {}))
    return messages


async def _process_sync_message(msg_id: str, fields: dict[str, Any]) -> None:
    redis = await get_redis()
    worker_name = settings.ADULT_MANUAL_SYNC_CONSUMER
    run_id = await _create_task_run(task_key="sync", label="sync", message_id=msg_id, worker_name=worker_name)

    last_progress_flush = 0.0

    async def _progress(payload: dict[str, Any]) -> None:
        nonlocal last_progress_flush
        now_ts = time.monotonic()
        if now_ts - last_progress_flush < 0.8:
            return
        last_progress_flush = now_ts
        pc = int(payload.get("progress_current", 0) or 0)
        pt = int(payload.get("progress_total", 0) or 0)
        pp = float(payload.get("progress_percent", 0.0) or 0.0)
        msg = str(payload.get("message") or f"Processed {pc}/{pt}")
        await _update_task_run(
            run_id,
            status=str(payload.get("status") or "running"),
            running=True,
            phase=str(payload.get("phase") or "sync"),
            message=msg,
            progress_current=pc,
            progress_total=pt,
            progress_percent=pp,
        )

    try:
        logger.info("Manual adult sync task started, message_id=%s", msg_id)
        result = await sync_adult_catalog(progress_cb=_progress)
        processed = int(result.get("updated", 0) or 0)
        total = max(processed, int(result.get("total", processed) or processed), 1)
        await _update_task_run(
            run_id,
            status="ok",
            running=False,
            phase="done",
            message=f"Processed {processed}/{total}",
            progress_current=processed,
            progress_total=total,
            progress_percent=100.0,
            result_json={"message_id": msg_id, "worker": worker_name, "result": result},
            finished=True,
        )
        logger.info("Manual adult sync task completed, message_id=%s result=%s", msg_id, result)
    except asyncio.CancelledError as exc:
        await _update_task_run(
            run_id,
            status="cancelled",
            running=False,
            phase="cancelled",
            message="Cancelled",
            error_short=exc.__class__.__name__,
            finished=True,
        )
        logger.warning("Manual adult sync task cancelled, message_id=%s", msg_id)
        raise
    except Exception as exc:
        err_text = str(exc).strip() or exc.__class__.__name__
        tb = traceback.format_exc(limit=20)
        await _update_task_run(
            run_id,
            status="failed",
            running=False,
            phase="error",
            message=f"Failed: {err_text}",
            error_short=f"{exc.__class__.__name__}: {err_text}",
            error_full=tb,
            finished=True,
        )
        logger.exception("Manual adult sync task failed, message_id=%s: %s", msg_id, exc)
    finally:
        await redis.xack(settings.ADULT_MANUAL_SYNC_STREAM, settings.ADULT_MANUAL_SYNC_GROUP, msg_id)
        await redis.xdel(settings.ADULT_MANUAL_SYNC_STREAM, msg_id)


async def _process_txt_message(msg_id: str, fields: dict[str, Any]) -> None:
    redis = await get_redis()
    path = _decode(fields.get("path")).strip() or None
    worker_name = settings.ADULT_MANUAL_TXT_CONSUMER
    run_id = await _create_task_run(task_key="txt_sync", label="txt_sync", message_id=msg_id, worker_name=worker_name)

    last_progress_flush = 0.0

    async def _progress(payload: dict[str, Any]) -> None:
        nonlocal last_progress_flush
        now_ts = time.monotonic()
        if now_ts - last_progress_flush < 0.8:
            return
        last_progress_flush = now_ts
        pc = int(payload.get("progress_current", 0) or 0)
        pt = int(payload.get("progress_total", 0) or 0)
        pp = float(payload.get("progress_percent", 0.0) or 0.0)
        msg = str(payload.get("message") or f"Processed {pc}/{pt}")
        await _update_task_run(
            run_id,
            status=str(payload.get("status") or "running"),
            running=True,
            phase=str(payload.get("phase") or "txt_sync"),
            message=msg,
            progress_current=pc,
            progress_total=pt,
            progress_percent=pp,
        )

    try:
        logger.info("Manual adult txt->db task started, message_id=%s path=%s", msg_id, path)
        result = await sync_adult_catalog_from_txt(path=path, progress_cb=_progress)
        processed = int(result.get("processed", 0) or 0)
        total = max(processed, int(result.get("total", processed) or processed), 1)
        await _update_task_run(
            run_id,
            status="ok",
            running=False,
            phase="done",
            message=f"Processed {processed}/{total}",
            progress_current=processed,
            progress_total=total,
            progress_percent=100.0,
            result_json={"message_id": msg_id, "worker": worker_name, "path": path, "result": result},
            finished=True,
        )
        logger.info("Manual adult txt->db task completed, message_id=%s result=%s", msg_id, result)
    except asyncio.CancelledError as exc:
        await _update_task_run(
            run_id,
            status="cancelled",
            running=False,
            phase="cancelled",
            message="Cancelled",
            error_short=exc.__class__.__name__,
            finished=True,
        )
        logger.warning("Manual adult txt->db task cancelled, message_id=%s", msg_id)
        raise
    except Exception as exc:
        err_text = str(exc).strip() or exc.__class__.__name__
        tb = traceback.format_exc(limit=20)
        await _update_task_run(
            run_id,
            status="failed",
            running=False,
            phase="error",
            message=f"Failed: {err_text}",
            error_short=f"{exc.__class__.__name__}: {err_text}",
            error_full=tb,
            finished=True,
        )
        logger.exception("Manual adult txt->db task failed, message_id=%s: %s", msg_id, exc)
    finally:
        await redis.xack(settings.ADULT_MANUAL_TXT_STREAM, settings.ADULT_MANUAL_TXT_GROUP, msg_id)
        await redis.xdel(settings.ADULT_MANUAL_TXT_STREAM, msg_id)


async def background_manual_adult_sync_worker(stop_event: asyncio.Event) -> None:
    await _ensure_group(settings.ADULT_MANUAL_SYNC_STREAM, settings.ADULT_MANUAL_SYNC_GROUP)
    redis = await get_redis()

    while not stop_event.is_set():
        if services_killed():
            break
        if not services_enabled():
            await asyncio.sleep(1.0)
            continue
        try:
            reclaimed = await _claim_stale_messages(
                stream=settings.ADULT_MANUAL_SYNC_STREAM,
                group=settings.ADULT_MANUAL_SYNC_GROUP,
                consumer=settings.ADULT_MANUAL_SYNC_CONSUMER,
                min_idle_ms=settings.ADULT_MANUAL_TASK_RECLAIM_IDLE_MS,
                count=settings.ADULT_MANUAL_TASK_RECLAIM_COUNT,
            )
            if reclaimed:
                for msg_id, fields in reclaimed:
                    await _process_sync_message(msg_id, fields)
                continue

            data = await redis.xreadgroup(
                groupname=settings.ADULT_MANUAL_SYNC_GROUP,
                consumername=settings.ADULT_MANUAL_SYNC_CONSUMER,
                streams={settings.ADULT_MANUAL_SYNC_STREAM: ">"},
                count=settings.ADULT_MANUAL_TASK_READ_COUNT,
                block=1000,
            )
            if not data:
                continue
            for _, messages in data:
                for msg_id, fields in messages:
                    await _process_sync_message(_normalize_msg_id(msg_id), fields or {})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("manual adult sync queue worker error: %s", exc)
            await asyncio.sleep(1.0)


async def background_manual_adult_txt_worker(stop_event: asyncio.Event) -> None:
    await _ensure_group(settings.ADULT_MANUAL_TXT_STREAM, settings.ADULT_MANUAL_TXT_GROUP)
    redis = await get_redis()

    while not stop_event.is_set():
        if services_killed():
            break
        if not services_enabled():
            await asyncio.sleep(1.0)
            continue
        try:
            reclaimed = await _claim_stale_messages(
                stream=settings.ADULT_MANUAL_TXT_STREAM,
                group=settings.ADULT_MANUAL_TXT_GROUP,
                consumer=settings.ADULT_MANUAL_TXT_CONSUMER,
                min_idle_ms=settings.ADULT_MANUAL_TASK_RECLAIM_IDLE_MS,
                count=settings.ADULT_MANUAL_TASK_RECLAIM_COUNT,
            )
            if reclaimed:
                for msg_id, fields in reclaimed:
                    await _process_txt_message(msg_id, fields)
                continue

            data = await redis.xreadgroup(
                groupname=settings.ADULT_MANUAL_TXT_GROUP,
                consumername=settings.ADULT_MANUAL_TXT_CONSUMER,
                streams={settings.ADULT_MANUAL_TXT_STREAM: ">"},
                count=settings.ADULT_MANUAL_TASK_READ_COUNT,
                block=1000,
            )
            if not data:
                continue
            for _, messages in data:
                for msg_id, fields in messages:
                    await _process_txt_message(_normalize_msg_id(msg_id), fields or {})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("manual adult txt queue worker error: %s", exc)
            await asyncio.sleep(1.0)
