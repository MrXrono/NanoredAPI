from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.redis import get_redis
from app.services.remnawave_adult import sync_adult_catalog, sync_adult_catalog_from_txt
from app.services.runtime_control import services_enabled, services_killed

logger = logging.getLogger(__name__)


def _decode(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "ignore")
    return str(raw or "")


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


async def _process_sync_message(msg_id: str, fields: dict[str, Any]) -> None:
    redis = await get_redis()
    try:
        logger.info("Manual adult sync task started, message_id=%s", msg_id)
        result = await sync_adult_catalog()
        logger.info("Manual adult sync task completed, message_id=%s result=%s", msg_id, result)
    except Exception as exc:
        logger.exception("Manual adult sync task failed, message_id=%s: %s", msg_id, exc)
    finally:
        await redis.xack(settings.ADULT_MANUAL_SYNC_STREAM, settings.ADULT_MANUAL_SYNC_GROUP, msg_id)
        await redis.xdel(settings.ADULT_MANUAL_SYNC_STREAM, msg_id)


async def _process_txt_message(msg_id: str, fields: dict[str, Any]) -> None:
    redis = await get_redis()
    path = _decode(fields.get("path")).strip() or None
    try:
        logger.info("Manual adult txt->db task started, message_id=%s path=%s", msg_id, path)
        result = await sync_adult_catalog_from_txt(path=path)
        logger.info("Manual adult txt->db task completed, message_id=%s result=%s", msg_id, result)
    except Exception as exc:
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
                    await _process_sync_message(str(msg_id), fields or {})
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
                    await _process_txt_message(str(msg_id), fields or {})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("manual adult txt queue worker error: %s", exc)
            await asyncio.sleep(1.0)
