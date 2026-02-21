import hashlib
import logging
import uuid

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import get_redis
from app.core.security import verify_api_key
from app.models.device import Device

logger = logging.getLogger(__name__)

_API_KEY_CACHE_TTL_SECONDS = 300


async def get_device_by_api_key(
    api_key: str,
    db: AsyncSession,
    *,
    require_account_id: bool = False,
) -> Device:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-API-Key")

    parts = api_key.split(":", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key format")

    device_id_str, secret = parts
    try:
        device_id = uuid.UUID(device_id_str)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

    result = await db.execute(select(Device).where(Device.id == device_id))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Device not found")

    if device.is_blocked:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Device is blocked")

    # bcrypt is CPU-heavy. Cache successful checks for the exact pair
    # (device, stored hash revision, secret) for a short TTL.
    cache_ok = False
    cache_key = hashlib.sha256(f"{device.id}:{device.api_key_hash}:{secret}".encode()).hexdigest()
    redis_key = f"auth:device_api:{cache_key}"

    try:
        redis = await get_redis()
        cache_ok = await redis.exists(redis_key) == 1
    except Exception as exc:  # pragma: no cover - best-effort cache only
        logger.debug("API key cache read skipped: %s", exc)

    if not cache_ok:
        if not verify_api_key(secret, device.api_key_hash):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
        try:
            redis = await get_redis()
            await redis.set(redis_key, "1", ex=_API_KEY_CACHE_TTL_SECONDS)
        except Exception as exc:  # pragma: no cover - best-effort cache only
            logger.debug("API key cache write skipped: %s", exc)

    if require_account_id and not device.account_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Device account_id is not set")

    return device
