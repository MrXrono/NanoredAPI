from __future__ import annotations

import asyncio
from datetime import datetime, timezone

_RUNTIME_LOCK = asyncio.Lock()
_RUNTIME_STATE: dict[str, object] = {
    "services_enabled": True,
    "updated_at": datetime.now(timezone.utc).isoformat(),
    "reason": "startup",
}


def get_services_state() -> dict[str, object]:
    return dict(_RUNTIME_STATE)


def services_enabled() -> bool:
    return bool(_RUNTIME_STATE.get("services_enabled", True))


async def set_services_enabled(enabled: bool, reason: str = "manual") -> dict[str, object]:
    async with _RUNTIME_LOCK:
        _RUNTIME_STATE["services_enabled"] = bool(enabled)
        _RUNTIME_STATE["updated_at"] = datetime.now(timezone.utc).isoformat()
        _RUNTIME_STATE["reason"] = str(reason or "manual")
        return dict(_RUNTIME_STATE)
