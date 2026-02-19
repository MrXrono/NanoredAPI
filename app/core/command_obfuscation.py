"""Small command obfuscation utilities for admin<->client control plane."""

import base64
import json
from typing import Any

_OBFUSCATION_KEY = 0x5A
_OBFUSCATED_PREFIX = "fbx1."


def _xor_bytes(data: bytes, key: int = _OBFUSCATION_KEY) -> bytes:
    return bytes(b ^ key for b in data)


def encode_command_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    masked = _xor_bytes(raw.encode("utf-8"))
    return _OBFUSCATED_PREFIX + base64.urlsafe_b64encode(masked).decode("ascii")


def decode_command_payload(raw: str) -> dict[str, Any] | None:
    raw = (raw or "").strip()
    if not raw:
        return None

    # Backward-compatible plain JSON.
    if raw.startswith("{") and raw.endswith("}"):
        try:
            return json.loads(raw)
        except Exception:
            pass

    candidate = raw
    if candidate.startswith(_OBFUSCATED_PREFIX):
        candidate = candidate[len(_OBFUSCATED_PREFIX):]

    try:
        if len(candidate) % 4:
            candidate += "=" * (4 - (len(candidate) % 4))
        raw_bytes = base64.urlsafe_b64decode(candidate.encode("ascii"))
        decoded = _xor_bytes(raw_bytes).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        return None
