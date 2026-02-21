from __future__ import annotations

from threading import Lock
from typing import Any

RSYSLOG_PATH = "/api/v1/client/remnawave/ingest"
NANOREDVPN_PATHS = {
    "/api/v1/client/sni/batch",
    "/api/v1/client/sni/raw",
    "/api/v1/client/dns/batch",
    "/api/v1/client/connections/batch",
}

_lock = Lock()
_state: dict[str, dict[str, float | int]] = {
    "rsyslog": {
        "received": 0,
        "processed": 0,
        "requests_total": 0,
        "requests_ok": 0,
        "latency_total_ms": 0.0,
        "latency_samples": 0,
    },
    "nanoredvpn": {
        "received": 0,
        "processed": 0,
        "requests_total": 0,
        "requests_ok": 0,
        "latency_total_ms": 0.0,
        "latency_samples": 0,
    },
}


def _category_for_path(path: str) -> str | None:
    if path == RSYSLOG_PATH:
        return "rsyslog"
    if path in NANOREDVPN_PATHS:
        return "nanoredvpn"
    return None


def observe_api_timing(path: str, status_code: int, duration_ms: float) -> None:
    category = _category_for_path(path)
    if not category:
        return
    with _lock:
        bucket = _state[category]
        bucket["requests_total"] = int(bucket["requests_total"]) + 1
        if 200 <= int(status_code) < 400:
            bucket["requests_ok"] = int(bucket["requests_ok"]) + 1
        bucket["latency_total_ms"] = float(bucket["latency_total_ms"]) + float(duration_ms)
        bucket["latency_samples"] = int(bucket["latency_samples"]) + 1


def record_rsyslog_ingest(received: int, processed: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["received"] = int(bucket["received"]) + max(0, int(received))
        bucket["processed"] = int(bucket["processed"]) + max(0, int(processed))


def record_nanoredvpn_ingest(received: int, processed: int) -> None:
    with _lock:
        bucket = _state["nanoredvpn"]
        bucket["received"] = int(bucket["received"]) + max(0, int(received))
        bucket["processed"] = int(bucket["processed"]) + max(0, int(processed))


def _snapshot_bucket(bucket: dict[str, float | int]) -> dict[str, Any]:
    samples = int(bucket.get("latency_samples", 0) or 0)
    total_ms = float(bucket.get("latency_total_ms", 0.0) or 0.0)
    return {
        "received": int(bucket.get("received", 0) or 0),
        "processed": int(bucket.get("processed", 0) or 0),
        "requests_total": int(bucket.get("requests_total", 0) or 0),
        "requests_ok": int(bucket.get("requests_ok", 0) or 0),
        "avg_latency_ms": round((total_ms / samples), 2) if samples > 0 else 0.0,
    }


def get_ingest_metrics_snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "rsyslog": _snapshot_bucket(_state["rsyslog"]),
            "nanoredvpn": _snapshot_bucket(_state["nanoredvpn"]),
        }
