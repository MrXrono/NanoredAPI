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
_state: dict[str, dict[str, Any]] = {
    "rsyslog": {
        "received": 0,
        "validated_ok": 0,
        "processed_ok": 0,
        "inserted_new": 0,
        "deduplicated": 0,
        "rejected": 0,
        "failed": 0,
        "retried": 0,
        "requests_total": 0,
        "requests_ok": 0,
        "latency_total_ms": 0.0,
        "latency_samples": 0,
        "reject_reasons": {},
    },
    "nanoredvpn": {
        "received": 0,
        "processed": 0,
        "failed": 0,
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


def record_rsyslog_enqueue(received: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["received"] = int(bucket["received"]) + max(0, int(received))


def record_rsyslog_result(
    *,
    validated_ok: int,
    processed_ok: int,
    inserted_new: int,
    deduplicated: int,
    rejected: int,
    reject_reasons: dict[str, int] | None = None,
) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["validated_ok"] = int(bucket["validated_ok"]) + max(0, int(validated_ok))
        bucket["processed_ok"] = int(bucket["processed_ok"]) + max(0, int(processed_ok))
        bucket["inserted_new"] = int(bucket["inserted_new"]) + max(0, int(inserted_new))
        bucket["deduplicated"] = int(bucket["deduplicated"]) + max(0, int(deduplicated))
        bucket["rejected"] = int(bucket["rejected"]) + max(0, int(rejected))
        reasons = bucket.setdefault("reject_reasons", {})
        if isinstance(reasons, dict) and reject_reasons:
            for key, value in reject_reasons.items():
                k = str(key or "unknown")
                reasons[k] = int(reasons.get(k, 0)) + max(0, int(value or 0))


def record_rsyslog_failed(failed: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["failed"] = int(bucket.get("failed", 0)) + max(0, int(failed))


def record_rsyslog_retried(count: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["retried"] = int(bucket.get("retried", 0)) + max(0, int(count))


def record_rsyslog_processed(processed: int) -> None:
    # Backward-compatible helper.
    record_rsyslog_result(
        validated_ok=processed,
        processed_ok=processed,
        inserted_new=0,
        deduplicated=0,
        rejected=0,
        reject_reasons=None,
    )


def record_rsyslog_ingest(received: int, processed: int) -> None:
    # Backward-compatible helper for old code paths.
    record_rsyslog_enqueue(received)
    record_rsyslog_processed(processed)


def record_nanoredvpn_ingest(received: int, processed: int, failed: int = 0) -> None:
    with _lock:
        bucket = _state["nanoredvpn"]
        bucket["received"] = int(bucket["received"]) + max(0, int(received))
        bucket["processed"] = int(bucket["processed"]) + max(0, int(processed))
        bucket["failed"] = int(bucket.get("failed", 0)) + max(0, int(failed))


def _snapshot_bucket(bucket: dict[str, Any], *, rsyslog: bool = False) -> dict[str, Any]:
    samples = int(bucket.get("latency_samples", 0) or 0)
    total_ms = float(bucket.get("latency_total_ms", 0.0) or 0.0)
    base = {
        "received": int(bucket.get("received", 0) or 0),
        "failed": int(bucket.get("failed", 0) or 0),
        "requests_total": int(bucket.get("requests_total", 0) or 0),
        "requests_ok": int(bucket.get("requests_ok", 0) or 0),
        "avg_latency_ms": round((total_ms / samples), 2) if samples > 0 else 0.0,
    }
    if rsyslog:
        reasons = bucket.get("reject_reasons") or {}
        top_reasons = sorted(
            ((str(k), int(v)) for k, v in reasons.items()),
            key=lambda x: x[1],
            reverse=True,
        )[:8]
        base.update(
            {
                "validated_ok": int(bucket.get("validated_ok", 0) or 0),
                "processed_ok": int(bucket.get("processed_ok", 0) or 0),
                "inserted_new": int(bucket.get("inserted_new", 0) or 0),
                "deduplicated": int(bucket.get("deduplicated", 0) or 0),
                "rejected": int(bucket.get("rejected", 0) or 0),
                "retried": int(bucket.get("retried", 0) or 0),
                "reject_reasons": [{"reason": k, "count": v} for k, v in top_reasons],
                "processed": int(bucket.get("processed_ok", 0) or 0),  # backward compatibility for UI
            }
        )
    else:
        base["processed"] = int(bucket.get("processed", 0) or 0)
    return base


def get_ingest_metrics_snapshot() -> dict[str, Any]:
    with _lock:
        return {
            "rsyslog": _snapshot_bucket(_state["rsyslog"], rsyslog=True),
            "nanoredvpn": _snapshot_bucket(_state["nanoredvpn"], rsyslog=False),
        }
