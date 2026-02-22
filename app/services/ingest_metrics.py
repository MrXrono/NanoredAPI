from __future__ import annotations

from threading import Lock
from typing import Any

import redis

from app.core.config import settings

RSYSLOG_PATH = "/api/v1/client/remnawave/ingest"
NANOREDVPN_PATHS = {
    "/api/v1/client/sni/batch",
    "/api/v1/client/sni/raw",
    "/api/v1/client/dns/batch",
    "/api/v1/client/connections/batch",
}

RSYSLOG_SHARED_METRICS_KEY = "metrics:ingest:rsyslog"
_RS_REJECT_PREFIX = "reject_reason:"

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

_sync_redis: redis.Redis | None = None
_sync_redis_unavailable = False


def _category_for_path(path: str) -> str | None:
    if path == RSYSLOG_PATH:
        return "rsyslog"
    if path in NANOREDVPN_PATHS:
        return "nanoredvpn"
    return None


def _get_sync_redis() -> redis.Redis | None:
    global _sync_redis, _sync_redis_unavailable
    if _sync_redis_unavailable:
        return None
    if _sync_redis is None:
        try:
            _sync_redis = redis.Redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.2,
            )
        except Exception:
            _sync_redis_unavailable = True
            _sync_redis = None
            return None
    return _sync_redis


def _shared_incr(field: str, value: int) -> None:
    inc = max(0, int(value or 0))
    if inc <= 0:
        return
    client = _get_sync_redis()
    if client is None:
        return
    try:
        client.hincrby(RSYSLOG_SHARED_METRICS_KEY, field, inc)
    except Exception:
        return


def _shared_incr_reject_reasons(reject_reasons: dict[str, int] | None) -> None:
    if not reject_reasons:
        return
    client = _get_sync_redis()
    if client is None:
        return
    try:
        for key, value in reject_reasons.items():
            reason = str(key or "unknown")
            inc = max(0, int(value or 0))
            if inc > 0:
                client.hincrby(RSYSLOG_SHARED_METRICS_KEY, f"{_RS_REJECT_PREFIX}{reason}", inc)
    except Exception:
        return


def _shared_snapshot() -> dict[str, Any] | None:
    client = _get_sync_redis()
    if client is None:
        return None
    try:
        raw = client.hgetall(RSYSLOG_SHARED_METRICS_KEY) or {}
    except Exception:
        return None
    if not raw:
        return None

    out: dict[str, Any] = {
        "received": 0,
        "validated_ok": 0,
        "processed_ok": 0,
        "inserted_new": 0,
        "deduplicated": 0,
        "rejected": 0,
        "failed": 0,
        "retried": 0,
        "reject_reasons": [],
    }
    reasons: list[tuple[str, int]] = []
    for key, value in raw.items():
        if key.startswith(_RS_REJECT_PREFIX):
            reason = key[len(_RS_REJECT_PREFIX) :] or "unknown"
            try:
                count = int(value or 0)
            except Exception:
                count = 0
            if count > 0:
                reasons.append((reason, count))
            continue
        if key in out:
            try:
                out[key] = int(value or 0)
            except Exception:
                out[key] = 0

    reasons.sort(key=lambda x: x[1], reverse=True)
    out["reject_reasons"] = [{"reason": k, "count": v} for k, v in reasons[:8]]
    return out


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
    _shared_incr("received", received)


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

    _shared_incr("validated_ok", validated_ok)
    _shared_incr("processed_ok", processed_ok)
    _shared_incr("inserted_new", inserted_new)
    _shared_incr("deduplicated", deduplicated)
    _shared_incr("rejected", rejected)
    _shared_incr_reject_reasons(reject_reasons)


def record_rsyslog_failed(failed: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["failed"] = int(bucket.get("failed", 0)) + max(0, int(failed))
    _shared_incr("failed", failed)


def record_rsyslog_retried(count: int) -> None:
    with _lock:
        bucket = _state["rsyslog"]
        bucket["retried"] = int(bucket.get("retried", 0)) + max(0, int(count))
    _shared_incr("retried", count)


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


def _merge_shared_rsyslog(snapshot: dict[str, Any]) -> dict[str, Any]:
    shared = _shared_snapshot()
    if not shared:
        return snapshot

    merged = dict(snapshot)
    merged["received"] = int(shared.get("received", snapshot.get("received", 0)) or 0)
    merged["validated_ok"] = int(shared.get("validated_ok", snapshot.get("validated_ok", 0)) or 0)
    merged["processed_ok"] = int(shared.get("processed_ok", snapshot.get("processed_ok", 0)) or 0)
    merged["inserted_new"] = int(shared.get("inserted_new", snapshot.get("inserted_new", 0)) or 0)
    merged["deduplicated"] = int(shared.get("deduplicated", snapshot.get("deduplicated", 0)) or 0)
    merged["rejected"] = int(shared.get("rejected", snapshot.get("rejected", 0)) or 0)
    merged["failed"] = int(shared.get("failed", snapshot.get("failed", 0)) or 0)
    merged["retried"] = int(shared.get("retried", snapshot.get("retried", 0)) or 0)
    merged["processed"] = int(merged.get("processed_ok", 0) or 0)
    shared_reasons = shared.get("reject_reasons")
    if isinstance(shared_reasons, list):
        merged["reject_reasons"] = shared_reasons
    return merged


def get_ingest_metrics_snapshot() -> dict[str, Any]:
    with _lock:
        rsyslog_local = _snapshot_bucket(_state["rsyslog"], rsyslog=True)
        nanored_local = _snapshot_bucket(_state["nanoredvpn"], rsyslog=False)

    return {
        "rsyslog": _merge_shared_rsyslog(rsyslog_local),
        "nanoredvpn": nanored_local,
    }
