import uuid
import ipaddress
import asyncio
import socket
import time
import traceback
import psutil
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy import select, func, desc, and_, or_, delete, text, exists, update, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis
from app.core.security import get_current_admin
from app.core.logging_buffer import logging_buffer
from app.models.device import Device
from app.models.session import Session
from app.models.sni_log import SNILog
from app.models.dns_log import DNSLog
from app.models.connection_log import ConnectionLog
from app.models.error_log import ErrorLog
from app.models.account import Account
from app.models.device_permission import DevicePermission
from app.models.device_log import DeviceLog
from app.models.device_change_log import DeviceChangeLog
from app.models.remnawave_log import (
    RemnawaveAccount,
    RemnawaveDNSQuery,
    RemnawaveDNSUnique,
    AdultDomainCatalog,
    AdultDomainExclusion,
    AdultSyncState,
)

from app.services.ingest_metrics import get_ingest_metrics_snapshot
from app.services.remnawave_ingest_queue import get_remnawave_queue_stats
from app.services.remnawave_adult import (
    cleanup_adult_catalog_garbage,
    force_recheck_all_dns_unique,
    get_adult_sync_runtime_state,
    get_adult_sync_schedule,
    normalize_remnawave_domain,
    set_adult_sync_schedule,
    sync_adult_catalog,
    sync_adult_catalog_from_txt,
)

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(get_current_admin)])

_PERMISSION_LABELS = {
    "android.permission.CAMERA": "Камера",
    "android.permission.READ_MEDIA_IMAGES": "Фото/изображения",
    "android.permission.READ_MEDIA_VIDEO": "Видео",
    "android.permission.READ_MEDIA_AUDIO": "Аудио",
    "android.permission.READ_EXTERNAL_STORAGE": "Файлы (чтение)",
    "android.permission.WRITE_EXTERNAL_STORAGE": "Файлы (запись)",
    "android.permission.POST_NOTIFICATIONS": "Уведомления",
    "android.permission.REQUEST_INSTALL_PACKAGES": "Установка APK",
    "android.permission.ACCESS_NETWORK_STATE": "Состояние сети",
    "android.permission.INTERNET": "Интернет",
    "android.permission.FOREGROUND_SERVICE": "Фоновый сервис",
    "android.permission.FOREGROUND_SERVICE_DATA_SYNC": "Фоновая синхронизация",
    "android.permission.QUERY_ALL_PACKAGES": "Список приложений",
}

_IP_DOMAIN_CACHE_TTL_SEC = 60 * 60 * 6
_ip_domain_cache: dict[str, tuple[float, str | None]] = {}
_adult_manual_sync_task: asyncio.Task | None = None
_adult_manual_recheck_task: asyncio.Task | None = None
_adult_manual_txt_sync_task: asyncio.Task | None = None
_adult_manual_cleanup_task: asyncio.Task | None = None
_DATABASE_STATUS_CACHE_TTL_SEC = 20
_database_status_cache: dict[str, object] = {"ts": 0.0, "data": None}
_adult_task_state: dict[str, dict] = {
    "sync": {"label": "sync", "running": False, "status": "idle"},
    "recheck": {"label": "recheck", "running": False, "status": "idle"},
    "txt_sync": {"label": "txt_sync", "running": False, "status": "idle"},
    "cleanup": {"label": "cleanup", "running": False, "status": "idle"},
}


def _invalidate_database_status_cache() -> None:
    _database_status_cache["ts"] = 0.0
    _database_status_cache["data"] = None


def _task_running(task_ref_name: str) -> bool:
    task = globals().get(task_ref_name)
    return isinstance(task, asyncio.Task) and not task.done()


def _task_state_snapshot() -> dict[str, dict]:
    return {k: dict(v) for k, v in _adult_task_state.items()}


def _task_set_started(task_key: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    item = _adult_task_state.setdefault(task_key, {})
    item.update(
        {
            "running": True,
            "status": "running",
            "started_at": now,
            "finished_at": None,
            "message": "Started",
            "progress_current": 0,
            "progress_total": 0,
            "progress_percent": 0.0,
            "last_error": None,
        }
    )


def _task_progress_cb(task_key: str):
    def _inner(payload: dict):
        item = _adult_task_state.setdefault(task_key, {})
        item["running"] = True
        item["status"] = str(payload.get("status") or "running")
        item["message"] = str(payload.get("message") or item.get("message") or "")
        item["phase"] = str(payload.get("phase") or item.get("phase") or "")
        item["progress_current"] = int(payload.get("progress_current", item.get("progress_current", 0)) or 0)
        item["progress_total"] = int(payload.get("progress_total", item.get("progress_total", 0)) or 0)
        item["progress_percent"] = round(float(payload.get("progress_percent", item.get("progress_percent", 0.0)) or 0.0), 2)
        item["updated_at"] = datetime.now(timezone.utc).isoformat()
    return _inner


def _task_set_done(task_key: str, *, status: str, result: dict | None = None, error: str | None = None) -> None:
    item = _adult_task_state.setdefault(task_key, {})
    item["running"] = False
    item["status"] = status
    item["finished_at"] = datetime.now(timezone.utc).isoformat()
    if result is not None:
        item["result"] = result
    if error:
        item["last_error"] = error
        item["message"] = error
    elif status in {"ok", "done", "completed"}:
        item["message"] = "Completed"
        item["progress_percent"] = 100.0



# ==================== HELPER ====================

async def _device_ids_for_account(account_id: str, db: AsyncSession) -> list[uuid.UUID]:
    """Return device IDs belonging to an account."""
    result = await db.execute(select(Device.id).where(Device.account_id == account_id))
    return [r[0] for r in result.all()]


def _parse_uuid(value: str, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def _parse_iso_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def _count_redis_keys(redis, pattern: str) -> int:
    total = 0
    async for _ in redis.scan_iter(pattern):
        total += 1
    return total


def _start_background_task(task_name: str, task_ref_name: str, runner, task_key: str | None = None):
    globals_ref = globals()
    task = globals_ref.get(task_ref_name)
    if isinstance(task, asyncio.Task) and not task.done():
        return {"ok": True, "started": False, "message": f"{task_name} is already running"}
    if task_key:
        _task_set_started(task_key)

    async def _guarded_runner():
        try:
            result = await runner()
            if task_key:
                _task_set_done(task_key, status="ok", result=result if isinstance(result, dict) else {"result": str(result)})
        except Exception as exc:
            tb = traceback.format_exc(limit=20)
            err_text = f"{exc.__class__.__name__}: {exc}"
            logging_buffer.add("error", f"adult-sync {task_name} failed: {err_text}\n{tb}")
            if task_key:
                _task_set_done(task_key, status="failed", error=f"{err_text}\n{tb}")
        finally:
            _invalidate_database_status_cache()

    globals_ref[task_ref_name] = asyncio.create_task(_guarded_runner())
    _invalidate_database_status_cache()
    return {"ok": True, "started": True, "message": f"{task_name} started"}


def _is_ip_literal(value: str | None) -> bool:
    if not value:
        return False
    try:
        ipaddress.ip_address(value.strip())
        return True
    except ValueError:
        return False


def _normalize_domain_candidate(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().strip(".").lower()
    if not normalized or _is_ip_literal(normalized):
        return None
    return normalized


async def _reverse_dns_name(ip: str) -> str | None:
    try:
        host, _, _ = await asyncio.to_thread(socket.gethostbyaddr, ip)
        return _normalize_domain_candidate(host)
    except Exception:
        return None


async def _build_ip_display_map(values: list[str], db: AsyncSession) -> dict[str, str]:
    ips = sorted({v.strip() for v in values if _is_ip_literal(v)})
    if not ips:
        return {}

    now = time.time()
    display_map: dict[str, str] = {}
    unresolved: set[str] = set(ips)

    # 1) Prefer recently observed DNS mappings from telemetry.
    dns_rows = await db.execute(
        select(DNSLog.resolved_ip, DNSLog.domain)
        .where(DNSLog.resolved_ip.in_(ips))
        .order_by(desc(DNSLog.timestamp))
    )
    for resolved_ip, domain in dns_rows.all():
        ip = (resolved_ip or "").strip()
        if ip not in unresolved:
            continue
        mapped_domain = _normalize_domain_candidate(domain)
        if mapped_domain:
            display_map[ip] = f"{ip} ({mapped_domain})"
            unresolved.discard(ip)

    # 2) Use in-memory cache for reverse DNS labels.
    for ip in list(unresolved):
        cached = _ip_domain_cache.get(ip)
        if cached and now - cached[0] <= _IP_DOMAIN_CACHE_TTL_SEC:
            cached_domain = cached[1]
            if cached_domain:
                display_map[ip] = f"{ip} ({cached_domain})"
            unresolved.discard(ip)

    # 3) Reverse DNS fallback for remaining IPs.
    for ip in list(unresolved):
        domain = await _reverse_dns_name(ip)
        _ip_domain_cache[ip] = (now, domain)
        if domain:
            display_map[ip] = f"{ip} ({domain})"
        unresolved.discard(ip)

    return display_map


# ==================== DASHBOARD ====================


@router.get("/database-status")
async def database_status(db: AsyncSession = Depends(get_db)):
    now_ts = time.time()
    cached_data = _database_status_cache.get("data")
    cached_ts = float(_database_status_cache.get("ts", 0.0) or 0.0)
    if cached_data is not None and (now_ts - cached_ts) <= _DATABASE_STATUS_CACHE_TTL_SEC:
        return cached_data

    redis = await get_redis()

    command_total = 0
    command_devices_with_pending = 0
    top_queues = []

    # Redis stats
    try:
        async for key in redis.scan_iter("commands:*"):
            queue_len = await redis.llen(key)
            if queue_len > 0:
                command_devices_with_pending += 1
            command_total += queue_len
            device_id = key.replace("commands:", "", 1)
            if device_id and queue_len > 0:
                top_queues.append({"device_id": device_id, "pending": queue_len})
    except Exception:
        command_total = 0
        command_devices_with_pending = 0
        top_queues = []

    top_queues.sort(key=lambda x: x["pending"], reverse=True)

    redis_info = {}
    try:
        redis_info = await redis.info()
    except Exception:
        redis_info = {}

    # PostgreSQL connections and size
    conn_stats = {}
    try:
        conn_row = await db.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE state = 'active') AS active,
                    COUNT(*) FILTER (WHERE state = 'idle') AS idle,
                    COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
                    COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL) AS waiting,
                    COUNT(*) AS total
                FROM pg_stat_activity
                WHERE datname = current_database()
                """
            )
        )
        conn_stats = conn_row.mappings().first() or {}
    except Exception:
        conn_stats = {}

    max_connections = 0
    try:
        max_conn_row = await db.execute(
            text("SELECT setting::int AS max_connections FROM pg_settings WHERE name='max_connections'")
        )
        max_connections = max_conn_row.scalar() or 0
    except Exception:
        max_connections = 0

    db_size_bytes = 0
    try:
        db_size_row = await db.execute(
            text("SELECT pg_database_size(current_database()) AS db_size")
        )
        db_size_bytes = db_size_row.scalar() or 0
    except Exception:
        db_size_bytes = 0

    pg_perf = {
        "cache_hit_ratio": 0.0,
        "xact_commit": 0,
        "xact_rollback": 0,
        "deadlocks": 0,
        "temp_bytes": 0,
    }
    try:
        perf_row = await db.execute(
            text(
                """
                SELECT
                    CASE
                        WHEN (blks_hit + blks_read) > 0
                            THEN round((blks_hit::numeric / (blks_hit + blks_read)) * 100, 2)
                        ELSE 0
                    END AS cache_hit_ratio,
                    xact_commit,
                    xact_rollback,
                    deadlocks,
                    temp_bytes
                FROM pg_stat_database
                WHERE datname = current_database()
                """
            )
        )
        perf = perf_row.mappings().first() or {}
        pg_perf = {
            "cache_hit_ratio": float(perf.get("cache_hit_ratio", 0.0) or 0.0),
            "xact_commit": int(perf.get("xact_commit", 0) or 0),
            "xact_rollback": int(perf.get("xact_rollback", 0) or 0),
            "deadlocks": int(perf.get("deadlocks", 0) or 0),
            "temp_bytes": int(perf.get("temp_bytes", 0) or 0),
        }
    except Exception:
        pg_perf = {
            "cache_hit_ratio": 0.0,
            "xact_commit": 0,
            "xact_rollback": 0,
            "deadlocks": 0,
            "temp_bytes": 0,
        }

    rsyslog_stats = {"count_1m": 0, "bytes_1m": 0, "bytes_per_entry_1m": 0}
    try:
        minute_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
        rsyslog_row = await db.execute(
            select(
                func.count(RemnawaveDNSQuery.id).label("count_1m"),
                func.coalesce(
                    func.sum(
                        func.coalesce(func.length(RemnawaveDNSQuery.account_login), 0)
                        + func.coalesce(func.length(RemnawaveDNSQuery.dns), 0)
                        + func.coalesce(func.length(RemnawaveDNSQuery.node_name), 0)
                    ),
                    0,
                ).label("bytes_1m"),
            ).where(RemnawaveDNSQuery.requested_at >= minute_ago)
        )
        rsyslog_data = rsyslog_row.mappings().first() or {}
        rsyslog_count = int(rsyslog_data.get("count_1m", 0) or 0)
        rsyslog_bytes = int(rsyslog_data.get("bytes_1m", 0) or 0)
        rsyslog_stats = {
            "count_1m": rsyslog_count,
            "bytes_1m": rsyslog_bytes,
            "bytes_per_entry_1m": round(rsyslog_bytes / rsyslog_count, 2) if rsyslog_count else 0,
        }
    except Exception:
        rsyslog_stats = {"count_1m": 0, "bytes_1m": 0, "bytes_per_entry_1m": 0}

    adult_sync = {
        "status": "not_started",
        "status_hint": "sync has not run yet",
        "last_run_at": None,
        "next_sync_eta": None,
        "last_version": "-",
        "last_updated_rows": 0,
        "catalog_domains_enabled": 0,
        "catalog_domains_total": 0,
        "catalog_sources": {
            "blocklistproject": 0,
            "oisd": 0,
            "v2fly": 0,
        },
        "unique_domains_total": 0,
        "unique_adult_total": 0,
        "unique_need_recheck": 0,
        "adult_coverage_percent": 0.0,
        "manual_tasks": {
            "sync": _task_running("_adult_manual_sync_task"),
            "recheck": _task_running("_adult_manual_recheck_task"),
            "txt_sync": _task_running("_adult_manual_txt_sync_task"),
            "cleanup": _task_running("_adult_manual_cleanup_task"),
        },
        "task_details": _task_state_snapshot(),
        "services": {
            "scheduler": "unknown",
            "recheck_worker": "unknown",
            "catalog_sync_lock": "unknown",
            "last_loop_at": None,
            "last_error_at": None,
        },
        "schedule": {
            "weekday": 6,
            "hour": 3,
            "minute": 0,
            "weekday_label": "Sun",
            "source": "default",
        },
    }

    try:
        sync_state = await db.get(AdultSyncState, "adult_domain_sync")
        if sync_state:
            state_stats = sync_state.stats_json or {}
            last_run = sync_state.last_run_at
            adult_sync["status"] = sync_state.status or "unknown"
            adult_sync["last_run_at"] = last_run.isoformat() if last_run else None
            adult_sync["next_sync_eta"] = (last_run + timedelta(days=7)).isoformat() if last_run else None
            adult_sync["last_version"] = (
                str(state_stats.get("version"))
                if state_stats.get("version")
                else (sync_state.last_watermark or "-")
            )
            adult_sync["last_updated_rows"] = int(state_stats.get("updated", 0) or 0)
            if state_stats.get("status"):
                adult_sync["status_hint"] = str(state_stats.get("status"))
    except Exception:
        pass

    try:
        schedule = await get_adult_sync_schedule()
        weekday = int(schedule.get("weekday", 6) or 6)
        hour = int(schedule.get("hour", 3) or 3)
        minute = int(schedule.get("minute", 0) or 0)
        labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        adult_sync["schedule"] = {
            "weekday": weekday,
            "hour": hour,
            "minute": minute,
            "weekday_label": labels[weekday] if 0 <= weekday < 7 else str(weekday),
            "source": str(schedule.get("source") or "unknown"),
        }
    except Exception:
        pass

    try:
        runtime = get_adult_sync_runtime_state()
        scheduler_running = bool(runtime.get("running"))
        adult_sync["services"] = {
            "scheduler": "running" if scheduler_running else "stopped",
            "recheck_worker": "running" if adult_sync["manual_tasks"]["recheck"] else "idle",
            "catalog_sync_lock": "running" if adult_sync["manual_tasks"]["sync"] or adult_sync["manual_tasks"]["txt_sync"] else "idle",
            "last_loop_at": runtime.get("last_loop_at"),
            "last_error_at": runtime.get("last_error"),
        }
        if runtime.get("next_sync_at"):
            adult_sync["next_sync_eta"] = runtime.get("next_sync_at")
    except Exception:
        pass

    try:
        row = await db.execute(
            select(
                func.count(AdultDomainCatalog.domain).label("catalog_total"),
                func.count(AdultDomainCatalog.domain).filter(AdultDomainCatalog.is_enabled.is_(True)).label("catalog_enabled"),
                func.count(AdultDomainCatalog.domain).filter(
                    and_(
                        AdultDomainCatalog.is_enabled.is_(True),
                        AdultDomainCatalog.source_mask.op("&")(1) != 0,
                    )
                ).label("src_blocklist"),
                func.count(AdultDomainCatalog.domain).filter(
                    and_(
                        AdultDomainCatalog.is_enabled.is_(True),
                        AdultDomainCatalog.source_mask.op("&")(2) != 0,
                    )
                ).label("src_oisd"),
                func.count(AdultDomainCatalog.domain).filter(
                    and_(
                        AdultDomainCatalog.is_enabled.is_(True),
                        AdultDomainCatalog.source_mask.op("&")(4) != 0,
                    )
                ).label("src_v2fly"),
            )
        )
        data = row.mappings().first() or {}
        adult_sync["catalog_domains_total"] = int(data.get("catalog_total", 0) or 0)
        adult_sync["catalog_domains_enabled"] = int(data.get("catalog_enabled", 0) or 0)
        adult_sync["catalog_sources"] = {
            "blocklistproject": int(data.get("src_blocklist", 0) or 0),
            "oisd": int(data.get("src_oisd", 0) or 0),
            "v2fly": int(data.get("src_v2fly", 0) or 0),
        }
    except Exception:
        pass

    try:
        row = await db.execute(
            select(
                func.count(RemnawaveDNSUnique.dns_root).label("unique_total"),
                func.count(RemnawaveDNSUnique.dns_root).filter(RemnawaveDNSUnique.is_adult.is_(True)).label("adult_total"),
                func.count(RemnawaveDNSUnique.dns_root).filter(RemnawaveDNSUnique.need_recheck.is_(True)).label("need_recheck"),
            )
        )
        data = row.mappings().first() or {}
        unique_total = int(data.get("unique_total", 0) or 0)
        unique_adult = int(data.get("adult_total", 0) or 0)
        adult_sync["unique_domains_total"] = unique_total
        adult_sync["unique_adult_total"] = unique_adult
        adult_sync["unique_need_recheck"] = int(data.get("need_recheck", 0) or 0)
        adult_sync["adult_coverage_percent"] = round((unique_adult / unique_total) * 100, 2) if unique_total else 0.0
    except Exception:
        pass

    if adult_sync["status"] in {"not_started", "unknown"}:
        if adult_sync["catalog_domains_enabled"] > 0:
            adult_sync["status"] = "loaded"
            adult_sync["status_hint"] = "catalog loaded, waiting scheduled sync"
        elif adult_sync["unique_domains_total"] > 0:
            adult_sync["status"] = "catalog_empty"
            adult_sync["status_hint"] = "domains exist in logs, but adult catalog is empty"

    try:
        cpu_percent = float(await asyncio.to_thread(psutil.cpu_percent, 0.2))
        memory_percent = float(psutil.virtual_memory().percent)
    except Exception:
        cpu_percent = 0.0
        memory_percent = 0.0


    db_tables = []
    try:
        db_tables = await db.execute(
            text(
                """
                SELECT
                    relname,
                    pg_total_relation_size(format('%I.%I', schemaname, relname)) AS size_bytes,
                    COALESCE(n_live_tup, 0) AS live_rows,
                    COALESCE(n_dead_tup, 0) AS dead_rows,
                    COALESCE(n_tup_ins, 0) AS tup_ins,
                    COALESCE(n_tup_upd, 0) AS tup_upd,
                    COALESCE(n_tup_del, 0) AS tup_del,
                    COALESCE(n_tup_hot_upd, 0) AS tup_hot_upd,
                    COALESCE(vacuum_count, 0) AS vacuum_count,
                    COALESCE(autovacuum_count, 0) AS autovacuum_count,
                    GREATEST(
                        COALESCE(last_autovacuum, to_timestamp(0)),
                        COALESCE(last_vacuum, to_timestamp(0))
                    ) AS last_vacuum_at
                FROM pg_stat_user_tables
                ORDER BY size_bytes DESC
                LIMIT 8
                """
            )
        )
        db_tables = db_tables.mappings().all()
    except Exception:
        db_tables = []

    online_devices = 0
    try:
        online_devices = await _count_redis_keys(redis, "online:*")
    except Exception:
        online_devices = 0

    ingest_metrics = get_ingest_metrics_snapshot()
    try:
        ingest_metrics["rsyslog_queue"] = await get_remnawave_queue_stats()
    except Exception:
        ingest_metrics["rsyslog_queue"] = {"stream_len": 0, "pending": 0, "dead_len": 0}

    response_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "postgres": {
            "max_connections": max_connections,
            "connections": {
                "active": int(conn_stats.get("active", 0) or 0),
                "idle": int(conn_stats.get("idle", 0) or 0),
                "idle_in_transaction": int(conn_stats.get("idle_in_transaction", 0) or 0),
                "waiting": int(conn_stats.get("waiting", 0) or 0),
                "total": int(conn_stats.get("total", 0) or 0),
            },
            "size_bytes": int(db_size_bytes or 0),
            "performance": pg_perf,
        },
        "redis": {
            "online_devices": online_devices,
            "memory_used_human": redis_info.get("used_memory_human", "-"),
            "connected_clients": int(redis_info.get("connected_clients", 0) or 0),
            "command_queue": {
                "total": command_total,
                "devices_with_pending": command_devices_with_pending,
                "avg_per_device": round((command_total / command_devices_with_pending) if command_devices_with_pending else 0, 2),
                "top_devices": top_queues[:10],
            },
        },
        "system": {
            "cpu_percent": round(cpu_percent, 2),
            "memory_percent": round(memory_percent, 2),
        },
        "database_tables": [
            {
                "name": str(row.get("relname") or "-"),
                "size_bytes": int(row.get("size_bytes") or 0),
                "live_rows": int(row.get("live_rows") or 0),
                "dead_rows": int(row.get("dead_rows") or 0),
                "dead_ratio_percent": round(
                    (int(row.get("dead_rows") or 0) / max(1, (int(row.get("live_rows") or 0) + int(row.get("dead_rows") or 0)))) * 100,
                    2,
                ),
                "tup_ins": int(row.get("tup_ins") or 0),
                "tup_upd": int(row.get("tup_upd") or 0),
                "tup_del": int(row.get("tup_del") or 0),
                "tup_hot_upd": int(row.get("tup_hot_upd") or 0),
                "vacuum_count": int(row.get("vacuum_count") or 0),
                "autovacuum_count": int(row.get("autovacuum_count") or 0),
                "last_vacuum_at": row.get("last_vacuum_at").isoformat() if row.get("last_vacuum_at") else None,
            }
            for row in db_tables
        ],
        "rsyslog": rsyslog_stats,
        "adult_sync": adult_sync,
        "api_ingest": ingest_metrics,
    }
    _database_status_cache["ts"] = time.time()
    _database_status_cache["data"] = response_data
    return response_data


@router.post("/adult-sync/run")
async def run_adult_sync_now():
    async def _runner():
        return await sync_adult_catalog(progress_cb=_task_progress_cb("sync"))

    return _start_background_task("sync", "_adult_manual_sync_task", _runner, task_key="sync")


@router.post("/adult-sync/recheck-all")
async def run_adult_recheck_all_now():
    async def _runner():
        return await force_recheck_all_dns_unique(limit=5000, progress_cb=_task_progress_cb("recheck"))

    return _start_background_task("full recheck", "_adult_manual_recheck_task", _runner, task_key="recheck")


@router.post("/adult-sync/sync-from-txt")
async def run_adult_sync_from_txt_now(path: str | None = Body(default=None, embed=True)):
    async def _runner():
        return await sync_adult_catalog_from_txt(path=path, progress_cb=_task_progress_cb("txt_sync"))

    return _start_background_task("txt sync", "_adult_manual_txt_sync_task", _runner, task_key="txt_sync")


@router.post("/adult-sync/cleanup-garbage")
async def run_adult_cleanup_now():
    async def _runner():
        return await cleanup_adult_catalog_garbage(progress_cb=_task_progress_cb("cleanup"))

    return _start_background_task("cleanup", "_adult_manual_cleanup_task", _runner, task_key="cleanup")


@router.post("/adult-sync/schedule")
async def set_adult_sync_schedule_endpoint(
    weekday: int = Body(default=6, embed=True),
    hour: int = Body(default=3, embed=True),
    minute: int = Body(default=0, embed=True),
):
    try:
        data = await set_adult_sync_schedule(weekday=weekday, hour=hour, minute=minute)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    _invalidate_database_status_cache()
    return {"ok": True, "schedule": data, "message": "weekly schedule updated"}


@router.get("/dashboard")
async def dashboard(db: AsyncSession = Depends(get_db)):
    redis = await get_redis()

    online_count = await _count_redis_keys(redis, "online:*")

    total_devices = (await db.execute(select(func.count(Device.id)))).scalar() or 0

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_sessions = (await db.execute(
        select(func.count(Session.id)).where(Session.connected_at >= today_start)
    )).scalar() or 0

    today_traffic = (await db.execute(
        select(
            func.coalesce(func.sum(Session.bytes_downloaded), 0),
            func.coalesce(func.sum(Session.bytes_uploaded), 0),
        ).where(Session.connected_at >= today_start)
    )).one()

    total_traffic = (await db.execute(
        select(
            func.coalesce(func.sum(Session.bytes_downloaded), 0),
            func.coalesce(func.sum(Session.bytes_uploaded), 0),
        )
    )).one()

    week_start = today_start - timedelta(days=6)
    day_bucket = func.date_trunc(text("'day'"), Session.connected_at)
    sessions_daily_rows = (
        await db.execute(
            select(
                day_bucket.label("day"),
                func.count(Session.id).label("count"),
            )
            .where(Session.connected_at >= week_start)
            .group_by(day_bucket)
            .order_by(day_bucket)
        )
    ).all()
    sessions_daily_map = {
        row.day.date().isoformat(): int(row.count or 0)
        for row in sessions_daily_rows
        if row.day is not None
    }
    sessions_per_day = []
    for i in range(6, -1, -1):
        day_start = (datetime.now(timezone.utc) - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
        key = day_start.date().isoformat()
        sessions_per_day.append({"date": key, "count": sessions_daily_map.get(key, 0)})

    return {
        "online_count": online_count,
        "total_devices": total_devices,
        "today_sessions": today_sessions,
        "today_downloaded": today_traffic[0],
        "today_uploaded": today_traffic[1],
        "total_downloaded": total_traffic[0],
        "total_uploaded": total_traffic[1],
        "sessions_per_day": sessions_per_day,
    }


# ==================== DASHBOARD: TOP SNI (paginated) ====================

@router.get("/dashboard/top-sni")
async def dashboard_top_sni(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    base_query = (
        select(SNILog.domain, func.sum(SNILog.hit_count).label("hits"))
        .where(SNILog.first_seen >= today_start)
        .group_by(SNILog.domain)
    )
    total = (await db.execute(select(func.count()).select_from(base_query.subquery()))).scalar() or 0
    result = await db.execute(
        base_query.order_by(desc("hits"))
        .offset((page - 1) * per_page).limit(per_page)
    )
    rows = result.all()
    display_map = await _build_ip_display_map([d for d, _ in rows], db)
    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "domain": d,
            "domain_display": display_map.get(d, d),
            "hits": h,
        } for d, h in rows],
    }


# ==================== DASHBOARD: ACCOUNT STATS (paginated) ====================

@router.get("/dashboard/account-stats")
async def dashboard_account_stats(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    total = (await db.execute(select(func.count(Account.account_id)))).scalar() or 0
    page_accounts = (
        await db.execute(
            select(Account.account_id, Account.description)
            .order_by(Account.created_at)
            .offset((page - 1) * per_page)
            .limit(per_page)
        )
    ).all()
    account_ids = [row.account_id for row in page_accounts]

    items = []
    traffic_map: dict[str, dict[str, int]] = {}
    if account_ids:
        stats_rows = (
            await db.execute(
                select(
                    Account.account_id.label("account_id"),
                    func.coalesce(
                        func.sum(
                            case((Session.connected_at >= today_start, Session.bytes_downloaded), else_=0)
                        ),
                        0,
                    ).label("today_downloaded"),
                    func.coalesce(
                        func.sum(
                            case((Session.connected_at >= today_start, Session.bytes_uploaded), else_=0)
                        ),
                        0,
                    ).label("today_uploaded"),
                    func.coalesce(func.sum(Session.bytes_downloaded), 0).label("total_downloaded"),
                    func.coalesce(func.sum(Session.bytes_uploaded), 0).label("total_uploaded"),
                )
                .select_from(Account)
                .outerjoin(Device, Device.account_id == Account.account_id)
                .outerjoin(Session, Session.device_id == Device.id)
                .where(Account.account_id.in_(account_ids))
                .group_by(Account.account_id)
            )
        ).all()
        traffic_map = {
            row.account_id: {
                "today_downloaded": int(row.today_downloaded or 0),
                "today_uploaded": int(row.today_uploaded or 0),
                "total_downloaded": int(row.total_downloaded or 0),
                "total_uploaded": int(row.total_uploaded or 0),
            }
            for row in stats_rows
        }

    for row in page_accounts:
        data = traffic_map.get(
            row.account_id,
            {"today_downloaded": 0, "today_uploaded": 0, "total_downloaded": 0, "total_uploaded": 0},
        )
        items.append(
            {
                "account_id": row.account_id,
                "description": row.description,
                "today_downloaded": data["today_downloaded"],
                "today_uploaded": data["today_uploaded"],
                "total_downloaded": data["total_downloaded"],
                "total_uploaded": data["total_uploaded"],
            }
        )

    return {"total": total, "page": page, "per_page": per_page, "items": items}


# ==================== ACCOUNTS ====================

@router.get("/accounts")
async def list_accounts(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(
            select(
                Account.account_id,
                Account.description,
                Account.created_at,
                func.count(Device.id).label("device_count"),
            )
            .select_from(Account)
            .outerjoin(Device, Device.account_id == Account.account_id)
            .group_by(Account.account_id, Account.description, Account.created_at)
            .order_by(Account.created_at)
        )
    ).all()
    items = [
        {
            "account_id": row.account_id,
            "description": row.description,
            "device_count": int(row.device_count or 0),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]
    return {"items": items}


@router.post("/accounts/{account_id}/description")
async def set_account_description(
    account_id: str,
    description: str = Body("", embed=True),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Account).where(Account.account_id == account_id))
    account = result.scalar_one_or_none()
    if not account:
        account = Account(account_id=account_id, description=description)
        db.add(account)
    else:
        account.description = description
    return {"status": "ok"}


# ==================== DEVICES ====================

@router.get("/devices")
async def list_devices(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: str | None = None,
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Device).order_by(desc(Device.last_seen_at))
    if search:
        query = query.where(
            Device.android_id.ilike(f"%{search}%")
            | Device.device_model.ilike(f"%{search}%")
            | Device.manufacturer.ilike(f"%{search}%")
        )
    if account_id:
        query = query.where(Device.account_id == account_id)

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    devices = result.scalars().all()

    # Get account descriptions
    acc_result = await db.execute(select(Account))
    acc_map = {a.account_id: a.description for a in acc_result.scalars().all()}

    redis = await get_redis()
    items = []
    for d in devices:
        is_online = await redis.exists(f"online:{d.id}")
        items.append({
            "id": str(d.id),
            "android_id": d.android_id,
            "device_model": d.device_model,
            "manufacturer": d.manufacturer,
            "android_version": d.android_version,
            "app_version": d.app_version,
            "is_rooted": d.is_rooted,
            "carrier": d.carrier,
            "is_blocked": d.is_blocked,
            "is_online": bool(is_online),
            "note": d.note,
            "account_id": d.account_id,
            "account_description": acc_map.get(d.account_id),
            "created_at": d.created_at.isoformat() if d.created_at else None,
            "last_seen_at": d.last_seen_at.isoformat() if d.last_seen_at else None,
        })

    return {"total": total, "page": page, "per_page": per_page, "items": items}


@router.get("/devices/{device_id}")
async def get_device(device_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.id == _parse_uuid(device_id, "device_id")))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    redis = await get_redis()
    is_online = await redis.exists(f"online:{device.id}")

    # Get permissions
    perm_result = await db.execute(
        select(DevicePermission).where(DevicePermission.device_id == device.id)
    )
    permissions = [{
        "name": p.permission_name,
        "label": _PERMISSION_LABELS.get(p.permission_name, p.permission_name),
        "granted": p.granted,
    } for p in perm_result.scalars().all()]

    # Get battery level from last session
    last_session = (await db.execute(
        select(Session).where(Session.device_id == device.id)
        .order_by(desc(Session.connected_at)).limit(1)
    )).scalar_one_or_none()
    battery_level = last_session.battery_level if last_session else None

    # Get account description
    acc_desc = None
    if device.account_id:
        acc_result = await db.execute(select(Account).where(Account.account_id == device.account_id))
        acc = acc_result.scalar_one_or_none()
        if acc:
            acc_desc = acc.description

    return {
        "id": str(device.id),
        "android_id": device.android_id,
        "device_model": device.device_model,
        "manufacturer": device.manufacturer,
        "android_version": device.android_version,
        "api_level": device.api_level,
        "app_version": device.app_version,
        "screen_resolution": device.screen_resolution,
        "dpi": device.dpi,
        "language": device.language,
        "timezone": device.timezone,
        "is_rooted": device.is_rooted,
        "carrier": device.carrier,
        "ram_total_mb": device.ram_total_mb,
        "is_blocked": device.is_blocked,
        "is_online": bool(is_online),
        "note": device.note,
        "account_id": device.account_id,
        "account_description": acc_desc,
        "battery_level": battery_level,
        "permissions": permissions,
        "created_at": device.created_at.isoformat() if device.created_at else None,
        "last_seen_at": device.last_seen_at.isoformat() if device.last_seen_at else None,
    }


@router.post("/devices/{device_id}/note")
async def set_device_note(device_id: str, note: str = "", db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.id == _parse_uuid(device_id, "device_id")))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device.note = note
    return {"status": "ok"}


@router.post("/devices/{device_id}/account")
async def set_device_account(
    device_id: str,
    account_id: str = Body("", embed=True),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Device).where(Device.id == _parse_uuid(device_id, "device_id")))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device.account_id = account_id or None
    # Auto-create account if needed
    if account_id:
        acc_result = await db.execute(select(Account).where(Account.account_id == account_id))
        if not acc_result.scalar_one_or_none():
            db.add(Account(account_id=account_id))
    return {"status": "ok"}


# ==================== DEVICE CHANGES ====================

@router.get("/devices/{device_id}/changes")
async def get_device_changes(
    device_id: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    device_uuid = _parse_uuid(device_id, "device_id")
    query = (
        select(DeviceChangeLog)
        .where(DeviceChangeLog.device_id == device_uuid)
        .order_by(desc(DeviceChangeLog.changed_at))
    )
    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar() or 0
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    changes = result.scalars().all()
    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(c.id),
            "field_name": c.field_name,
            "old_value": c.old_value,
            "new_value": c.new_value,
            "changed_at": c.changed_at.isoformat() if c.changed_at else None,
        } for c in changes],
    }


# ==================== SESSIONS ====================

@router.get("/sessions")
async def list_sessions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    device_id: str | None = None,
    account_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Session).order_by(desc(Session.connected_at))
    if device_id:
        query = query.where(Session.device_id == _parse_uuid(device_id, "device_id"))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(Session.device_id.in_(dev_ids))
    if date_from:
        query = query.where(Session.connected_at >= _parse_iso_datetime(date_from, "date_from"))
    if date_to:
        query = query.where(Session.connected_at <= _parse_iso_datetime(date_to, "date_to"))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    sessions = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(s.id),
            "device_id": str(s.device_id),
            "server_address": s.server_address,
            "server_ip": s.server_ip,
            "server_ip_changes": s.server_ip_changes,
            "protocol": s.protocol,
            "client_ip": s.client_ip,
            "client_country": s.client_country,
            "client_city": s.client_city,
            "network_type": s.network_type,
            "wifi_ssid": s.wifi_ssid,
            "carrier": s.carrier,
            "bytes_downloaded": s.bytes_downloaded,
            "bytes_uploaded": s.bytes_uploaded,
            "connection_count": s.connection_count,
            "reconnect_count": s.reconnect_count,
            "latency_ms": s.latency_ms,
            "battery_level": s.battery_level,
            "connected_at": s.connected_at.isoformat() if s.connected_at else None,
            "disconnected_at": s.disconnected_at.isoformat() if s.disconnected_at else None,
        } for s in sessions],
    }


# ==================== SNI LOGS ====================

@router.get("/sni")
async def list_sni(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    domain: str | None = None,
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(SNILog).order_by(desc(SNILog.last_seen))
    if device_id:
        query = query.where(SNILog.device_id == _parse_uuid(device_id, "device_id"))
    if session_id:
        query = query.where(SNILog.session_id == _parse_uuid(session_id, "session_id"))
    if domain:
        query = query.where(SNILog.domain.ilike(f"%{domain}%"))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(SNILog.device_id.in_(dev_ids))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()
    display_map = await _build_ip_display_map([l.domain for l in logs], db)

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "session_id": str(l.session_id),
            "device_id": str(l.device_id),
            "domain": l.domain,
            "domain_display": display_map.get(l.domain, l.domain),
            "hit_count": l.hit_count,
            "bytes_total": l.bytes_total,
            "first_seen": l.first_seen.isoformat() if l.first_seen else None,
            "last_seen": l.last_seen.isoformat() if l.last_seen else None,
        } for l in logs],
    }


@router.get("/sni/top")
async def top_sni(
    limit: int = Query(50, ge=1, le=500),
    days: int = Query(7, ge=1, le=90),
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    query = (
        select(SNILog.domain, func.sum(SNILog.hit_count).label("hits"), func.sum(SNILog.bytes_total).label("bytes"))
        .where(SNILog.first_seen >= since)
    )
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(SNILog.device_id.in_(dev_ids))
    result = await db.execute(query.group_by(SNILog.domain).order_by(desc("hits")).limit(limit))
    return [{"domain": d, "hits": h, "bytes_total": b} for d, h, b in result.all()]


# ==================== DNS LOGS ====================

@router.get("/dns")
async def list_dns(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    domain: str | None = None,
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(DNSLog).order_by(desc(DNSLog.timestamp))
    if device_id:
        query = query.where(DNSLog.device_id == _parse_uuid(device_id, "device_id"))
    if session_id:
        query = query.where(DNSLog.session_id == _parse_uuid(session_id, "session_id"))
    if domain:
        query = query.where(DNSLog.domain.ilike(f"%{domain}%"))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(DNSLog.device_id.in_(dev_ids))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "session_id": str(l.session_id),
            "device_id": str(l.device_id),
            "domain": l.domain,
            "resolved_ip": l.resolved_ip,
            "query_type": l.query_type,
            "hit_count": l.hit_count,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
        } for l in logs],
    }


# ==================== CONNECTIONS ====================

@router.get("/connections")
async def list_connections(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    dest_ip: str | None = None,
    account_id: str | None = None,
    no_dns_only: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    query = select(ConnectionLog).order_by(desc(ConnectionLog.timestamp))
    if device_id:
        query = query.where(ConnectionLog.device_id == _parse_uuid(device_id, "device_id"))
    if session_id:
        query = query.where(ConnectionLog.session_id == _parse_uuid(session_id, "session_id"))
    if dest_ip:
        query = query.where(ConnectionLog.dest_ip.ilike(f"%{dest_ip}%"))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(ConnectionLog.device_id.in_(dev_ids))
    if no_dns_only:
        query = query.where(ConnectionLog.protocol == "no-dns")

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "session_id": str(l.session_id),
            "device_id": str(l.device_id),
            "dest_ip": l.dest_ip,
            "dest_port": l.dest_port,
            "protocol": l.protocol,
            "domain": l.domain,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
        } for l in logs],
    }


# ==================== ERRORS ====================

@router.get("/errors")
async def list_errors(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    device_id: str | None = None,
    error_type: str | None = None,
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(ErrorLog).order_by(desc(ErrorLog.timestamp))
    if device_id:
        query = query.where(ErrorLog.device_id == _parse_uuid(device_id, "device_id"))
    if error_type:
        query = query.where(ErrorLog.error_type == error_type)
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(ErrorLog.device_id.in_(dev_ids))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "session_id": str(l.session_id) if l.session_id else None,
            "device_id": str(l.device_id),
            "error_type": l.error_type,
            "message": l.message,
            "stacktrace": l.stacktrace,
            "app_version": l.app_version,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
        } for l in logs],
    }


# ==================== LOGS (LIVE JOURNAL) ====================

@router.post("/logs/start")
async def logs_start():
    logging_buffer.start()
    return {"status": "ok", "enabled": True}


@router.post("/logs/stop")
async def logs_stop():
    logging_buffer.stop()
    return {"status": "ok", "enabled": False}


@router.post("/logs/clear")
async def logs_clear():
    logging_buffer.clear()
    return {"status": "ok"}


@router.get("/logs")
async def get_logs(
    log_type: str = Query("all"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    logs = logging_buffer.get_logs(log_type=log_type, limit=limit, offset=offset)
    return {"enabled": logging_buffer.enabled, "items": logs, "count": len(logs)}


# ==================== DEVICE LOGS ====================

@router.get("/device-logs")
async def list_device_logs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    device_id: str | None = None,
    account_id: str | None = None,
    log_type: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(DeviceLog).order_by(desc(DeviceLog.uploaded_at))
    if device_id:
        query = query.where(DeviceLog.device_id == _parse_uuid(device_id, "device_id"))
    if log_type:
        query = query.where(DeviceLog.log_type == log_type)
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(DeviceLog.device_id.in_(dev_ids))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "device_id": str(l.device_id),
            "log_type": l.log_type,
            "content_size": len(l.content) if l.content else 0,
            "uploaded_at": l.uploaded_at.isoformat() if l.uploaded_at else None,
        } for l in logs],
    }


@router.get("/device-logs/{log_id}")
async def get_device_log(log_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeviceLog).where(DeviceLog.id == _parse_uuid(log_id, "log_id")))
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")
    return {
        "id": str(log.id),
        "device_id": str(log.device_id),
        "log_type": log.log_type,
        "content": log.content,
        "uploaded_at": log.uploaded_at.isoformat() if log.uploaded_at else None,
    }


@router.delete("/device-logs/{log_id}")
async def delete_device_log(log_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeviceLog).where(DeviceLog.id == _parse_uuid(log_id, "log_id")))
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")
    await db.delete(log)
    return {"status": "ok"}


@router.delete("/device-logs")
async def delete_all_device_logs(db: AsyncSession = Depends(get_db)):
    await db.execute(delete(DeviceLog))
    return {"status": "ok"}


@router.get("/device-logs/{log_id}/download")
async def download_device_log(log_id: str, db: AsyncSession = Depends(get_db)):
    from fastapi.responses import StreamingResponse
    import io

    result = await db.execute(select(DeviceLog).where(DeviceLog.id == _parse_uuid(log_id, "log_id")))
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")

    output = io.BytesIO(log.content.encode("utf-8"))
    filename = f"device_log_{str(log.device_id)[:8]}_{log.log_type}_{log.uploaded_at.strftime('%Y%m%d_%H%M%S')}.txt"
    return StreamingResponse(
        output,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/device-logs/{log_id}/upload")
async def upload_device_log_to_server(log_id: str, db: AsyncSession = Depends(get_db)):
    """Upload device log content to private-ai.tools and return the download URL."""
    import httpx
    import tempfile
    import os

    result = await db.execute(select(DeviceLog).where(DeviceLog.id == _parse_uuid(log_id, "log_id")))
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")

    filename = f"device_log_{str(log.device_id)[:8]}_{log.log_type}_{log.uploaded_at.strftime('%Y%m%d_%H%M%S')}.txt"

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write(log.content)
            tmp_path = tmp.name

        async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
            with open(tmp_path, 'rb') as f:
                resp = await client.post(
                    "https://private-ai.tools/upload",
                    files={"file": (filename, f, "text/plain")},
                )
            resp.raise_for_status()
            data = resp.json()
    finally:
        if 'tmp_path' in locals():
            os.unlink(tmp_path)

    return {"status": "ok", "url": data.get("url", "")}


# ==================== DEVICE COMMANDS ====================

@router.post("/devices/{device_id}/request-logs")
async def request_device_logs(device_id: str, db: AsyncSession = Depends(get_db)):
    """Queue a command for the device to upload its logs."""
    result = await db.execute(select(Device).where(Device.id == _parse_uuid(device_id, "device_id")))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    redis = await get_redis()
    import json
    cmd = json.dumps({
        "type": "upload_logs",
        "requested_at": datetime.now(timezone.utc).isoformat(),
    })
    await redis.lpush(f"commands:{device_id}", cmd)
    await redis.expire(f"commands:{device_id}", 86400)  # 24h TTL
    logging_buffer.add("processing", f"Запрос логов от устройства {device_id}")
    return {"status": "ok", "message": "Команда отправлена"}


# ==================== EXPORT ====================

@router.get("/export/sni")
async def export_sni_csv(
    days: int = Query(7, ge=1, le=90),
    account_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    from fastapi.responses import StreamingResponse
    import io
    import csv

    since = datetime.now(timezone.utc) - timedelta(days=days)
    query = select(SNILog).where(SNILog.first_seen >= since)
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(SNILog.device_id.in_(dev_ids))
    result = await db.execute(query.order_by(desc(SNILog.last_seen)).limit(10000))
    logs = result.scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["domain", "hit_count", "bytes_total", "device_id", "session_id", "first_seen", "last_seen"])
    for l in logs:
        writer.writerow([l.domain, l.hit_count, l.bytes_total, str(l.device_id), str(l.session_id),
                         l.first_seen.isoformat() if l.first_seen else "", l.last_seen.isoformat() if l.last_seen else ""])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=sni_export_{days}d.csv"},
    )


# ==================== REMNAWAVE LOGS ====================

@router.get('/remnawave-logs/accounts')
async def remnawave_accounts_summary(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(RemnawaveAccount)
    if search:
        q = q.where(RemnawaveAccount.account_login.ilike(f"%{search}%"))

    total = (await db.execute(select(func.count()).select_from(q.subquery()))).scalar() or 0
    rows = (await db.execute(
        q.order_by(desc(RemnawaveAccount.last_activity_at), RemnawaveAccount.account_login)
        .offset((page - 1) * per_page)
        .limit(per_page)
    )).scalars().all()

    now = datetime.now(timezone.utc)
    t24 = now - timedelta(hours=24)
    t7 = now - timedelta(days=7)
    t30 = now - timedelta(days=30)
    t365 = now - timedelta(days=365)

    logins = [acc.account_login for acc in rows]
    counters_map: dict[str, dict[str, int]] = {}
    if logins:
        counters_rows = (
            await db.execute(
                select(
                    RemnawaveDNSQuery.account_login.label("account_login"),
                    func.coalesce(
                        func.sum(case((RemnawaveDNSQuery.requested_at >= t24, 1), else_=0)),
                        0,
                    ).label("c24"),
                    func.coalesce(
                        func.sum(case((RemnawaveDNSQuery.requested_at >= t7, 1), else_=0)),
                        0,
                    ).label("c7"),
                    func.coalesce(
                        func.sum(case((RemnawaveDNSQuery.requested_at >= t30, 1), else_=0)),
                        0,
                    ).label("c30"),
                    func.coalesce(
                        func.sum(case((RemnawaveDNSQuery.requested_at >= t365, 1), else_=0)),
                        0,
                    ).label("c365"),
                )
                .where(RemnawaveDNSQuery.account_login.in_(logins))
                .group_by(RemnawaveDNSQuery.account_login)
            )
        ).all()
        counters_map = {
            row.account_login: {
                "c24": int(row.c24 or 0),
                "c7": int(row.c7 or 0),
                "c30": int(row.c30 or 0),
                "c365": int(row.c365 or 0),
            }
            for row in counters_rows
        }

    items = []
    for acc in rows:
        counts = counters_map.get(acc.account_login, {"c24": 0, "c7": 0, "c30": 0, "c365": 0})
        items.append({
            'account': acc.account_login,
            'last_activity': acc.last_activity_at.isoformat() if acc.last_activity_at else None,
            'requests_24h': counts["c24"],
            'requests_7d': counts["c7"],
            'requests_30d': counts["c30"],
            'requests_365d': counts["c365"],
        })

    return {'total': total, 'page': page, 'per_page': per_page, 'items': items}


@router.get('/remnawave-logs/nodes')
async def remnawave_nodes_summary(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    node_col = func.nullif(func.trim(RemnawaveDNSQuery.node_name), "").label("node")
    last_message_col = func.max(RemnawaveDNSQuery.requested_at).label("last_message")

    grouped = (
        select(node_col, last_message_col)
        .where(node_col.isnot(None))
        .group_by(node_col)
    )

    total = (await db.execute(select(func.count()).select_from(grouped.subquery()))).scalar() or 0
    rows = (await db.execute(
        grouped
        .order_by(desc(last_message_col), node_col)
        .offset((page - 1) * per_page)
        .limit(per_page)
    )).all()

    return {
        'total': total,
        'page': page,
        'per_page': per_page,
        'items': [
            {
                'node': row[0],
                'last_message': row[1].isoformat() if row[1] else None,
            }
            for row in rows
        ],
    }


@router.get('/remnawave-audit')
async def remnawave_audit(
    account: str | None = None,
    search: str | None = None,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    query = (
        select(
            RemnawaveDNSQuery.account_login.label('account_login'),
            RemnawaveDNSQuery.dns.label('dns_root'),
            RemnawaveDNSQuery.requested_at.label('requested_at'),
        )
        .join(
            RemnawaveDNSUnique,
            and_(
                RemnawaveDNSQuery.dns == RemnawaveDNSUnique.dns_root,
                RemnawaveDNSUnique.is_adult.is_(True),
            ),
        )
        .where(
            ~exists(
                select(1).where(AdultDomainExclusion.domain == RemnawaveDNSQuery.dns)
            )
        )
    )

    if account:
        account_q = account.strip()
        if account_q:
            query = query.where(RemnawaveDNSQuery.account_login.ilike(f"%{account_q}%"))

    if search:
        search_q = search.strip()
        if search_q:
            query = query.where(RemnawaveDNSQuery.dns.ilike(f"%{search_q}%"))

    if from_ts:
        query = query.where(RemnawaveDNSQuery.requested_at >= from_ts)
    if to_ts:
        query = query.where(RemnawaveDNSQuery.requested_at <= to_ts)

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar() or 0
    rows = (await db.execute(
        query.order_by(desc(RemnawaveDNSQuery.requested_at))
        .offset((page - 1) * per_page)
        .limit(per_page)
    )).all()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "items": [
            {
                "time": row.requested_at.isoformat() if row.requested_at else None,
                "account_login": row.account_login,
                "dns_root": row.dns_root,
            }
            for row in rows
        ],
    }


@router.post('/remnawave-audit/exclude')
async def remnawave_audit_exclude_domain(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    raw_domain = str((payload or {}).get("domain", "")).strip()
    reason = str((payload or {}).get("reason", "")).strip() or "manual_exclude"
    normalized = normalize_remnawave_domain(raw_domain)
    if not normalized:
        raise HTTPException(status_code=400, detail="Invalid domain")

    now = datetime.now(timezone.utc)
    existing = await db.get(AdultDomainExclusion, normalized)
    if existing:
        existing.reason = reason
        existing.updated_at = now
    else:
        db.add(AdultDomainExclusion(domain=normalized, reason=reason, created_at=now, updated_at=now))

    await db.execute(
        update(RemnawaveDNSUnique)
        .where(RemnawaveDNSUnique.dns_root == normalized)
        .values(
            is_adult=False,
            need_recheck=False,
            mark_source=["manual_exclude"],
            mark_version="manual_exclude",
            last_marked_at=now,
        )
    )
    await db.commit()
    return {"ok": True, "domain": normalized, "excluded": True}


@router.get('/remnawave-logs/{account_login}/top-domains')
async def remnawave_top_domains(
    account_login: str,
    limit: int = Query(25, ge=1, le=100),
    days: int = Query(365, ge=1, le=3650),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = (await db.execute(
        select(
            RemnawaveDNSQuery.dns,
            func.count(RemnawaveDNSQuery.id).label('hits'),
        )
        .where(
            RemnawaveDNSQuery.account_login == account_login,
            RemnawaveDNSQuery.requested_at >= since,
        )
        .group_by(RemnawaveDNSQuery.dns)
        .order_by(desc('hits'))
        .limit(limit)
    )).all()

    return {
        'account': account_login,
        'items': [{'dns': r[0], 'hits': r[1]} for r in rows],
    }


@router.get('/remnawave-logs/{account_login}/queries')
async def remnawave_recent_queries(
    account_login: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
    q: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(RemnawaveDNSQuery).where(RemnawaveDNSQuery.account_login == account_login)

    if from_ts:
        query = query.where(RemnawaveDNSQuery.requested_at >= from_ts)
    if to_ts:
        query = query.where(RemnawaveDNSQuery.requested_at <= to_ts)
    if q:
        ql = q.strip()
        if ql:
            query = query.where(
                RemnawaveDNSQuery.dns.ilike(f"%{ql}%")
            )

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar() or 0
    rows = (await db.execute(
        query.order_by(desc(RemnawaveDNSQuery.requested_at))
        .offset((page - 1) * per_page)
        .limit(per_page)
    )).scalars().all()

    return {
        'total': total,
        'page': page,
        'per_page': per_page,
        'items': [
            {
                'dns': r.dns,
                'requested_at': r.requested_at.isoformat() if r.requested_at else None,
                'node': r.node_name,
            }
            for r in rows
        ],
    }
