import uuid
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy import select, func, desc, and_, delete
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


# ==================== HELPER ====================

async def _device_ids_for_account(account_id: str, db: AsyncSession) -> list[uuid.UUID]:
    """Return device IDs belonging to an account."""
    result = await db.execute(select(Device.id).where(Device.account_id == account_id))
    return [r[0] for r in result.all()]


# ==================== DASHBOARD ====================

@router.get("/dashboard")
async def dashboard(db: AsyncSession = Depends(get_db)):
    redis = await get_redis()

    online_keys = await redis.keys("online:*")
    online_count = len(online_keys)

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

    sessions_per_day = []
    for i in range(6, -1, -1):
        day_start = (datetime.now(timezone.utc) - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        count = (await db.execute(
            select(func.count(Session.id)).where(
                and_(Session.connected_at >= day_start, Session.connected_at < day_end)
            )
        )).scalar() or 0
        sessions_per_day.append({"date": day_start.strftime("%Y-%m-%d"), "count": count})

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
    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{"domain": d, "hits": h} for d, h in result.all()],
    }


# ==================== DASHBOARD: ACCOUNT STATS (paginated) ====================

@router.get("/dashboard/account-stats")
async def dashboard_account_stats(
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Get all accounts
    total_q = await db.execute(select(func.count(Account.account_id)))
    total = total_q.scalar() or 0
    acc_result = await db.execute(
        select(Account).order_by(Account.created_at)
        .offset((page - 1) * per_page).limit(per_page)
    )
    accounts = acc_result.scalars().all()

    items = []
    for a in accounts:
        dev_ids = await _device_ids_for_account(a.account_id, db)
        if dev_ids:
            today_traffic = (await db.execute(
                select(
                    func.coalesce(func.sum(Session.bytes_downloaded), 0),
                    func.coalesce(func.sum(Session.bytes_uploaded), 0),
                ).where(Session.device_id.in_(dev_ids), Session.connected_at >= today_start)
            )).one()
            total_traffic = (await db.execute(
                select(
                    func.coalesce(func.sum(Session.bytes_downloaded), 0),
                    func.coalesce(func.sum(Session.bytes_uploaded), 0),
                ).where(Session.device_id.in_(dev_ids))
            )).one()
        else:
            today_traffic = (0, 0)
            total_traffic = (0, 0)

        items.append({
            "account_id": a.account_id,
            "description": a.description,
            "today_downloaded": today_traffic[0],
            "today_uploaded": today_traffic[1],
            "total_downloaded": total_traffic[0],
            "total_uploaded": total_traffic[1],
        })

    return {"total": total, "page": page, "per_page": per_page, "items": items}


# ==================== ACCOUNTS ====================

@router.get("/accounts")
async def list_accounts(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Account).order_by(Account.created_at))
    accounts = result.scalars().all()
    items = []
    for a in accounts:
        device_count = (await db.execute(
            select(func.count(Device.id)).where(Device.account_id == a.account_id)
        )).scalar() or 0
        items.append({
            "account_id": a.account_id,
            "description": a.description,
            "device_count": device_count,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        })
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
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
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
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
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
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
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
    query = (
        select(DeviceChangeLog)
        .where(DeviceChangeLog.device_id == uuid.UUID(device_id))
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
        query = query.where(Session.device_id == uuid.UUID(device_id))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(Session.device_id.in_(dev_ids))
    if date_from:
        query = query.where(Session.connected_at >= datetime.fromisoformat(date_from))
    if date_to:
        query = query.where(Session.connected_at <= datetime.fromisoformat(date_to))

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
        query = query.where(SNILog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(SNILog.session_id == uuid.UUID(session_id))
    if domain:
        query = query.where(SNILog.domain.ilike(f"%{domain}%"))
    if account_id:
        dev_ids = await _device_ids_for_account(account_id, db)
        query = query.where(SNILog.device_id.in_(dev_ids))

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
        query = query.where(DNSLog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(DNSLog.session_id == uuid.UUID(session_id))
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
        query = query.where(ConnectionLog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(ConnectionLog.session_id == uuid.UUID(session_id))
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
        query = query.where(ErrorLog.device_id == uuid.UUID(device_id))
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
        query = query.where(DeviceLog.device_id == uuid.UUID(device_id))
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
    result = await db.execute(select(DeviceLog).where(DeviceLog.id == uuid.UUID(log_id)))
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
    result = await db.execute(select(DeviceLog).where(DeviceLog.id == uuid.UUID(log_id)))
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

    result = await db.execute(select(DeviceLog).where(DeviceLog.id == uuid.UUID(log_id)))
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

    result = await db.execute(select(DeviceLog).where(DeviceLog.id == uuid.UUID(log_id)))
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
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
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
