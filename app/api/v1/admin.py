import uuid
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis
from app.core.security import get_current_admin
from app.models.device import Device
from app.models.session import Session
from app.models.sni_log import SNILog
from app.models.dns_log import DNSLog
from app.models.app_traffic import AppTraffic
from app.models.connection_log import ConnectionLog
from app.models.error_log import ErrorLog

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(get_current_admin)])


# ==================== DASHBOARD ====================

@router.get("/dashboard")
async def dashboard(db: AsyncSession = Depends(get_db)):
    redis = await get_redis()

    # Online devices
    online_keys = await redis.keys("online:*")
    online_count = len(online_keys)

    # Total devices
    total_devices = (await db.execute(select(func.count(Device.id)))).scalar() or 0

    # Today's sessions
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_sessions = (await db.execute(
        select(func.count(Session.id)).where(Session.connected_at >= today_start)
    )).scalar() or 0

    # Today's traffic
    today_traffic = (await db.execute(
        select(
            func.coalesce(func.sum(Session.bytes_downloaded), 0),
            func.coalesce(func.sum(Session.bytes_uploaded), 0),
        ).where(Session.connected_at >= today_start)
    )).one()

    # Total traffic all time
    total_traffic = (await db.execute(
        select(
            func.coalesce(func.sum(Session.bytes_downloaded), 0),
            func.coalesce(func.sum(Session.bytes_uploaded), 0),
        )
    )).one()

    # Top 10 SNI domains today
    top_sni = (await db.execute(
        select(SNILog.domain, func.sum(SNILog.hit_count).label("hits"))
        .where(SNILog.first_seen >= today_start)
        .group_by(SNILog.domain)
        .order_by(desc("hits"))
        .limit(10)
    )).all()

    # Errors today
    errors_today = (await db.execute(
        select(func.count(ErrorLog.id)).where(ErrorLog.timestamp >= today_start)
    )).scalar() or 0

    # Sessions per day (last 7 days)
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
        "top_sni": [{"domain": d, "hits": h} for d, h in top_sni],
        "errors_today": errors_today,
        "sessions_per_day": sessions_per_day,
    }


# ==================== DEVICES ====================

@router.get("/devices")
async def list_devices(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Device).order_by(desc(Device.last_seen_at))
    if search:
        query = query.where(
            Device.android_id.ilike(f"%{search}%")
            | Device.device_model.ilike(f"%{search}%")
            | Device.manufacturer.ilike(f"%{search}%")
        )
    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    devices = result.scalars().all()

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
        "created_at": device.created_at.isoformat() if device.created_at else None,
        "last_seen_at": device.last_seen_at.isoformat() if device.last_seen_at else None,
    }


@router.post("/devices/{device_id}/block")
async def block_device(device_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device.is_blocked = True
    return {"status": "blocked"}


@router.post("/devices/{device_id}/unblock")
async def unblock_device(device_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device.is_blocked = False
    return {"status": "unblocked"}


@router.post("/devices/{device_id}/note")
async def set_device_note(device_id: str, note: str = "", db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.id == uuid.UUID(device_id)))
    device = result.scalar_one_or_none()
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")
    device.note = note
    return {"status": "ok"}


# ==================== SESSIONS ====================

@router.get("/sessions")
async def list_sessions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    device_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(Session).order_by(desc(Session.connected_at))
    if device_id:
        query = query.where(Session.device_id == uuid.UUID(device_id))
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
    db: AsyncSession = Depends(get_db),
):
    query = select(SNILog).order_by(desc(SNILog.last_seen))
    if device_id:
        query = query.where(SNILog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(SNILog.session_id == uuid.UUID(session_id))
    if domain:
        query = query.where(SNILog.domain.ilike(f"%{domain}%"))

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
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(SNILog.domain, func.sum(SNILog.hit_count).label("hits"), func.sum(SNILog.bytes_total).label("bytes"))
        .where(SNILog.first_seen >= since)
        .group_by(SNILog.domain)
        .order_by(desc("hits"))
        .limit(limit)
    )
    return [{"domain": d, "hits": h, "bytes_total": b} for d, h, b in result.all()]


# ==================== DNS LOGS ====================

@router.get("/dns")
async def list_dns(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    domain: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(DNSLog).order_by(desc(DNSLog.timestamp))
    if device_id:
        query = query.where(DNSLog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(DNSLog.session_id == uuid.UUID(session_id))
    if domain:
        query = query.where(DNSLog.domain.ilike(f"%{domain}%"))

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


# ==================== APP TRAFFIC ====================

@router.get("/app-traffic")
async def list_app_traffic(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(AppTraffic).order_by(desc(AppTraffic.timestamp))
    if device_id:
        query = query.where(AppTraffic.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(AppTraffic.session_id == uuid.UUID(session_id))

    total = (await db.execute(select(func.count()).select_from(query.subquery()))).scalar()
    result = await db.execute(query.offset((page - 1) * per_page).limit(per_page))
    logs = result.scalars().all()

    return {
        "total": total, "page": page, "per_page": per_page,
        "items": [{
            "id": str(l.id),
            "session_id": str(l.session_id),
            "device_id": str(l.device_id),
            "package_name": l.package_name,
            "app_name": l.app_name,
            "bytes_downloaded": l.bytes_downloaded,
            "bytes_uploaded": l.bytes_uploaded,
            "timestamp": l.timestamp.isoformat() if l.timestamp else None,
        } for l in logs],
    }


@router.get("/app-traffic/top")
async def top_apps(
    limit: int = Query(50, ge=1, le=500),
    days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(
            AppTraffic.package_name,
            func.max(AppTraffic.app_name).label("app_name"),
            func.sum(AppTraffic.bytes_downloaded + AppTraffic.bytes_uploaded).label("total_bytes"),
        )
        .where(AppTraffic.timestamp >= since)
        .group_by(AppTraffic.package_name)
        .order_by(desc("total_bytes"))
        .limit(limit)
    )
    return [{"package_name": p, "app_name": a, "total_bytes": t} for p, a, t in result.all()]


# ==================== CONNECTIONS ====================

@router.get("/connections")
async def list_connections(
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=500),
    device_id: str | None = None,
    session_id: str | None = None,
    dest_ip: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    query = select(ConnectionLog).order_by(desc(ConnectionLog.timestamp))
    if device_id:
        query = query.where(ConnectionLog.device_id == uuid.UUID(device_id))
    if session_id:
        query = query.where(ConnectionLog.session_id == uuid.UUID(session_id))
    if dest_ip:
        query = query.where(ConnectionLog.dest_ip.ilike(f"%{dest_ip}%"))

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
    db: AsyncSession = Depends(get_db),
):
    query = select(ErrorLog).order_by(desc(ErrorLog.timestamp))
    if device_id:
        query = query.where(ErrorLog.device_id == uuid.UUID(device_id))
    if error_type:
        query = query.where(ErrorLog.error_type == error_type)

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


# ==================== EXPORT ====================

@router.get("/export/sni")
async def export_sni_csv(
    days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
):
    from fastapi.responses import StreamingResponse
    import io
    import csv

    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(SNILog).where(SNILog.first_seen >= since).order_by(desc(SNILog.last_seen)).limit(10000)
    )
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
