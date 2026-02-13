import secrets
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Header, Request, status
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis import get_redis
from app.core.security import verify_api_key
from app.core.logging_buffer import logging_buffer
from app.models.device import Device
from app.models.account import Account
from app.models.session import Session
from app.models.sni_log import SNILog
from app.models.dns_log import DNSLog
from app.models.app_traffic import AppTraffic
from app.models.connection_log import ConnectionLog
from app.models.error_log import ErrorLog
from app.models.device_permission import DevicePermission
from app.models.device_log import DeviceLog
from app.schemas.device import DeviceRegisterRequest, DeviceRegisterResponse
from app.schemas.session import SessionStartRequest, SessionStartResponse, SessionEndRequest
from app.schemas.telemetry import (
    SNIBatchRequest, DNSBatchRequest, AppTrafficBatchRequest,
    ConnectionBatchRequest, ErrorReportRequest, PermissionsBatchRequest,
    DeviceLogRequest,
)
from app.services.geoip import lookup_ip

import bcrypt

router = APIRouter(prefix="/client", tags=["client"])


async def _get_device(api_key: str, db: AsyncSession) -> Device:
    """Validate API key and return device."""
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
    if not verify_api_key(secret, device.api_key_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    if device.is_blocked:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Device is blocked")
    return device


# ==================== REGISTER ====================

@router.post("/register", response_model=DeviceRegisterResponse)
async def register_device(req: DeviceRegisterRequest, db: AsyncSession = Depends(get_db)):
    logging_buffer.add("processing", f"Регистрация устройства: android_id={req.android_id}, account_id={req.account_id}")

    # Auto-create account if account_id provided
    if req.account_id:
        acc_result = await db.execute(select(Account).where(Account.account_id == req.account_id))
        if not acc_result.scalar_one_or_none():
            db.add(Account(account_id=req.account_id))
            logging_buffer.add("processing", f"Создан аккаунт: {req.account_id}")

    # Check if already registered
    result = await db.execute(select(Device).where(Device.android_id == req.android_id))
    existing = result.scalar_one_or_none()
    if existing:
        secret = secrets.token_urlsafe(32)
        existing.api_key_hash = bcrypt.hashpw(secret.encode(), bcrypt.gensalt()).decode()
        existing.device_model = req.device_model or existing.device_model
        existing.manufacturer = req.manufacturer or existing.manufacturer
        existing.android_version = req.android_version or existing.android_version
        existing.api_level = req.api_level or existing.api_level
        existing.app_version = req.app_version or existing.app_version
        existing.screen_resolution = req.screen_resolution or existing.screen_resolution
        existing.dpi = req.dpi or existing.dpi
        existing.language = req.language or existing.language
        existing.timezone = req.timezone or existing.timezone
        existing.is_rooted = req.is_rooted
        existing.carrier = req.carrier or existing.carrier
        existing.ram_total_mb = req.ram_total_mb or existing.ram_total_mb
        if req.account_id:
            existing.account_id = req.account_id
        existing.last_seen_at = datetime.now(timezone.utc)
        await db.flush()
        logging_buffer.add("processing", f"Устройство перерегистрировано: {existing.id}, account_id={existing.account_id}")
        return DeviceRegisterResponse(device_id=str(existing.id), api_key=f"{existing.id}:{secret}")

    secret = secrets.token_urlsafe(32)
    device = Device(
        android_id=req.android_id,
        api_key_hash=bcrypt.hashpw(secret.encode(), bcrypt.gensalt()).decode(),
        device_model=req.device_model,
        manufacturer=req.manufacturer,
        android_version=req.android_version,
        api_level=req.api_level,
        app_version=req.app_version,
        screen_resolution=req.screen_resolution,
        dpi=req.dpi,
        language=req.language,
        timezone=req.timezone,
        is_rooted=req.is_rooted,
        carrier=req.carrier,
        ram_total_mb=req.ram_total_mb,
        account_id=req.account_id,
        last_seen_at=datetime.now(timezone.utc),
    )
    db.add(device)
    await db.flush()
    logging_buffer.add("processing", f"Новое устройство: {device.id}, account_id={device.account_id}")
    return DeviceRegisterResponse(device_id=str(device.id), api_key=f"{device.id}:{secret}")


# ==================== SESSION ====================

@router.post("/session/start", response_model=SessionStartResponse)
async def session_start(
    req: SessionStartRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    client_ip = request.headers.get("X-Real-IP") or request.client.host
    geo = lookup_ip(client_ip)

    session = Session(
        device_id=device.id,
        server_address=req.server_address,
        protocol=req.protocol,
        client_ip=client_ip,
        client_country=geo["country"],
        client_city=geo["city"],
        network_type=req.network_type,
        wifi_ssid=req.wifi_ssid,
        carrier=req.carrier,
        latency_ms=req.latency_ms,
        battery_level=req.battery_level,
    )
    db.add(session)
    await db.flush()

    device.last_seen_at = datetime.now(timezone.utc)

    redis = await get_redis()
    await redis.set(f"online:{device.id}", str(session.id), ex=300)

    logging_buffer.add("processing", f"Сессия начата: {session.id}, устройство={device.id}, протокол={req.protocol}, IP={client_ip}")
    return SessionStartResponse(session_id=str(session.id))


@router.post("/session/end")
async def session_end(
    req: SessionEndRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    result = await db.execute(
        select(Session).where(Session.id == uuid.UUID(req.session_id), Session.device_id == device.id)
    )
    session = result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    session.bytes_downloaded = req.bytes_downloaded
    session.bytes_uploaded = req.bytes_uploaded
    session.connection_count = req.connection_count
    session.reconnect_count = req.reconnect_count
    session.disconnected_at = datetime.now(timezone.utc)
    device.last_seen_at = datetime.now(timezone.utc)

    redis = await get_redis()
    await redis.delete(f"online:{device.id}")

    logging_buffer.add("processing", f"Сессия завершена: {req.session_id}, down={req.bytes_downloaded}, up={req.bytes_uploaded}")
    return {"status": "ok"}


# ==================== HEARTBEAT ====================

@router.post("/session/heartbeat")
async def session_heartbeat(
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    device.last_seen_at = datetime.now(timezone.utc)
    redis = await get_redis()
    keys = await redis.keys(f"online:{device.id}")
    if keys:
        await redis.expire(keys[0], 300)
    return {"status": "ok"}


# ==================== TELEMETRY ====================

@router.post("/sni/batch")
async def sni_batch(
    req: SNIBatchRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    session_id = uuid.UUID(req.session_id)
    now = datetime.now(timezone.utc)
    for entry in req.entries:
        log = SNILog(
            session_id=session_id,
            device_id=device.id,
            domain=entry.domain,
            hit_count=entry.hit_count,
            bytes_total=entry.bytes_total,
            first_seen=now,
            last_seen=now,
        )
        db.add(log)
    logging_buffer.add("processing", f"SNI batch: {len(req.entries)} записей от устройства {device.id}")
    return {"status": "ok", "count": len(req.entries)}


@router.post("/dns/batch")
async def dns_batch(
    req: DNSBatchRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    session_id = uuid.UUID(req.session_id)
    for entry in req.entries:
        log = DNSLog(
            session_id=session_id,
            device_id=device.id,
            domain=entry.domain,
            resolved_ip=entry.resolved_ip,
            query_type=entry.query_type,
            hit_count=entry.hit_count,
        )
        db.add(log)
    logging_buffer.add("processing", f"DNS batch: {len(req.entries)} записей от устройства {device.id}")
    return {"status": "ok", "count": len(req.entries)}


@router.post("/app-traffic/batch")
async def app_traffic_batch(
    req: AppTrafficBatchRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    session_id = uuid.UUID(req.session_id)
    for entry in req.entries:
        log = AppTraffic(
            session_id=session_id,
            device_id=device.id,
            package_name=entry.package_name,
            app_name=entry.app_name,
            bytes_downloaded=entry.bytes_downloaded,
            bytes_uploaded=entry.bytes_uploaded,
        )
        db.add(log)
    logging_buffer.add("processing", f"App traffic batch: {len(req.entries)} записей от устройства {device.id}")
    return {"status": "ok", "count": len(req.entries)}


@router.post("/connections/batch")
async def connections_batch(
    req: ConnectionBatchRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    session_id = uuid.UUID(req.session_id)
    for entry in req.entries:
        log = ConnectionLog(
            session_id=session_id,
            device_id=device.id,
            dest_ip=entry.dest_ip,
            dest_port=entry.dest_port,
            protocol=entry.protocol,
            domain=entry.domain,
        )
        db.add(log)
    logging_buffer.add("processing", f"Connections batch: {len(req.entries)} записей от устройства {device.id}")
    return {"status": "ok", "count": len(req.entries)}


@router.post("/error")
async def report_error(
    req: ErrorReportRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    log = ErrorLog(
        session_id=uuid.UUID(req.session_id) if req.session_id else None,
        device_id=device.id,
        error_type=req.error_type,
        message=req.message,
        stacktrace=req.stacktrace,
        app_version=req.app_version,
    )
    db.add(log)
    logging_buffer.add("error", f"Ошибка от устройства {device.id}: [{req.error_type}] {req.message}")
    return {"status": "ok"}


# ==================== PERMISSIONS ====================

@router.post("/permissions")
async def update_permissions(
    req: PermissionsBatchRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    await db.execute(delete(DevicePermission).where(DevicePermission.device_id == device.id))
    now = datetime.now(timezone.utc)
    for p in req.permissions:
        db.add(DevicePermission(
            device_id=device.id,
            permission_name=p.name,
            granted=p.granted,
            updated_at=now,
        ))
    logging_buffer.add("processing", f"Permissions: {len(req.permissions)} разрешений от устройства {device.id}")
    return {"status": "ok", "count": len(req.permissions)}


# ==================== DEVICE LOGS ====================

@router.post("/logs")
async def upload_device_log(
    req: DeviceLogRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    log = DeviceLog(
        device_id=device.id,
        log_type=req.log_type,
        content=req.content,
        app_version=req.app_version,
    )
    db.add(log)
    logging_buffer.add("processing", f"Лог от устройства {device.id}: тип={req.log_type}, размер={len(req.content)}")
    return {"status": "ok"}
