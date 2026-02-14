import json
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
from app.models.device_change_log import DeviceChangeLog
from app.schemas.device import DeviceRegisterRequest, DeviceRegisterResponse
from app.schemas.session import SessionStartRequest, SessionStartResponse, SessionEndRequest
from app.schemas.telemetry import (
    SNIBatchRequest, SNIRawRequest, DNSBatchRequest, AppTrafficBatchRequest,
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
        # If account changed, create new device entry with note
        if req.account_id and existing.account_id and req.account_id != existing.account_id:
            db.add(DeviceChangeLog(device_id=existing.id, field_name="account_id",
                                   old_value=existing.account_id, new_value=req.account_id))
            existing.note = f"(смена аккаунта: {existing.account_id} -> {req.account_id})"
            logging_buffer.add("processing", f"Смена аккаунта устройства {existing.id}: {existing.account_id} -> {req.account_id}")

        # Track field changes
        now = datetime.now(timezone.utc)
        fields = {
            "device_model": req.device_model, "manufacturer": req.manufacturer,
            "android_version": req.android_version, "api_level": str(req.api_level) if req.api_level else None,
            "app_version": req.app_version, "carrier": req.carrier,
            "screen_resolution": req.screen_resolution, "language": req.language,
            "timezone": req.timezone,
        }
        for field_name, new_val in fields.items():
            if new_val is None:
                continue
            old_val = str(getattr(existing, field_name, None) or "")
            if old_val != str(new_val):
                db.add(DeviceChangeLog(device_id=existing.id, field_name=field_name,
                                       old_value=old_val, new_value=str(new_val)))

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
        existing.last_seen_at = now
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
    await redis.delete(f"dns_map:{req.session_id}")

    logging_buffer.add("processing", f"Сессия завершена: {req.session_id}, down={req.bytes_downloaded}, up={req.bytes_uploaded}")
    return {"status": "ok"}


# ==================== HEARTBEAT ====================

@router.post("/session/heartbeat")
async def session_heartbeat(
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    import json as _json
    device = await _get_device(x_api_key, db)
    device.last_seen_at = datetime.now(timezone.utc)
    redis = await get_redis()
    keys = await redis.keys(f"online:{device.id}")
    if keys:
        await redis.expire(keys[0], 300)

    # Check for pending commands
    cmd_key = f"commands:{device.id}"
    commands = []
    while True:
        raw = await redis.rpop(cmd_key)
        if raw is None:
            break
        try:
            commands.append(_json.loads(raw))
        except Exception:
            commands.append({"type": "unknown", "raw": raw})

    return {"status": "ok", "commands": commands}


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


import re

_IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
# ISO8601 timestamp at the start of xray log lines: "2026-02-14T14:23:45Z"
_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)")
# "sniffed domain: example.com"
_SNIFFED_RE = re.compile(r"sniffed domain:\s+(\S+)")
# "app/dns: returning ... for domain example.com -> [1.2.3.4, 5.6.7.8]"
_DNS_RESOLVE_RE = re.compile(r"app/dns:.*domain\s+(\S+)\s+->\s+\[([^\]]+)\]")
# "default route for tcp:149.154.167.41:443" — IP-only connections (no sniffed domain)
_DIRECT_IP_ROUTE_RE = re.compile(r"default route for (?:tcp|udp):(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+")
# "tunneling request to tcp:149.154.167.41:443" — IP-only tunneling
_TUNNEL_IP_RE = re.compile(r"tunneling request to (?:tcp|udp):(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):\d+")
# Known infrastructure domains/IPs to skip
_SKIP_DOMAINS = {"proxy", "proxy]", "direct", "direct]", "block", "block]",
                 "https", "http", "tun", "socks", ""}
# Infrastructure IPs to skip (DNS servers, VPN server itself, local)
_SKIP_IP_PREFIXES = ("10.", "127.", "192.168.", "172.16.", "0.", "1.1.1.", "8.8.8.", "8.8.4.", "9.9.9.")


def _extract_timestamp(line: str) -> datetime | None:
    """Extract ISO8601 timestamp from the beginning of an xray log line."""
    m = _TIMESTAMP_RE.match(line.strip())
    if m:
        ts_str = m.group(1)
        try:
            if ts_str.endswith("Z"):
                return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _parse_dns_log(dns_log: str) -> tuple[
    list[tuple[str, datetime | None]],
    dict[str, tuple[str, datetime | None]],
    list[tuple[str, datetime | None]],
]:
    """Parse xray error log lines containing sniffed domains and DNS resolutions.

    Returns:
        - sniffed_domains: list of (domain, timestamp)
        - ip_to_domain: mapping of IP -> (domain, timestamp)
        - raw_ips_no_dns: list of (IP, timestamp) routed without DNS
    """
    sniffed_domains: list[tuple[str, datetime | None]] = []
    ip_to_domain: dict[str, tuple[str, datetime | None]] = {}
    raw_ips_no_dns: list[tuple[str, datetime | None]] = []
    resolved_ips: set[str] = set()

    for line in dns_log.splitlines():
        ts = _extract_timestamp(line)

        m = _SNIFFED_RE.search(line)
        if m:
            domain = m.group(1).strip()
            if domain and domain not in _SKIP_DOMAINS:
                sniffed_domains.append((domain, ts))
            continue

        m2 = _DNS_RESOLVE_RE.search(line)
        if m2:
            domain = m2.group(1).strip()
            ips_str = m2.group(2)
            for ip in ips_str.split(","):
                ip = ip.strip()
                if ip and _IP_RE.match(ip):
                    ip_to_domain[ip] = (domain, ts)
                    resolved_ips.add(ip)
            continue

        m3 = _DIRECT_IP_ROUTE_RE.search(line)
        if m3:
            ip = m3.group(1)
            if ip and not ip.startswith(_SKIP_IP_PREFIXES):
                raw_ips_no_dns.append((ip, ts))
            continue

        m4 = _TUNNEL_IP_RE.search(line)
        if m4:
            ip = m4.group(1)
            if ip and not ip.startswith(_SKIP_IP_PREFIXES):
                raw_ips_no_dns.append((ip, ts))

    filtered_ips = [(ip, ts) for ip, ts in raw_ips_no_dns
                    if ip not in resolved_ips and ip not in ip_to_domain]

    return sniffed_domains, ip_to_domain, filtered_ips


def _parse_access_log_ips(raw_log: str) -> list[str]:
    """Extract destination IPs from v2ray access log (TUN mode).

    Access log format: ... accepted tcp:IP:PORT [tun >> proxy]
    """
    ips: list[str] = []
    for line in raw_log.splitlines():
        line = line.strip()
        if not line or "accepted" not in line:
            continue
        try:
            accepted = line.split("accepted", 1)[1].strip()
            target = accepted.split()[0]
            for prefix in ("tcp:", "udp:"):
                if target.startswith(prefix):
                    target = target[len(prefix):]
            # Extract IP from IP:PORT
            last_colon = target.rfind(":")
            if last_colon > 0:
                host = target[:last_colon]
            else:
                host = target
            host = host.strip()
            if host and _IP_RE.match(host):
                ips.append(host)
        except Exception:
            continue
    return ips


@router.post("/sni/raw")
async def sni_raw(
    req: SNIRawRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    """Receive raw v2ray access + error logs, parse on server, save SNI entries.

    Primary source: sniffed domains from xray error log.
    Fallback: IP->domain mapping from DNS resolutions applied to access log IPs.
    """
    device = await _get_device(x_api_key, db)
    session_id = uuid.UUID(req.session_id)
    now = datetime.now(timezone.utc)
    redis = await get_redis()
    dns_map_key = f"dns_map:{session_id}"

    # Parse DNS/sniffing log (includes IPs without DNS from error log)
    sniffed_domains, ip_to_domain, dns_log_raw_ips = (
        _parse_dns_log(req.dns_log) if req.dns_log else ([], {}, [])
    )

    # Persist new ip_to_domain entries in Redis for this session
    if ip_to_domain:
        mapping = {ip: dom for ip, (dom, _ts) in ip_to_domain.items()}
        await redis.hset(dns_map_key, mapping=mapping)
        await redis.expire(dns_map_key, 3600)

    # Build full DNS map: merge stored Redis data with current batch
    full_ip_to_domain: dict[str, str] = {}
    stored = await redis.hgetall(dns_map_key)
    if stored:
        for k, v in stored.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            full_ip_to_domain[key] = val
    # Overlay current batch (may have timestamps, but we only need domain for resolution)
    for ip, (dom, _ts) in ip_to_domain.items():
        full_ip_to_domain[ip] = dom

    # Collect domains with timestamps: {domain: {count, first_ts, last_ts}}
    domain_data: dict[str, dict] = {}
    for domain, ts in sniffed_domains:
        if domain in domain_data:
            domain_data[domain]["count"] += 1
            if ts:
                if domain_data[domain]["first_ts"] is None or ts < domain_data[domain]["first_ts"]:
                    domain_data[domain]["first_ts"] = ts
                if domain_data[domain]["last_ts"] is None or ts > domain_data[domain]["last_ts"]:
                    domain_data[domain]["last_ts"] = ts
        else:
            domain_data[domain] = {"count": 1, "first_ts": ts, "last_ts": ts}

    # Resolve IPs from access log using full DNS map (current + all previous flushes)
    if req.raw_log and full_ip_to_domain:
        access_ips = _parse_access_log_ips(req.raw_log)
        for ip in access_ips:
            if ip in full_ip_to_domain:
                d = full_ip_to_domain[ip]
                if d not in domain_data:
                    domain_data[d] = {"count": 1, "first_ts": None, "last_ts": None}
                else:
                    domain_data[d]["count"] += 1

    # Collect unresolved IPs — these also go into SNI (not ConnectionLog)
    unresolved_ip_data: dict[str, dict] = {}

    # Source 1: IPs from dns_log routing lines that had no DNS resolution
    for ip, ts in dns_log_raw_ips:
        if ip not in full_ip_to_domain:
            if ip not in unresolved_ip_data:
                unresolved_ip_data[ip] = {"count": 1, "ts": ts}
            else:
                unresolved_ip_data[ip]["count"] += 1

    # Source 2: IPs from access log not in dns map
    if req.raw_log:
        access_ips = _parse_access_log_ips(req.raw_log)
        for ip in access_ips:
            if ip not in full_ip_to_domain and not ip.startswith(_SKIP_IP_PREFIXES):
                if ip not in unresolved_ip_data:
                    unresolved_ip_data[ip] = {"count": 1, "ts": None}
                else:
                    unresolved_ip_data[ip]["count"] += 1

    # Save SNI domain entries with real timestamps from xray logs
    for domain, data in domain_data.items():
        db.add(SNILog(
            session_id=session_id,
            device_id=device.id,
            domain=domain,
            hit_count=data["count"],
            bytes_total=0,
            first_seen=data["first_ts"] or now,
            last_seen=data["last_ts"] or now,
        ))

    # Save unresolved IPs as SNILog too (domain = IP address)
    for ip, data in unresolved_ip_data.items():
        db.add(SNILog(
            session_id=session_id,
            device_id=device.id,
            domain=ip,
            hit_count=data["count"],
            bytes_total=0,
            first_seen=data["ts"] or now,
            last_seen=data["ts"] or now,
        ))

    total = len(domain_data) + len(unresolved_ip_data)
    logging_buffer.add("processing",
        f"SNI raw: {len(domain_data)} доменов + {len(unresolved_ip_data)} IP (sniffed={len(sniffed_domains)}, "
        f"dns_map={len(full_ip_to_domain)}) от устройства {device.id}")
    return {
        "status": "ok", "count": total,
        "sniffed": len(sniffed_domains), "dns_resolved": len(full_ip_to_domain),
        "unresolved_ips": len(unresolved_ip_data),
    }


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


# ==================== COMMANDS ====================

@router.get("/commands")
async def get_pending_commands(
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    """Return pending commands for this device and clear the queue."""
    import json
    device = await _get_device(x_api_key, db)
    redis = await get_redis()
    key = f"commands:{device.id}"
    commands = []
    while True:
        raw = await redis.rpop(key)
        if raw is None:
            break
        try:
            commands.append(json.loads(raw))
        except Exception:
            commands.append({"type": "unknown", "raw": raw})
    return {"commands": commands}


# ==================== DEVICE LOGS ====================

@router.post("/logs")
async def upload_device_log(
    req: DeviceLogRequest,
    db: AsyncSession = Depends(get_db),
    x_api_key: str = Header(..., alias="X-API-Key"),
):
    device = await _get_device(x_api_key, db)
    # Strip null bytes that break PostgreSQL UTF-8 encoding
    clean_content = req.content.replace("\x00", "")
    log = DeviceLog(
        device_id=device.id,
        log_type=req.log_type,
        content=clean_content,
        app_version=req.app_version,
    )
    db.add(log)
    logging_buffer.add("processing", f"Лог от устройства {device.id}: тип={req.log_type}, размер={len(req.content)}")
    return {"status": "ok"}
