from __future__ import annotations

import asyncio
import ipaddress
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.remnawave_log import RemnawaveAccount, RemnawaveDNSQuery, RemnawaveDNSUnique, RemnawaveNode
from app.services.remnawave_adult import ensure_adult_schema_ready, normalize_remnawave_domain
from app.services.schema_bootstrap import ensure_base_schema_ready

_RNW_SCHEMA_LOCK = asyncio.Lock()
_RNW_SCHEMA_READY = False


def _iter_batch_slices(total_size: int, batch_size: int) -> list[tuple[int, int]]:
    """Return safe [start:end) slices; tail batch is automatically shortened."""
    safe_total = max(0, int(total_size or 0))
    safe_batch = max(1, int(batch_size or 1))
    return [(start, min(start + safe_batch, safe_total)) for start in range(0, safe_total, safe_batch)]


async def _ensure_remnawave_schema_ready(db: AsyncSession) -> None:
    global _RNW_SCHEMA_READY
    if _RNW_SCHEMA_READY:
        return
    async with _RNW_SCHEMA_LOCK:
        if _RNW_SCHEMA_READY:
            return
        await db.execute(text("ALTER TABLE remnawave_accounts ADD COLUMN IF NOT EXISTS total_requests BIGINT NOT NULL DEFAULT 0"))
        await db.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS remnawave_nodes (
                    node_name VARCHAR(128) PRIMARY KEY,
                    last_seen_at TIMESTAMPTZ NULL,
                    first_seen_at TIMESTAMPTZ NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await db.execute(text("CREATE INDEX IF NOT EXISTS ix_remnawave_nodes_last_seen_at ON remnawave_nodes (last_seen_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS ix_remnawave_accounts_total_requests ON remnawave_accounts (total_requests DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS ix_rnw_queries_account_requested_at ON remnawave_dns_queries (account_login, requested_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS ix_rnw_queries_account_dns_requested_at ON remnawave_dns_queries (account_login, dns, requested_at DESC)"))
        await db.commit()
        _RNW_SCHEMA_READY = True


def _normalize_dns_or_ip(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None

    host = raw
    if "://" in host:
        host = host.split("://", 1)[1]
    host = host.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].strip()
    if not host:
        return None

    if host.startswith("[") and "]" in host:
        host = host[1:host.index("]")].strip()
    elif host.count(":") == 1 and host.rsplit(":", 1)[1].isdigit():
        host = host.rsplit(":", 1)[0].strip()

    if not host:
        return None

    try:
        ipaddress.ip_address(host)
        return host
    except Exception:
        pass

    return normalize_remnawave_domain(host)


@dataclass(slots=True)
class RemnawaveIngestResult:
    received: int = 0
    validated_ok: int = 0
    rejected: int = 0
    processed: int = 0
    inserted_queries: int = 0
    accounts_upserted: int = 0
    unique_candidates: int = 0
    unique_inserted: int = 0
    unique_updated: int = 0
    rejected_reasons: dict[str, int] = field(default_factory=dict)



def _parse_timestamp(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text_value = value.strip()
        if not text_value:
            return fallback
        try:
            if text_value.endswith("Z"):
                text_value = text_value[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text_value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return fallback
    return fallback


async def process_remnawave_ingest_entries(db: AsyncSession, entries: list[dict[str, Any]]) -> RemnawaveIngestResult:
    now = datetime.now(timezone.utc)
    result = RemnawaveIngestResult(received=len(entries), processed=0)

    if not entries:
        return result

    account_last_activity: dict[str, datetime] = {}
    account_request_counts: dict[str, int] = {}
    node_last_seen: dict[str, datetime] = {}
    node_first_seen: dict[str, datetime] = {}
    dns_unique_points: dict[str, tuple[datetime, datetime]] = {}
    query_rows: list[dict[str, Any]] = []

    reject_reasons: dict[str, int] = {}

    def reject(reason: str) -> None:
        reject_reasons[reason] = int(reject_reasons.get(reason, 0)) + 1

    for entry in entries:
        account_login = str(entry.get("account") or "").strip()
        raw_dns = entry.get("dns")
        dns_root = _normalize_dns_or_ip(raw_dns)

        if not account_login:
            reject("empty_account")
            continue
        if not raw_dns:
            reject("empty_dns")
            continue
        if not dns_root:
            reject("invalid_dns")
            continue

        ts = _parse_timestamp(entry.get("timestamp"), now)
        prev_ts = account_last_activity.get(account_login)
        if prev_ts is None or ts > prev_ts:
            account_last_activity[account_login] = ts
        account_request_counts[account_login] = int(account_request_counts.get(account_login, 0)) + 1

        node_name = str(entry.get("node") or "").strip()
        if node_name:
            last_seen = node_last_seen.get(node_name)
            if last_seen is None or ts > last_seen:
                node_last_seen[node_name] = ts
            first_seen = node_first_seen.get(node_name)
            if first_seen is None or ts < first_seen:
                node_first_seen[node_name] = ts

        prev_dns = dns_unique_points.get(dns_root)
        if prev_dns is None:
            dns_unique_points[dns_root] = (ts, ts)
        else:
            first_seen, last_seen = prev_dns
            dns_unique_points[dns_root] = (min(first_seen, ts), max(last_seen, ts))

        query_rows.append(
            {
                "id": uuid.uuid4(),
                "account_login": account_login,
                "dns": dns_root,
                "node_name": None,
                "requested_at": ts,
            }
        )

    result.validated_ok = len(query_rows)
    result.rejected = max(0, result.received - result.validated_ok)
    result.rejected_reasons = reject_reasons

    result.processed = len(query_rows)
    if not query_rows:
        return result

    async def _upsert_accounts_once() -> None:
        for account_login in sorted(account_last_activity):
            ts = account_last_activity[account_login]
            req_count = int(account_request_counts.get(account_login, 0))
            account_stmt = pg_insert(RemnawaveAccount).values(
                account_login=account_login,
                last_activity_at=ts,
                total_requests=req_count,
                created_at=now,
                updated_at=now,
            )
            account_stmt = account_stmt.on_conflict_do_update(
                index_elements=[RemnawaveAccount.account_login],
                set_={
                    "last_activity_at": func.greatest(RemnawaveAccount.last_activity_at, account_stmt.excluded.last_activity_at),
                    "total_requests": RemnawaveAccount.total_requests + account_stmt.excluded.total_requests,
                    "updated_at": now,
                },
            )
            await db.execute(account_stmt)

    async def _upsert_nodes_once() -> None:
        for node_name in sorted(node_last_seen):
            last_seen = node_last_seen[node_name]
            first_seen = node_first_seen.get(node_name, last_seen)
            node_stmt = pg_insert(RemnawaveNode).values(
                node_name=node_name,
                first_seen_at=first_seen,
                last_seen_at=last_seen,
                updated_at=now,
            )
            node_stmt = node_stmt.on_conflict_do_update(
                index_elements=[RemnawaveNode.node_name],
                set_={
                    "first_seen_at": func.least(RemnawaveNode.first_seen_at, node_stmt.excluded.first_seen_at),
                    "last_seen_at": func.greatest(RemnawaveNode.last_seen_at, node_stmt.excluded.last_seen_at),
                    "updated_at": now,
                },
            )
            await db.execute(node_stmt)

    try:
        await _ensure_remnawave_schema_ready(db)
        await _upsert_accounts_once()
        if node_last_seen:
            await _upsert_nodes_once()
    except Exception as exc:
        if any(tag in str(exc).lower() for tag in ("does not exist", "undefinedtable", "undefined table", "undefinedcolumn", "undefined column")):
            if not await ensure_base_schema_ready(force=True):
                raise
            await _ensure_remnawave_schema_ready(db)
            await _upsert_accounts_once()
            if node_last_seen:
                await _upsert_nodes_once()
        else:
            raise

    result.accounts_upserted = len(account_last_activity)

    for start_idx, end_idx in _iter_batch_slices(len(query_rows), 1000):
        chunk = query_rows[start_idx:end_idx]
        if not chunk:
            continue
        await db.execute(pg_insert(RemnawaveDNSQuery).values(chunk))
    result.inserted_queries = len(query_rows)

    if await ensure_adult_schema_ready():
        dns_rows = [
            {
                "dns_root": dns_root,
                "is_adult": False,
                "first_seen": first_seen,
                "last_seen": last_seen,
                "need_recheck": True,
            }
            for dns_root, (first_seen, last_seen) in sorted(dns_unique_points.items())
        ]
        result.unique_candidates = len(dns_rows)
        inserted_new = 0
        updated_existing = 0

        for start_idx, end_idx in _iter_batch_slices(len(dns_rows), 1000):
            chunk = dns_rows[start_idx:end_idx]
            if not chunk:
                continue
            try:
                dns_unique_stmt = pg_insert(RemnawaveDNSUnique).values(chunk)
                dns_unique_stmt = dns_unique_stmt.on_conflict_do_update(
                    index_elements=[RemnawaveDNSUnique.dns_root],
                    set_={
                        "first_seen": func.least(RemnawaveDNSUnique.first_seen, dns_unique_stmt.excluded.first_seen),
                        "last_seen": func.greatest(RemnawaveDNSUnique.last_seen, dns_unique_stmt.excluded.last_seen),
                        "need_recheck": True,
                    },
                ).returning(text("xmax = 0 AS inserted"))

                ret = await db.execute(dns_unique_stmt)
                rows = ret.fetchall()
                inserted_chunk = sum(1 for row in rows if bool(row[0]))
                inserted_new += inserted_chunk
                updated_existing += max(0, len(rows) - inserted_chunk)
            except SQLAlchemyError as exc:
                msg = str(exc).lower()
                if "does not exist" in msg or "undefinedtable" in msg:
                    break
                raise

        result.unique_inserted = inserted_new
        result.unique_updated = updated_existing

    return result
