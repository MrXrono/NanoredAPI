from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.remnawave_log import RemnawaveAccount, RemnawaveDNSQuery, RemnawaveDNSUnique
from app.services.remnawave_adult import ensure_adult_schema_ready, normalize_remnawave_domain
from app.services.schema_bootstrap import ensure_base_schema_ready


@dataclass(slots=True)
class RemnawaveIngestResult:
    received: int = 0
    processed: int = 0
    inserted_queries: int = 0
    accounts_upserted: int = 0


def _parse_timestamp(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return fallback
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return fallback
    return fallback


async def process_remnawave_ingest_entries(db: AsyncSession, entries: list[dict[str, Any]]) -> RemnawaveIngestResult:
    now = datetime.now(timezone.utc)
    result = RemnawaveIngestResult(received=len(entries), processed=0, inserted_queries=0, accounts_upserted=0)

    if not entries:
        return result

    account_last_activity: dict[str, datetime] = {}
    dns_unique_points: dict[str, tuple[datetime, datetime]] = {}
    query_rows: list[dict[str, Any]] = []

    for entry in entries:
        account_login = str(entry.get("account") or "").strip()
        dns_root = normalize_remnawave_domain(entry.get("dns"))
        if not account_login or not dns_root:
            continue

        ts = _parse_timestamp(entry.get("timestamp"), now)
        prev_ts = account_last_activity.get(account_login)
        if prev_ts is None or ts > prev_ts:
            account_last_activity[account_login] = ts

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
                "node_name": str(entry.get("node") or "").strip() or None,
                "requested_at": ts,
            }
        )

    result.processed = len(query_rows)
    if not query_rows:
        return result

    async def _upsert_accounts_once() -> None:
        for account_login in sorted(account_last_activity):
            ts = account_last_activity[account_login]
            account_stmt = pg_insert(RemnawaveAccount).values(
                account_login=account_login,
                last_activity_at=ts,
                created_at=now,
                updated_at=now,
            )
            account_stmt = account_stmt.on_conflict_do_update(
                index_elements=[RemnawaveAccount.account_login],
                set_={
                    "last_activity_at": func.greatest(RemnawaveAccount.last_activity_at, account_stmt.excluded.last_activity_at),
                    "updated_at": now,
                },
            )
            await db.execute(account_stmt)

    try:
        await _upsert_accounts_once()
    except Exception as exc:
        if any(tag in str(exc).lower() for tag in ("does not exist", "undefinedtable", "undefined table")):
            if not await ensure_base_schema_ready(force=True):
                raise
            await _upsert_accounts_once()
        else:
            raise

    result.accounts_upserted = len(account_last_activity)

    for i in range(0, len(query_rows), 1000):
        chunk = query_rows[i : i + 1000]
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
        for i in range(0, len(dns_rows), 1000):
            chunk = dns_rows[i : i + 1000]
            try:
                dns_unique_stmt = pg_insert(RemnawaveDNSUnique).values(chunk)
                dns_unique_stmt = dns_unique_stmt.on_conflict_do_update(
                    index_elements=[RemnawaveDNSUnique.dns_root],
                    set_={
                        "first_seen": func.least(RemnawaveDNSUnique.first_seen, dns_unique_stmt.excluded.first_seen),
                        "last_seen": func.greatest(RemnawaveDNSUnique.last_seen, dns_unique_stmt.excluded.last_seen),
                        "need_recheck": True,
                    },
                )
                await db.execute(dns_unique_stmt)
            except SQLAlchemyError as exc:
                msg = str(exc).lower()
                if "does not exist" in msg or "undefinedtable" in msg:
                    break
                raise

    return result
