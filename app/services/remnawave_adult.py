import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from collections.abc import Awaitable, Callable

import httpx
from sqlalchemy import inspect
from sqlalchemy import String, and_, bindparam, cast, delete, desc, exists, func, or_, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import ARRAY, insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session, engine
from app.services.schema_bootstrap import ensure_base_schema_ready
from app.models.remnawave_log import (
    AdultDomainBucket09,
    AdultDomainBucketAD,
    AdultDomainBucketEH,
    AdultDomainBucketIL,
    AdultDomainBucketMP,
    AdultDomainBucketOld,
    AdultDomainBucketQT,
    AdultDomainBucketUX,
    AdultDomainBucketYZ,
    AdultDomainCatalog,
    AdultDomainExclusion,
    AdultSyncState,
    RemnawaveDNSUnique,
)
from app.services.runtime_control import services_enabled, services_killed

logger = logging.getLogger(__name__)

try:  # optional; fallback when dependency is not installed
    import tldextract
except Exception:  # pragma: no cover
    tldextract = None


ADULT_SYNC_JOB = "adult_domain_sync"
ADULT_SYNC_SCHEDULE_JOB = "adult_domain_sync_schedule"
SOURCE_BLOCKLIST = 1
SOURCE_OISD = 2
SOURCE_V2FLY = 4
SOURCE_TXT_IMPORT = 8
SOURCE_LABELS = {
    SOURCE_BLOCKLIST: "blocklistproject",
    SOURCE_OISD: "oisd",
    SOURCE_V2FLY: "v2fly",
    SOURCE_TXT_IMPORT: "txt_import",
}

BLOCKLIST_URL = "https://blocklistproject.github.io/Lists/alt-version/porn-nl.txt"
OISD_ROOT_URL = "https://oisd.nl/includedlists/nsfw"
V2FLY_ROOT_URL = "https://raw.githubusercontent.com/v2fly/domain-list-community/refs/heads/master/data/category-porn"

ADULT_SYNC_DB_CHUNK_SIZE = max(500, int(os.getenv("ADULT_SYNC_DB_CHUNK_SIZE", "5000")))
ADULT_SYNC_PARSE_LINE_CHUNK_SIZE = max(500, int(os.getenv("ADULT_SYNC_PARSE_LINE_CHUNK_SIZE", "5000")))
ADULT_SYNC_TXT_DB_CHUNK_SIZE = max(500, min(20000, int(os.getenv("ADULT_SYNC_TXT_DB_CHUNK_SIZE", str(ADULT_SYNC_DB_CHUNK_SIZE)))))
ADULT_SYNC_TXT_DB_COMMIT_EVERY = max(1, int(os.getenv("ADULT_SYNC_TXT_DB_COMMIT_EVERY", "10")))
ADULT_SYNC_TXT_COPY_ENABLED = os.getenv("ADULT_SYNC_TXT_COPY_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
ADULT_SYNC_TXT_MERGE_CHUNK_SIZE = max(1000, int(os.getenv("ADULT_SYNC_TXT_MERGE_CHUNK_SIZE", "50000")))
ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS = max(1, int(os.getenv("ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS", "4")))
ADULT_SYNC_DB_RETRY_BASE_SLEEP_MS = max(10, int(os.getenv("ADULT_SYNC_DB_RETRY_BASE_SLEEP_MS", "100")))
ADULT_SYNC_DB_MERGE_CHUNK_SIZE = max(1000, int(os.getenv("ADULT_SYNC_DB_MERGE_CHUNK_SIZE", "25000")))
ADULT_SYNC_DB_STALE_CHUNK_SIZE = max(1000, int(os.getenv("ADULT_SYNC_DB_STALE_CHUNK_SIZE", "25000")))
ADULT_SYNC_TXT_PATH = os.getenv("ADULT_SYNC_TXT_PATH", "/app/data/artifacts/adult_domains_merged.txt").strip()
ADULT_SYNC_SOURCES_DIR = Path(os.getenv("ADULT_SYNC_SOURCES_DIR", "/app/data/artifacts/adult_sources").strip() or "/app/data/artifacts/adult_sources")
ADULT_SYNC_BUCKETS_DIR = Path(os.getenv("ADULT_SYNC_BUCKETS_DIR", "/app/data/artifacts/adult_buckets").strip() or "/app/data/artifacts/adult_buckets")
ADULT_SYNC_SOURCE_MANIFEST_PATH = Path(
    os.getenv("ADULT_SYNC_SOURCE_MANIFEST_PATH", str(ADULT_SYNC_SOURCES_DIR / "manifest.json")).strip()
    or str(ADULT_SYNC_SOURCES_DIR / "manifest.json")
)
ADULT_SYNC_GARBAGE_RETENTION_DAYS = max(1, int(os.getenv("ADULT_SYNC_GARBAGE_RETENTION_DAYS", "30")))
ADULT_SYNC_WEEKDAY_DEFAULT = max(0, min(6, int(os.getenv("ADULT_SYNC_WEEKDAY", "6"))))
ADULT_SYNC_HOUR_DEFAULT = max(0, min(23, int(os.getenv("ADULT_SYNC_HOUR_UTC", "3"))))
ADULT_SYNC_MINUTE_DEFAULT = max(0, min(59, int(os.getenv("ADULT_SYNC_MINUTE_UTC", "0"))))
ADULT_SYNC_USE_TLDEXTRACT = os.getenv("ADULT_SYNC_USE_TLDEXTRACT", "0").strip().lower() in {"1", "true", "yes", "on"}
ADULT_RECHECK_BATCH_LIMIT = max(100, int(os.getenv("ADULT_RECHECK_BATCH_LIMIT", "1000")))
ADULT_RECHECK_MAX_BATCHES_PER_LOOP = max(1, int(os.getenv("ADULT_RECHECK_MAX_BATCHES_PER_LOOP", "2")))
ADULT_RECHECK_LOOP_SLEEP_SECONDS = max(5, int(os.getenv("ADULT_RECHECK_LOOP_SLEEP_SECONDS", "30")))
ADULT_UNIQUE_STATS_CACHE_TTL_SEC = max(10, int(os.getenv("ADULT_UNIQUE_STATS_CACHE_TTL_SEC", "60")))
ADULT_MARK_RECHECK_CHUNK_SIZE = max(500, int(os.getenv("ADULT_MARK_RECHECK_CHUNK_SIZE", "5000")))
ADULT_MARK_RECHECK_MAX_ROWS_PER_SYNC = max(1000, int(os.getenv("ADULT_MARK_RECHECK_MAX_ROWS_PER_SYNC", "200000")))
ADULT_SYNC_CLEANUP_TABLES = (
    "adult_domain_catalog",
    "remnawave_dns_unique",
    "remnawave_dns_queries",
    "remnawave_accounts",
)
KNOWN_E_TLD_SUFFIXES = {
    "co.uk",
    "com.au",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "co.jp",
    "ne.jp",
    "ac.jp",
    "co.kr",
    "com.br",
    "com.cn",
    "co.in",
    "co.za",
    "net.cn",
}

_CATALOG_SYNC_LOCK = asyncio.Lock()
_FULL_RECHECK_LOCK = asyncio.Lock()
_TXT_SYNC_LOCK = asyncio.Lock()
_MAINTENANCE_LOCK = asyncio.Lock()
_SCHEMA_READY_LOCK = asyncio.Lock()
ADULT_BUCKET_FILE_TO_TABLE = {
    "0-9.txt": "adult_domain_bucket_0_9",
    "a-d.txt": "adult_domain_bucket_a_d",
    "e-h.txt": "adult_domain_bucket_e_h",
    "i-l.txt": "adult_domain_bucket_i_l",
    "m-p.txt": "adult_domain_bucket_m_p",
    "q-t.txt": "adult_domain_bucket_q_t",
    "u-x.txt": "adult_domain_bucket_u_x",
    "y-z.txt": "adult_domain_bucket_y_z",
    "old.txt": "adult_domain_bucket_old",
}
_ADULT_BUCKET_TABLE_MODEL_BY_NAME = {
    "adult_domain_bucket_0_9": AdultDomainBucket09,
    "adult_domain_bucket_a_d": AdultDomainBucketAD,
    "adult_domain_bucket_e_h": AdultDomainBucketEH,
    "adult_domain_bucket_i_l": AdultDomainBucketIL,
    "adult_domain_bucket_m_p": AdultDomainBucketMP,
    "adult_domain_bucket_q_t": AdultDomainBucketQT,
    "adult_domain_bucket_u_x": AdultDomainBucketUX,
    "adult_domain_bucket_y_z": AdultDomainBucketYZ,
    "adult_domain_bucket_old": AdultDomainBucketOld,
}
_ADULT_TABLES = {
    "adult_domain_catalog",
    "remnawave_dns_unique",
    "adult_sync_state",
    "adult_domain_exclusions",
    *set(ADULT_BUCKET_FILE_TO_TABLE.values()),
}
_adult_schema_ready = False
_adult_table_tuned = False
_ADULT_STAGING_TABLE = "adult_domain_catalog_staging"
_ADULT_TXT_STAGING_TABLE = "adult_domain_catalog_txt_staging"
ProgressCallback = Callable[[dict], None | Awaitable[None]]
_SCHEDULE_LOCK = asyncio.Lock()
_schedule_cache: dict | None = None
_bg_runtime_state: dict[str, object] = {
    "running": False,
    "last_loop_at": None,
    "last_error": None,
    "next_sync_at": None,
    "schedule": {
        "weekday": ADULT_SYNC_WEEKDAY_DEFAULT,
        "hour": ADULT_SYNC_HOUR_DEFAULT,
        "minute": ADULT_SYNC_MINUTE_DEFAULT,
        "source": "env/default",
    },
}


def _iter_batch_slices(total_size: int, batch_size: int) -> list[tuple[int, int]]:
    """Return safe [start:end) slices; tail batch is automatically shortened."""
    safe_total = max(0, int(total_size or 0))
    safe_batch = max(1, int(batch_size or 1))
    return [(start, min(start + safe_batch, safe_total)) for start in range(0, safe_total, safe_batch)]


def _msg_has_undefined_table(err: Exception) -> bool:
    msg = str(err).lower()
    return "does not exist" in msg or "undefinedtable" in msg or "undefined table" in msg


def _msg_has_deadlock(err: Exception) -> bool:
    msg = str(err).lower()
    return "deadlock detected" in msg or "deadlockdetectederror" in msg


def _msg_has_retryable_db_error(err: Exception) -> bool:
    msg = str(err).lower()
    cls = err.__class__.__name__.lower()
    tokens = (
        "deadlock detected",
        "deadlockdetectederror",
        "could not serialize access",
        "serializationfailure",
        "lock timeout",
        "locknotavailable",
        "statement timeout",
        "querycanceled",
        "timeouterror",
    )
    if isinstance(err, TimeoutError):
        return True
    return any(token in msg for token in tokens) or any(token in cls for token in tokens)


async def _run_with_db_retry(
    op: Callable[[], Awaitable[None]],
    *,
    db: AsyncSession,
    op_name: str,
    max_attempts: int = ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS,
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            await op()
            return
        except Exception as exc:
            await db.rollback()
            retryable = _msg_has_retryable_db_error(exc)
            if retryable and attempt < max_attempts:
                sleep_sec = (ADULT_SYNC_DB_RETRY_BASE_SLEEP_MS / 1000.0) * attempt
                logger.warning("%s failed with retryable error (%s/%s): %s", op_name, attempt, max_attempts, exc)
                await asyncio.sleep(sleep_sec)
                continue
            raise


async def _required_adult_tables_exist() -> bool:
    async with engine.connect() as conn:
        return await conn.run_sync(
            lambda c: _ADULT_TABLES.issubset(set(inspect(c).get_table_names()))
        )


async def _ensure_adult_schema() -> bool:
    global _adult_schema_ready
    if _adult_schema_ready and await _required_adult_tables_exist():
        return True

    async with _SCHEMA_READY_LOCK:
        if _adult_schema_ready and await _required_adult_tables_exist():
            return True

        if not await ensure_base_schema_ready():
            logger.warning("adult schema ensure: global schema bootstrap failed")
            return False

        _adult_schema_ready = await _required_adult_tables_exist()
        if not _adult_schema_ready:
            logger.warning("adult schema ensure: remnawave tables still missing after global bootstrap")
            return False
        await _ensure_adult_catalog_table_tuning()
        return True


async def ensure_adult_schema_ready() -> bool:
    """Public helper for ensuring adult detection tables exist before write/reads."""
    return await _ensure_adult_schema()


async def _ensure_adult_catalog_table_tuning() -> None:
    """Apply per-table storage/autovacuum options for lower write overhead."""
    global _adult_table_tuned
    if _adult_table_tuned:
        return

    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    ALTER TABLE adult_domain_catalog SET (
                        fillfactor = 90,
                        autovacuum_vacuum_scale_factor = 0.01,
                        autovacuum_analyze_scale_factor = 0.005,
                        autovacuum_vacuum_threshold = 1000,
                        autovacuum_analyze_threshold = 500
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_remnawave_dns_unique_recheck_last_seen
                    ON remnawave_dns_unique (last_seen)
                    WHERE need_recheck IS TRUE
                    """
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_remnawave_dns_unique_is_adult_true
                    ON remnawave_dns_unique (dns_root)
                    WHERE is_adult IS TRUE
                    """
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS ix_remnawave_dns_unique_adult_last_seen
                    ON remnawave_dns_unique (last_seen DESC)
                    WHERE is_adult IS TRUE
                    """
                )
            )
        _adult_table_tuned = True
    except Exception:
        logger.warning("adult catalog tuning skipped", exc_info=True)


async def _prepare_adult_staging_table(db: AsyncSession) -> None:
    await db.execute(
        text(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS {_ADULT_STAGING_TABLE} (
                domain varchar(255) PRIMARY KEY,
                category varchar(64) NOT NULL,
                source_mask integer NOT NULL,
                source_text jsonb,
                list_version varchar(64),
                checked_at timestamptz,
                is_enabled boolean NOT NULL DEFAULT TRUE
            )
            """
        )
    )
    await db.execute(text(f"TRUNCATE TABLE {_ADULT_STAGING_TABLE}"))


async def _prepare_adult_txt_staging_table(db: AsyncSession) -> None:
    await db.execute(
        text(
            f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS {_ADULT_TXT_STAGING_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                domain varchar(255) NOT NULL
            )
            """
        )
    )
    await db.execute(text(f"ALTER TABLE {_ADULT_TXT_STAGING_TABLE} ADD COLUMN IF NOT EXISTS id BIGSERIAL"))
    await db.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{_ADULT_TXT_STAGING_TABLE}_id ON {_ADULT_TXT_STAGING_TABLE}(id)"))
    await db.execute(text(f"TRUNCATE TABLE {_ADULT_TXT_STAGING_TABLE} RESTART IDENTITY"))


async def _copy_insert_txt_domains(db: AsyncSession, domains: list[str]) -> None:
    if not domains:
        return
    filtered_domains = [d for d in domains if _is_domain_db_safe(d)]
    if not filtered_domains:
        return

    if ADULT_SYNC_TXT_COPY_ENABLED:
        try:
            conn = await db.connection()
            raw_conn = await conn.get_raw_connection()
            driver_conn = getattr(raw_conn, "driver_connection", None)
            if driver_conn is None:
                raise RuntimeError("No asyncpg driver connection available")
            records = [(domain,) for domain in filtered_domains]
            await driver_conn.copy_records_to_table(
                _ADULT_TXT_STAGING_TABLE,
                records=records,
                columns=["domain"],
            )
            return
        except Exception as exc:
            logger.warning("adult txt sync: COPY failed, fallback to array insert: %s", exc)

    stmt = text(
        f"""
        INSERT INTO {_ADULT_TXT_STAGING_TABLE} (domain)
        SELECT d.domain
        FROM unnest(:domains) AS d(domain)
        """
    ).bindparams(bindparam("domains", type_=ARRAY(String())))
    await db.execute(stmt, {"domains": filtered_domains})


async def _merge_txt_staging_into_main_staging(
    db: AsyncSession,
    *,
    version: str,
    checked_at: datetime,
) -> int:
    """Merge TXT staging in bounded chunks to avoid full-table GROUP BY timeouts."""
    total_inserted = 0
    last_id = 0

    while True:
        res = await db.execute(
            text(
                f"""
                WITH batch AS (
                    SELECT id, domain
                    FROM {_ADULT_TXT_STAGING_TABLE}
                    WHERE id > :last_id
                    ORDER BY id
                    LIMIT :batch_size
                ),
                ins AS (
                    INSERT INTO {_ADULT_STAGING_TABLE}
                        (domain, category, source_mask, source_text, list_version, checked_at, is_enabled)
                    SELECT
                        b.domain,
                        'adult',
                        :source_mask,
                        CAST(:source_text AS jsonb),
                        :list_version,
                        :checked_at,
                        TRUE
                    FROM batch AS b
                    ON CONFLICT (domain) DO NOTHING
                    RETURNING 1
                )
                SELECT
                    COALESCE((SELECT MAX(id) FROM batch), :last_id) AS next_last_id,
                    (SELECT COUNT(*) FROM batch) AS batch_count,
                    (SELECT COUNT(*) FROM ins) AS inserted_count
                """
            ),
            {
                "last_id": last_id,
                "batch_size": ADULT_SYNC_TXT_MERGE_CHUNK_SIZE,
                "source_mask": int(SOURCE_TXT_IMPORT),
                "source_text": json.dumps(["txt_import"]),
                "list_version": version,
                "checked_at": checked_at,
            },
        )
        row = res.first()
        if not row:
            break

        batch_count = int(row.batch_count or 0)
        inserted_count = int(row.inserted_count or 0)
        last_id = int(row.next_last_id or last_id)
        total_inserted += inserted_count

        if batch_count <= 0:
            break

        await db.commit()

    return total_inserted



async def _bulk_insert_staging_rows(db: AsyncSession, rows: list[dict]) -> None:
    if not rows:
        return
    prepared_rows = []
    for row in rows:
        mapped = dict(row)
        mapped["source_text"] = json.dumps(mapped.get("source_text") or [])
        prepared_rows.append(mapped)
    await db.execute(
        text(
            f"""
            INSERT INTO {_ADULT_STAGING_TABLE}
                (domain, category, source_mask, source_text, list_version, checked_at, is_enabled)
            VALUES
                (:domain, :category, :source_mask, CAST(:source_text AS jsonb), :list_version, :checked_at, :is_enabled)
            ON CONFLICT (domain) DO UPDATE
            SET
                source_mask = {_ADULT_STAGING_TABLE}.source_mask | EXCLUDED.source_mask,
                source_text = EXCLUDED.source_text,
                list_version = EXCLUDED.list_version,
                checked_at = EXCLUDED.checked_at,
                is_enabled = EXCLUDED.is_enabled,
                category = EXCLUDED.category
            """
        ),
        prepared_rows,
    )


async def _bulk_insert_staging_domains(
    db: AsyncSession,
    domains: list[str],
    *,
    version: str,
    checked_at: datetime,
) -> None:
    if not domains:
        return

    # Defensive filter for malformed rows that may trigger asyncpg ProgrammingError
    domains = [d for d in domains if _is_domain_db_safe(d)]
    if not domains:
        return

    stmt = text(
        f"""
        INSERT INTO {_ADULT_STAGING_TABLE}
            (domain, category, source_mask, source_text, list_version, checked_at, is_enabled)
        SELECT
            d.domain,
            'adult',
            :source_mask,
            CAST(:source_text AS jsonb),
            :list_version,
            :checked_at,
            TRUE
        FROM unnest(:domains) AS d(domain)
        ON CONFLICT (domain) DO NOTHING
        """
    ).bindparams(bindparam("domains", type_=ARRAY(String())))

    params = {
        "domains": domains,
        "source_mask": int(SOURCE_TXT_IMPORT),
        "source_text": json.dumps(["txt_import"]),
        "list_version": version,
        "checked_at": checked_at,
    }

    try:
        await db.execute(stmt, params)
    except SQLAlchemyError as exc:
        logger.warning("adult txt sync: fast array insert failed, fallback to executemany: %s", exc)
        fallback_rows = [
            {
                "domain": domain,
                "category": "adult",
                "source_mask": int(SOURCE_TXT_IMPORT),
                "source_text": ["txt_import"],
                "list_version": version,
                "checked_at": checked_at,
                "is_enabled": True,
            }
            for domain in domains
        ]
        await _bulk_insert_staging_rows(db, fallback_rows)


async def _merge_staging_into_catalog(db: AsyncSession, version: str) -> int:
    total_merged = 0
    while True:
        row = None
        for attempt in range(1, ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS + 1):
            try:
                res = await db.execute(
                    text(
                        f"""
                        WITH picked AS (
                            SELECT ctid, domain, category, source_mask, source_text, checked_at
                            FROM {_ADULT_STAGING_TABLE}
                            ORDER BY domain
                            LIMIT :chunk_size
                        ),
                        ins AS (
                            INSERT INTO adult_domain_catalog (domain, category, source_mask, source_text, list_version, checked_at, is_enabled)
                            SELECT p.domain, p.category, p.source_mask, p.source_text, :version, p.checked_at, TRUE
                            FROM picked p
                            ON CONFLICT (domain) DO UPDATE
                            SET
                                source_mask = adult_domain_catalog.source_mask | EXCLUDED.source_mask,
                                source_text = EXCLUDED.source_text,
                                list_version = EXCLUDED.list_version,
                                checked_at = EXCLUDED.checked_at,
                                is_enabled = EXCLUDED.is_enabled,
                                category = EXCLUDED.category
                            WHERE
                                adult_domain_catalog.is_enabled IS DISTINCT FROM TRUE
                                OR adult_domain_catalog.list_version IS DISTINCT FROM EXCLUDED.list_version
                                OR adult_domain_catalog.source_mask IS DISTINCT FROM (adult_domain_catalog.source_mask | EXCLUDED.source_mask)
                                OR adult_domain_catalog.source_text::text IS DISTINCT FROM EXCLUDED.source_text::text
                                OR adult_domain_catalog.category IS DISTINCT FROM EXCLUDED.category
                            RETURNING 1
                        ),
                        del_rows AS (
                            DELETE FROM {_ADULT_STAGING_TABLE} s
                            USING picked p
                            WHERE s.ctid = p.ctid
                            RETURNING 1
                        )
                        SELECT
                            (SELECT COUNT(*) FROM picked) AS picked_count,
                            (SELECT COUNT(*) FROM ins) AS merged_count
                        """
                    ),
                    {
                        "version": version,
                        "chunk_size": ADULT_SYNC_DB_MERGE_CHUNK_SIZE,
                    },
                )
                row = res.first()
                await db.commit()
                break
            except Exception as exc:
                await db.rollback()
                if _msg_has_retryable_db_error(exc) and attempt < ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS:
                    sleep_sec = (ADULT_SYNC_DB_RETRY_BASE_SLEEP_MS / 1000.0) * attempt
                    logger.warning("adult merge chunk retry %s/%s: %s", attempt, ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS, exc)
                    await asyncio.sleep(sleep_sec)
                    continue
                raise
        if row is None:
            break
        picked_count = int(row.picked_count or 0)
        merged_count = int(row.merged_count or 0)
        if picked_count <= 0:
            break
        total_merged += merged_count
    return total_merged


async def _disable_stale_catalog_rows(db: AsyncSession, *, version: str) -> int:
    total_disabled = 0
    while True:
        row = None
        for attempt in range(1, ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS + 1):
            try:
                res = await db.execute(
                    text(
                        """
                        WITH picked AS (
                            SELECT ctid
                            FROM adult_domain_catalog
                            WHERE is_enabled IS TRUE AND list_version <> :version
                            LIMIT :chunk_size
                            FOR UPDATE SKIP LOCKED
                        ),
                        upd AS (
                            UPDATE adult_domain_catalog c
                            SET
                                is_enabled = FALSE,
                                source_mask = 0,
                                source_text = '[]'::jsonb
                            FROM picked
                            WHERE c.ctid = picked.ctid
                            RETURNING 1
                        )
                        SELECT (SELECT COUNT(*) FROM upd) AS disabled_count
                        """
                    ),
                    {
                        "version": version,
                        "chunk_size": ADULT_SYNC_DB_STALE_CHUNK_SIZE,
                    },
                )
                row = res.first()
                await db.commit()
                break
            except Exception as exc:
                await db.rollback()
                if _msg_has_retryable_db_error(exc) and attempt < ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS:
                    sleep_sec = (ADULT_SYNC_DB_RETRY_BASE_SLEEP_MS / 1000.0) * attempt
                    logger.warning("adult stale disable chunk retry %s/%s: %s", attempt, ADULT_SYNC_DB_RETRY_MAX_ATTEMPTS, exc)
                    await asyncio.sleep(sleep_sec)
                    continue
                raise
        if row is None:
            break
        disabled = int(row.disabled_count or 0)
        if disabled <= 0:
            break
        total_disabled += disabled
    return total_disabled


async def _replace_bucket_table_domains(
    db: AsyncSession,
    *,
    table_name: str,
    domains: list[str],
    version: str,
    checked_at: datetime,
) -> int:
    await db.execute(text(f"TRUNCATE TABLE {table_name}"))
    if not domains:
        return 0

    inserted = 0
    stmt = text(
        f"""
        INSERT INTO {table_name} (domain, list_version, checked_at)
        SELECT d.domain, :list_version, :checked_at
        FROM unnest(:domains) AS d(domain)
        """
    ).bindparams(bindparam("domains", type_=ARRAY(String())))

    for start, end in _iter_batch_slices(len(domains), ADULT_SYNC_TXT_DB_CHUNK_SIZE):
        chunk = domains[start:end]
        if not chunk:
            continue
        await db.execute(
            stmt,
            {
                "domains": chunk,
                "list_version": version,
                "checked_at": checked_at,
            },
        )
        inserted += len(chunk)
    return inserted


async def _sync_bucket_tables(
    db: AsyncSession,
    *,
    version: str,
    checked_at: datetime,
    bucket_files: dict[str, str],
) -> dict[str, int]:
    results: dict[str, int] = {}
    for file_name, table_name in ADULT_BUCKET_FILE_TO_TABLE.items():
        path_raw = bucket_files.get(file_name)
        path_obj = Path(path_raw) if path_raw else (ADULT_SYNC_BUCKETS_DIR / file_name)
        domains: list[str] = []
        if path_obj.exists():
            with path_obj.open("r", encoding="utf-8", errors="replace") as fp:
                for line in fp:
                    value = line.strip().lower()
                    if not value:
                        continue
                    if len(value) > 253:
                        continue
                    domains.append(value)
        deduped = sorted(set(domains))
        results[file_name] = await _replace_bucket_table_domains(
            db,
            table_name=table_name,
            domains=deduped,
            version=version,
            checked_at=checked_at,
        )
    return results



def _is_ip_literal(value: str | None) -> bool:
    if not value:
        return False
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _sanitize_token(token: str) -> str:
    t = token.strip().strip()
    if not t:
        return ""

    if t.startswith("@@||"):
        t = t[3:]
    elif t.startswith("||"):
        t = t[2:]

    t = t.lstrip("|")
    if t.startswith("*."):
        t = t[2:]
    if t.startswith("www."):
        t = t[4:]

    for sep in (" #", "\t#", " ;", " //", "|"):
        if sep in t:
            t = t.split(sep, 1)[0].strip()

    if "^" in t:
        t = t.split("^", 1)[0]

    t = t.replace("[.]", ".")
    t = t.replace("(dot)", ".")

    t = t.split("?")[0].split("#", 1)[0]
    t = t.split("/", 1)[0]

    return t.strip()


def normalize_remnawave_domain(value: str | None) -> str | None:
    if not value:
        return None

    raw = _sanitize_token(value)
    if not raw:
        return None

    host = raw.strip().strip(".").lower()
    if not host:
        return None

    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]

    # support raw host without scheme
    if "://" in host:
        host = host.split("://", 1)[1]

    host = host.split("/")[0].split("?")[0].split("#", 1)[0]
    if ":" in host and host.count(":") == 1 and host.rsplit(":", 1)[1].isdigit():
        host = host.rsplit(":", 1)[0]

    host = host.strip(".")
    if not host:
        return None

    if _is_ip_literal(host):
        return None

    try:
        host = host.encode("idna").decode("ascii")
    except Exception:
        return None

    if host.startswith(".") or host.endswith("."):
        return None
    if host.startswith("-") or host.endswith("-"):
        return None

    parts = host.split(".")
    if len(parts) < 2:
        return None
    if any(not p or p.startswith("-") or p.endswith("-") for p in parts):
        return None

    if tldextract is not None and ADULT_SYNC_USE_TLDEXTRACT:
        extracted = tldextract.extract(host)
        if extracted.domain and extracted.suffix:
            root = f"{extracted.domain}.{extracted.suffix}".lower()
            if root and not _is_ip_literal(root):
                return root

    for suffix in sorted(KNOWN_E_TLD_SUFFIXES, key=len, reverse=True):
        if host == suffix:
            return None
        if host.endswith(f".{suffix}"):
            base = host[: -(len(suffix) + 1)]
            if base:
                label = base.rsplit(".", 1)[-1]
                if label and not label.startswith("-") and not label.endswith("-"):
                    return f"{label}.{suffix}"

    return f"{parts[-2]}.{parts[-1]}"




def _match_candidate_domains(value: str | None) -> list[str]:
    """Return possible catalog/exclusion domain candidates for a DNS value.

    Supports historical rows where dns_root may still contain a full host
    and/or prefixed domains like 0-0-...-example.com.
    """
    if not value:
        return []
    raw = str(value).strip().lower().strip('.')
    if not raw:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def _push(v: str | None) -> None:
        if not v:
            return
        vv = v.strip().lower().strip('.')
        if not vv or vv in seen:
            return
        if not _is_domain_db_safe(vv):
            return
        seen.add(vv)
        candidates.append(vv)

    # 1) canonical normalized root
    _push(normalize_remnawave_domain(raw))

    # 2) canonicalized current value (strips numeric-prefix patterns)
    _push(_canonical_domain_for_match(raw))

    # 3) specific -> generic suffix chain
    parts = raw.split('.')
    if len(parts) >= 2:
        for i in range(0, len(parts) - 1):
            suffix = '.'.join(parts[i:])
            _push(suffix)
            _push(_canonical_domain_for_match(suffix))

    return candidates

def _canonical_domain_for_match(domain: str | None) -> str | None:
    if not domain:
        return None
    raw = str(domain).strip().lower().strip('.')
    if not raw:
        return None

    parts = raw.split('.', 1)
    if len(parts) == 2:
        first, rest = parts
        # strip one or many leading numeric prefixes: 0-foo, 0-0-foo, 127-0-0-1-foo
        stripped_first = re.sub(r"^(?:\d+-)+", "", first)
        if stripped_first and stripped_first != first:
            candidate = f"{stripped_first}.{rest}"
            if _is_domain_db_safe(candidate):
                return candidate

    if _is_domain_db_safe(raw):
        return raw
    return None


def _is_domain_db_safe(domain: str) -> bool:
    if not domain:
        return False
    if len(domain) > 253:
        return False
    labels = domain.split(".")
    if len(labels) < 2:
        return False
    for lbl in labels:
        if not lbl or len(lbl) > 63:
            return False
    return True


def _chunk_normalized_domains(lines: list[str], chunk_size: int) -> tuple[list[list[str]], int]:
    """Utility helper for tests: normalize, validate, dedupe inside each chunk."""
    size = max(1, int(chunk_size or 1))
    chunks: list[list[str]] = []
    current: list[str] = []
    seen: set[str] = set()
    skipped_invalid = 0

    for raw_line in lines:
        domain = normalize_remnawave_domain(raw_line)
        if not domain or not _is_domain_db_safe(domain):
            skipped_invalid += 1
            continue
        if domain in seen:
            continue
        seen.add(domain)
        current.append(domain)
        if len(current) >= size:
            chunks.append(current)
            current = []
            seen = set()

    if current:
        chunks.append(current)
    return chunks, skipped_invalid


def _sources_from_mask(mask: int) -> list[str]:
    out: list[str] = []
    for bit, name in sorted(SOURCE_LABELS.items()):
        if mask & bit:
            out.append(name)
    return out


async def _emit_progress(progress_cb: ProgressCallback | None, **payload) -> None:
    if progress_cb is None:
        return
    try:
        maybe = progress_cb(payload)
        if asyncio.iscoroutine(maybe):
            await maybe
    except Exception:
        logger.debug("adult progress callback failed", exc_info=True)


def _weekday_label(weekday: int) -> str:
    names = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    return names[max(0, min(6, weekday))]


async def get_adult_sync_schedule() -> dict:
    global _schedule_cache
    async with _SCHEDULE_LOCK:
        if _schedule_cache is not None:
            return dict(_schedule_cache)

        schedule = {
            "weekday": ADULT_SYNC_WEEKDAY_DEFAULT,
            "hour": ADULT_SYNC_HOUR_DEFAULT,
            "minute": ADULT_SYNC_MINUTE_DEFAULT,
            "source": "env/default",
        }
        try:
            if await _ensure_adult_schema():
                async with async_session() as db:
                    state = await db.get(AdultSyncState, ADULT_SYNC_SCHEDULE_JOB)
                if state and isinstance(state.stats_json, dict):
                    stats = state.stats_json
                    weekday = int(stats.get("weekday", schedule["weekday"]) or schedule["weekday"])
                    hour = int(stats.get("hour", schedule["hour"]) or schedule["hour"])
                    minute = int(stats.get("minute", schedule["minute"]) or schedule["minute"])
                    schedule = {
                        "weekday": max(0, min(6, weekday)),
                        "hour": max(0, min(23, hour)),
                        "minute": max(0, min(59, minute)),
                        "source": "db",
                    }
        except Exception:
            logger.warning("adult schedule read failed; fallback to defaults", exc_info=True)

        _schedule_cache = schedule
        return dict(schedule)


async def set_adult_sync_schedule(*, weekday: int, hour: int, minute: int) -> dict:
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be in range 0..6")
    if hour < 0 or hour > 23:
        raise ValueError("hour must be in range 0..23")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be in range 0..59")
    if not await _ensure_adult_schema():
        raise RuntimeError("adult schema missing")

    stats = {"weekday": int(weekday), "hour": int(hour), "minute": int(minute)}
    async with async_session() as db:
        stmt = pg_insert(AdultSyncState).values(
            job_name=ADULT_SYNC_SCHEDULE_JOB,
            last_run_at=datetime.now(timezone.utc),
            last_watermark=f"{weekday}:{hour:02d}:{minute:02d}",
            status="ok",
            stats_json=stats,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[AdultSyncState.job_name],
            set_={
                "last_run_at": stmt.excluded.last_run_at,
                "last_watermark": stmt.excluded.last_watermark,
                "status": stmt.excluded.status,
                "stats_json": stmt.excluded.stats_json,
            },
        )
        await db.execute(stmt)
        await db.commit()

    global _schedule_cache
    _schedule_cache = {"weekday": weekday, "hour": hour, "minute": minute, "source": "db"}
    return {
        "weekday": weekday,
        "hour": hour,
        "minute": minute,
        "weekday_label": _weekday_label(weekday),
        "source": "db",
    }


def get_adult_sync_runtime_state() -> dict:
    state = dict(_bg_runtime_state)
    schedule = state.get("schedule")
    if isinstance(schedule, dict):
        state["schedule"] = dict(schedule)
    return state


def _iter_line_tokens(raw_line: str) -> list[str]:
    raw = raw_line.strip()
    if not raw or raw[0] in {"#", "!", ";"}:
        return []
    if re.match(r"^(?:0\.0\.0\.0|127\.0\.0\.1|::1)\s+", raw):
        return raw.split()[1:]
    return [token for token in re.split(r"[\s,]+", raw) if token]


def _bucket_file_for_domain(domain: str) -> str:
    ch = (domain.strip().lower()[:1] if domain else "")
    if not ch:
        return "old.txt"
    if ch.isdigit():
        return "0-9.txt"
    if "a" <= ch <= "d":
        return "a-d.txt"
    if "e" <= ch <= "h":
        return "e-h.txt"
    if "i" <= ch <= "l":
        return "i-l.txt"
    if "m" <= ch <= "p":
        return "m-p.txt"
    if "q" <= ch <= "t":
        return "q-t.txt"
    if "u" <= ch <= "x":
        return "u-x.txt"
    if "y" <= ch <= "z":
        return "y-z.txt"
    return "old.txt"


def _bucket_table_name_for_domain(domain: str) -> str:
    bucket_file = _bucket_file_for_domain(domain)
    return ADULT_BUCKET_FILE_TO_TABLE.get(bucket_file, ADULT_BUCKET_FILE_TO_TABLE["old.txt"])


def _bucket_table_names_for_candidates(candidates: list[str]) -> list[str]:
    if not candidates:
        return [ADULT_BUCKET_FILE_TO_TABLE["old.txt"]]
    names: set[str] = set()
    for candidate in candidates:
        bucket_file = _bucket_file_for_domain(candidate)
        names.add(ADULT_BUCKET_FILE_TO_TABLE.get(bucket_file, ADULT_BUCKET_FILE_TO_TABLE["old.txt"]))
    names.add(ADULT_BUCKET_FILE_TO_TABLE["old.txt"])
    ordered = [table for table in ADULT_BUCKET_FILE_TO_TABLE.values() if table in names]
    return ordered or [ADULT_BUCKET_FILE_TO_TABLE["old.txt"]]


async def _find_catalog_match_in_bucket_tables(db: AsyncSession, candidates: list[str]) -> tuple[int, str | None] | None:
    if not candidates:
        return None
    table_names = _bucket_table_names_for_candidates(candidates)
    for table_name in table_names:
        model = _ADULT_BUCKET_TABLE_MODEL_BY_NAME.get(table_name)
        if model is None:
            continue
        match = (
            await db.execute(
                select(model.list_version)
                .where(model.domain.in_(candidates))
                .order_by(desc(model.checked_at), desc(model.list_version))
                .limit(1)
            )
        ).first()
        if match is not None:
            return SOURCE_TXT_IMPORT, match[0]
    return None


def _extract_oisd_txt_links(html: str, base_url: str) -> list[str]:
    raw_links = re.findall(r"href=\"([^\"]+)\"|href='([^']+)'", html)
    urls = set()
    for a, b in raw_links:
        raw = (a or b or "").strip()
        if not raw:
            continue

        raw = re.sub(r"</a>$", "", raw, flags=re.IGNORECASE)
        if ".txt" not in raw.lower():
            continue

        full = urljoin(base_url, raw)
        if full.lower().endswith(".txt"):
            urls.add(full)

    return sorted(urls)


def _extract_v2fly_includes(text_value: str) -> list[str]:
    include_urls = []
    for line in text_value.splitlines():
        s = line.strip()
        if not s.lower().startswith("include:"):
            continue

        val = s.split(":", 1)[1].strip()
        if not val:
            continue
        if val.startswith("http://") or val.startswith("https://"):
            include_urls.append(val)
        else:
            include_urls.append(f"https://raw.githubusercontent.com/v2fly/domain-list-community/master/data/{val}")
    return include_urls


def _extract_domains_and_invalid_tokens(text_value: str) -> tuple[set[str], set[str]]:
    domains: set[str] = set()
    invalid: set[str] = set()

    for raw_line in text_value.splitlines():
        for token in _iter_line_tokens(raw_line):
            root = normalize_remnawave_domain(token)
            if root and _is_domain_db_safe(root):
                domains.add(root)
                continue

            cleaned = _sanitize_token(token).strip().lower().strip(".")
            if cleaned and any(ch.isalnum() for ch in cleaned) and len(cleaned) <= 253:
                invalid.add(cleaned)

    return domains, invalid


def _safe_source_filename(url: str) -> str:
    parsed = urlparse(url)
    base = f"{parsed.netloc}{parsed.path}".strip() or "source"
    base = re.sub(r"[^a-zA-Z0-9._-]+", "_", base).strip("_")
    if not base:
        base = "source"
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"{base[:120]}_{digest}.txt"


def _load_source_manifest(path: Path) -> dict:
    if not path.exists():
        return {"sources": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("sources"), dict):
            return payload
    except Exception:
        logger.warning("adult sync: failed to parse source manifest %s", path, exc_info=True)
    return {"sources": {}}


def _save_source_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


async def _request_with_retry(
    *,
    client: "httpx.AsyncClient",
    method: str,
    url: str,
    max_retries: int,
    headers: dict[str, str] | None = None,
    allowed_statuses: set[int] | None = None,
) -> "httpx.Response":
    last_error: Exception | None = None
    accepted = allowed_statuses or {200}
    for _ in range(max(1, max_retries)):
        try:
            resp = await client.request(method, url, follow_redirects=True, headers=headers)
            if resp.status_code not in accepted:
                raise RuntimeError(f"{url} -> HTTP {resp.status_code}")
            return resp
        except Exception as exc:  # pragma: no cover
            last_error = exc
            await asyncio.sleep(0.7)
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


async def _fetch_cached_source_file(
    *,
    client: "httpx.AsyncClient",
    manifest: dict,
    url: str,
    source_mask: int,
    parse_domains: bool,
    max_retries: int,
) -> dict:
    sources_manifest = manifest.setdefault("sources", {})
    entry = sources_manifest.get(url) if isinstance(sources_manifest.get(url), dict) else {}
    file_name = str(entry.get("file_name") or _safe_source_filename(url))
    file_path = ADULT_SYNC_SOURCES_DIR / file_name

    cached_etag = str(entry.get("etag") or "").strip()
    cached_last_modified = str(entry.get("last_modified") or "").strip()
    cond_headers: dict[str, str] = {}
    if cached_etag:
        cond_headers["If-None-Match"] = cached_etag
    if cached_last_modified:
        cond_headers["If-Modified-Since"] = cached_last_modified

    get_resp = await _request_with_retry(
        client=client,
        method="GET",
        url=url,
        max_retries=max_retries,
        headers=cond_headers or None,
        allowed_statuses={200, 304},
    )

    changed = True
    remote_meta = {
        "content_length": None,
        "etag": get_resp.headers.get("etag"),
        "last_modified": get_resp.headers.get("last-modified"),
    }
    content_length = get_resp.headers.get("content-length")
    if content_length and content_length.isdigit():
        remote_meta["content_length"] = int(content_length)

    if get_resp.status_code == 304 and file_path.exists():
        payload = file_path.read_bytes()
        local_sha = hashlib.sha256(payload).hexdigest()
        expected_sha = str(entry.get("sha256") or "").strip()
        if expected_sha and local_sha != expected_sha:
            logger.warning("adult sync: cached hash mismatch for %s, forcing re-download", url)
            forced_resp = await _request_with_retry(
                client=client,
                method="GET",
                url=url,
                max_retries=max_retries,
                allowed_statuses={200},
            )
            payload = forced_resp.content
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(payload)
            changed = True
            remote_meta["etag"] = forced_resp.headers.get("etag") or remote_meta.get("etag")
            remote_meta["last_modified"] = forced_resp.headers.get("last-modified") or remote_meta.get("last_modified")
            forced_cl = forced_resp.headers.get("content-length")
            if forced_cl and forced_cl.isdigit():
                remote_meta["content_length"] = int(forced_cl)
        else:
            changed = False
    else:
        payload = get_resp.content
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(payload)

    sha256 = hashlib.sha256(payload).hexdigest()
    local_size = len(payload)
    text_value = payload.decode("utf-8", errors="replace")

    sources_manifest[url] = {
        "url": url,
        "file_name": file_name,
        "path": str(file_path),
        "size": local_size,
        "sha256": sha256,
        "etag": remote_meta.get("etag") or entry.get("etag"),
        "last_modified": remote_meta.get("last_modified") or entry.get("last_modified"),
        "source_mask": int(source_mask),
        "parse_domains": bool(parse_domains),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "url": url,
        "path": str(file_path),
        "source_mask": int(source_mask),
        "parse_domains": bool(parse_domains),
        "changed": bool(changed),
        "text": text_value,
        "size": local_size,
    }


def _write_bucket_files(bucket_map: dict[str, set[str]]) -> dict[str, str]:
    ADULT_SYNC_BUCKETS_DIR.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    for file_name in ADULT_BUCKET_FILE_TO_TABLE:
        rows = sorted(bucket_map.get(file_name, set()))
        out_path = ADULT_SYNC_BUCKETS_DIR / file_name
        if rows:
            out_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        else:
            out_path.write_text("", encoding="utf-8")
        written[file_name] = str(out_path)
    return written


async def _parse_source_file_sequential(
    *,
    source_path: str,
    source_url: str,
    source_mask: int,
    source_index: int,
    source_total: int,
    domain_map: dict[str, int],
    bucket_map: dict[str, set[str]],
    progress_cb: ProgressCallback | None = None,
) -> tuple[int, int, int]:
    parsed_lines = 0
    parsed_chunks = 0
    invalid_tokens = 0
    path = Path(source_path)
    if not path.exists():
        return parsed_lines, parsed_chunks, invalid_tokens

    lines_buffer: list[str] = []

    def _process_lines(lines: list[str]) -> tuple[int, int, int]:
        nonlocal parsed_lines, parsed_chunks, invalid_tokens
        if not lines:
            return 0, 0, 0
        valid_domains, invalid = _extract_domains_and_invalid_tokens("".join(lines))
        for domain in valid_domains:
            domain_map[domain] = domain_map.get(domain, 0) | int(source_mask)
            bucket_map[_bucket_file_for_domain(domain)].add(domain)
        for token in invalid:
            bucket_map["old.txt"].add(token)
        parsed_lines += len(lines)
        parsed_chunks += 1
        invalid_tokens += len(invalid)
        return len(valid_domains), len(invalid), len(lines)

    with path.open("r", encoding="utf-8", errors="replace") as fp:
        for line in fp:
            lines_buffer.append(line)
            if len(lines_buffer) >= ADULT_SYNC_PARSE_LINE_CHUNK_SIZE:
                valid_count, invalid_count, line_count = _process_lines(lines_buffer)
                lines_buffer = []
                await _emit_progress(
                    progress_cb,
                    phase="parse",
                    progress_current=max(0, source_index - 1),
                    progress_total=max(source_total, 1),
                    progress_percent=38.0 + (max(0, source_index - 1) / max(source_total, 1)) * 22.0,
                    message=(
                        f"Parsing [{source_index}/{source_total}] {source_url} | "
                        f"chunk={parsed_chunks} lines+={line_count} domains+={valid_count} invalid+={invalid_count}"
                    ),
                )
    if lines_buffer:
        valid_count, invalid_count, line_count = _process_lines(lines_buffer)
        await _emit_progress(
            progress_cb,
            phase="parse",
            progress_current=max(0, source_index - 1),
            progress_total=max(source_total, 1),
            progress_percent=38.0 + (max(0, source_index - 1) / max(source_total, 1)) * 22.0,
            message=(
                f"Parsing [{source_index}/{source_total}] {source_url} | "
                f"chunk={parsed_chunks} lines+={line_count} domains+={valid_count} invalid+={invalid_count}"
            ),
        )

    await _emit_progress(
        progress_cb,
        phase="parse",
        progress_current=source_index,
        progress_total=max(source_total, 1),
        progress_percent=38.0 + (source_index / max(source_total, 1)) * 22.0,
        message=f"Parsed source [{source_index}/{source_total}] {source_url}",
    )
    return parsed_lines, parsed_chunks, invalid_tokens


async def collect_adult_domain_map(
    timeout: int = 25,
    max_retries: int = 4,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    domain_map: dict[str, int] = {}
    bucket_map: dict[str, set[str]] = {name: set() for name in ADULT_BUCKET_FILE_TO_TABLE}
    manifest = _load_source_manifest(ADULT_SYNC_SOURCE_MANIFEST_PATH)
    sources: list[dict] = []
    failed: list[str] = []
    changed_count = 0
    processed_sources = 0

    async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "NanoRed/RemnawaveAuditor"}) as client:
        async def _fetch(url: str, source_mask: int, parse_domains: bool) -> dict | None:
            nonlocal changed_count, processed_sources
            try:
                await _emit_progress(
                    progress_cb,
                    phase="collect",
                    progress_current=processed_sources,
                    progress_total=max(processed_sources + 1, 1),
                    progress_percent=min(35.0, 2.0 + processed_sources * 0.2),
                    message=f"Collecting source: {url}",
                )
                item = await _fetch_cached_source_file(
                    client=client,
                    manifest=manifest,
                    url=url,
                    source_mask=source_mask,
                    parse_domains=parse_domains,
                    max_retries=max_retries,
                )
                if item.get("changed"):
                    changed_count += 1
                sources.append(
                    {
                        "url": item.get("url"),
                        "path": item.get("path"),
                        "source_mask": int(item.get("source_mask") or 0),
                        "parse_domains": bool(item.get("parse_domains")),
                        "changed": bool(item.get("changed")),
                        "size": int(item.get("size") or 0),
                    }
                )
                processed_sources += 1
                logger.info(
                    "adult sync: source processed %s changed=%s size=%s",
                    url,
                    bool(item.get("changed")),
                    int(item.get("size") or 0),
                )
                await _emit_progress(
                    progress_cb,
                    phase="collect",
                    progress_current=processed_sources,
                    progress_total=max(processed_sources, 1),
                    progress_percent=min(38.0, 3.0 + processed_sources * 0.25),
                    message=f"Source done: {url}",
                )
                return item
            except Exception as exc:
                failed.append(url)
                logger.warning("adult sync: source failed %s: %s", url, exc)
                await _emit_progress(
                    progress_cb,
                    phase="collect",
                    progress_current=processed_sources,
                    progress_total=max(processed_sources + len(failed), 1),
                    progress_percent=min(38.0, 3.0 + processed_sources * 0.2),
                    message=f"Source failed: {url} ({exc})",
                    status="running",
                )
                return None

        await _fetch(BLOCKLIST_URL, SOURCE_BLOCKLIST, True)

        queue = [V2FLY_ROOT_URL]
        seen = set(queue)
        while queue:
            url = queue.pop(0)
            item = await _fetch(url, SOURCE_V2FLY, True)
            if not item:
                continue
            for inc in _extract_v2fly_includes(item.get("text") or ""):
                if inc not in seen:
                    seen.add(inc)
                    queue.append(inc)

        oisd_root = await _fetch(OISD_ROOT_URL, SOURCE_OISD, False)
        if oisd_root:
            for u in _extract_oisd_txt_links(oisd_root.get("text") or "", OISD_ROOT_URL):
                await _fetch(u, SOURCE_OISD, True)

    _save_source_manifest(ADULT_SYNC_SOURCE_MANIFEST_PATH, manifest)

    parse_sources = [s for s in sources if bool(s.get("parse_domains"))]
    total_parse_sources = len(parse_sources)
    parsed_lines_total = 0
    parsed_chunks_total = 0
    invalid_tokens_total = 0

    for idx, item in enumerate(parse_sources, start=1):
        parsed_lines, parsed_chunks, invalid_tokens = await _parse_source_file_sequential(
            source_path=str(item.get("path") or ""),
            source_url=str(item.get("url") or ""),
            source_mask=int(item.get("source_mask") or 0),
            source_index=idx,
            source_total=total_parse_sources,
            domain_map=domain_map,
            bucket_map=bucket_map,
            progress_cb=progress_cb,
        )
        parsed_lines_total += parsed_lines
        parsed_chunks_total += parsed_chunks
        invalid_tokens_total += invalid_tokens

    bucket_files = _write_bucket_files(bucket_map)
    bucket_counts = {name: len(values) for name, values in bucket_map.items()}

    return {
        "domain_map": domain_map,
        "bucket_files": bucket_files,
        "bucket_counts": bucket_counts,
        "source_files_total": len(sources),
        "source_files_changed": changed_count,
        "source_files_unchanged": max(0, len(sources) - changed_count),
        "source_files_failed": failed,
        "parse_sources_total": total_parse_sources,
        "parse_lines_total": parsed_lines_total,
        "parse_chunks_total": parsed_chunks_total,
        "parse_invalid_total": invalid_tokens_total,
        "manifest_path": str(ADULT_SYNC_SOURCE_MANIFEST_PATH),
    }


async def _upsert_sync_state(*, status: str, stats: dict, version: str, last_watermark: str | None = None) -> None:
    async with async_session() as db:
        stmt = pg_insert(AdultSyncState).values(
            job_name=ADULT_SYNC_JOB,
            last_run_at=datetime.now(timezone.utc),
            last_watermark=last_watermark or version,
            status=status,
            stats_json=stats,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[AdultSyncState.job_name],
            set_={
                "last_run_at": stmt.excluded.last_run_at,
                "last_watermark": stmt.excluded.last_watermark,
                "status": stmt.excluded.status,
                "stats_json": stmt.excluded.stats_json,
            },
        )
        await db.execute(stmt)
        await db.commit()


async def sync_adult_catalog(progress_cb: ProgressCallback | None = None) -> dict:
    if not await _ensure_adult_schema():
        return {
            "version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "domains": 0,
            "updated": 0,
            "status": "schema_missing",
        }

    async with _CATALOG_SYNC_LOCK:
        return await _sync_adult_catalog_internal(progress_cb=progress_cb)


async def _sync_adult_catalog_internal(progress_cb: ProgressCallback | None = None) -> dict:
    start = datetime.now(timezone.utc)
    version = start.strftime("%Y%m%dT%H%M%SZ")
    await _emit_progress(progress_cb, phase="collect", progress_current=0, progress_total=100, progress_percent=1.0, message="Collecting lists")
    collect_stats = await collect_adult_domain_map(progress_cb=progress_cb)
    domain_map: dict[str, int] = collect_stats.get("domain_map") or {}
    await _emit_progress(
        progress_cb,
        phase="collect",
        progress_current=len(domain_map),
        progress_total=len(domain_map),
        progress_percent=40.0,
        message=f"Collected {len(domain_map)} domains",
    )

    if not domain_map:
        stats = {
            "version": version,
            "domains": 0,
            "updated": 0,
            "status": "empty",
            "bucket_counts": collect_stats.get("bucket_counts") or {},
            "source_files_total": int(collect_stats.get("source_files_total") or 0),
            "source_files_changed": int(collect_stats.get("source_files_changed") or 0),
            "source_files_unchanged": int(collect_stats.get("source_files_unchanged") or 0),
            "source_files_failed": collect_stats.get("source_files_failed") or [],
            "manifest_path": collect_stats.get("manifest_path"),
        }
        await _upsert_sync_state(status="warn", stats=stats, version=version)
        await _emit_progress(progress_cb, phase="done", progress_current=0, progress_total=0, progress_percent=100.0, message="No domains collected", status="warn")
        return stats

    items = list(domain_map.items())
    chunk_size = ADULT_SYNC_DB_CHUNK_SIZE

    async with async_session() as db:
        await _prepare_adult_staging_table(db)
        total_items = len(items)
        processed_items = 0
        staged_batches = 0
        for start_idx, end_idx in _iter_batch_slices(len(items), chunk_size):
            chunk = items[start_idx:end_idx]
            if not chunk:
                continue
            rows = []
            for domain, mask in chunk:
                rows.append(
                    {
                        "domain": domain,
                        "category": "adult",
                        "source_mask": int(mask),
                        "source_text": _sources_from_mask(mask),
                        "list_version": version,
                        "checked_at": start,
                        "is_enabled": True,
                    }
                )
            await _bulk_insert_staging_rows(db, rows)
            staged_batches += 1
            if staged_batches % ADULT_SYNC_TXT_DB_COMMIT_EVERY == 0:
                await db.commit()
            processed_items += len(chunk)
            await _emit_progress(
                progress_cb,
                phase="stage",
                progress_current=processed_items,
                progress_total=total_items,
                progress_percent=40.0 + (processed_items / max(total_items, 1)) * 45.0,
                message=f"Prepared {processed_items}/{total_items}",
            )
        await db.commit()

        await _emit_progress(progress_cb, phase="bucket_sync", progress_current=0, progress_total=1, progress_percent=86.0, message="Refreshing bucket tables")
        bucket_table_rows = await _sync_bucket_tables(
            db,
            version=version,
            checked_at=start,
            bucket_files=collect_stats.get("bucket_files") or {},
        )
        await db.commit()

        merged_total = await _merge_staging_into_catalog(db, version=version)
        await _emit_progress(progress_cb, phase="merge", progress_current=merged_total, progress_total=max(merged_total, 1), progress_percent=93.0, message=f"Merged into catalog: {merged_total}")

        disabled_total = await _disable_stale_catalog_rows(db, version=version)
        await _emit_progress(progress_cb, phase="cleanup", progress_current=disabled_total, progress_total=max(disabled_total, 1), progress_percent=96.0, message=f"Disabled stale rows: {disabled_total}")

        marked_for_recheck = await _mark_matching_domains_for_recheck(db=db, version=version)
        await db.commit()
        await _emit_progress(progress_cb, phase="recheck_mark", progress_current=marked_for_recheck, progress_total=max(marked_for_recheck, 1), progress_percent=99.0, message=f"Marked for recheck: {marked_for_recheck}")

        stats = {
            "version": version,
            "domains": len(domain_map),
            "updated": marked_for_recheck,
            "merged_total": merged_total,
            "disabled_stale": disabled_total,
            "status": "ok",
            "bucket_counts": collect_stats.get("bucket_counts") or {},
            "bucket_table_rows": bucket_table_rows,
            "source_files_total": int(collect_stats.get("source_files_total") or 0),
            "source_files_changed": int(collect_stats.get("source_files_changed") or 0),
            "source_files_unchanged": int(collect_stats.get("source_files_unchanged") or 0),
            "source_files_failed": collect_stats.get("source_files_failed") or [],
            "manifest_path": collect_stats.get("manifest_path"),
            "bucket_files": collect_stats.get("bucket_files") or {},
        }
        await _upsert_sync_state(status="ok", stats=stats, version=version, last_watermark=str(start))
        await _emit_progress(progress_cb, phase="done", progress_current=1, progress_total=1, progress_percent=100.0, message="Catalog sync finished", status="ok")
        return stats


async def process_dns_unique_recheck_batch(limit: int = ADULT_RECHECK_BATCH_LIMIT, session: AsyncSession | None = None) -> int:
    if session is None:
        async with async_session() as db:
            return await _process_dns_unique_recheck_batch(db, limit=limit)
    return await _process_dns_unique_recheck_batch(session, limit=limit)


async def force_recheck_all_dns_unique(limit: int = ADULT_RECHECK_BATCH_LIMIT, progress_cb: ProgressCallback | None = None) -> dict[str, int]:
    """Force full recheck of all unique domains against current adult catalog."""
    if not await _ensure_adult_schema():
        return {
            "marked": 0,
            "processed": 0,
            "status": "schema_missing",
        }

    async with _FULL_RECHECK_LOCK:
        async with async_session() as db:
            await _emit_progress(progress_cb, phase="mark", progress_current=0, progress_total=1, progress_percent=2.0, message="Marking rows for recheck")

            already_marked = int(
                (
                    await db.execute(
                        select(func.count(RemnawaveDNSUnique.dns_root)).where(RemnawaveDNSUnique.need_recheck.is_(True))
                    )
                ).scalar()
                or 0
            )

            mark_stmt = (
                update(RemnawaveDNSUnique)
                .where(RemnawaveDNSUnique.need_recheck.is_(False))
                .values(need_recheck=True)
            )
            mark_res = await db.execute(mark_stmt)
            await db.commit()

            marked_new = int(mark_res.rowcount or 0)
            target_total = max(0, already_marked + marked_new)
            processed = 0

            await _emit_progress(
                progress_cb,
                phase="recheck",
                progress_current=processed,
                progress_total=target_total,
                progress_percent=5.0 if target_total else 100.0,
                message=f"Marked new: {marked_new}, already pending: {already_marked}",
            )

            while True:
                changed = await _process_dns_unique_recheck_batch(db, limit=limit)
                if changed <= 0:
                    break
                processed += changed
                progress_total = max(target_total, processed)
                await _emit_progress(
                    progress_cb,
                    phase="recheck",
                    progress_current=processed,
                    progress_total=progress_total,
                    progress_percent=5.0 + (processed / max(progress_total, 1)) * 95.0,
                    message=f"Processed {processed}/{progress_total}",
                )

            remaining = int((await db.execute(select(func.count(RemnawaveDNSUnique.dns_root)).where(RemnawaveDNSUnique.need_recheck.is_(True)))).scalar() or 0)
            progress_total = max(target_total, processed)
            status = "ok" if remaining == 0 else "partial"
            result = {
                "marked_new": marked_new,
                "already_marked": already_marked,
                "target_total": target_total,
                "processed": processed,
                "remaining": remaining,
                "status": status,
            }
            await _emit_progress(
                progress_cb,
                phase="done",
                progress_current=processed,
                progress_total=progress_total,
                progress_percent=100.0,
                message="Full recheck finished" if remaining == 0 else f"Full recheck finished with {remaining} remaining rows",
                status=status,
            )
            return result


async def sync_adult_catalog_from_txt(path: str | None = None, progress_cb: ProgressCallback | None = None) -> dict:
    """Sync adult catalog from TXT list and refresh bucket tables + catalog."""
    if not await _ensure_adult_schema():
        return {"status": "schema_missing", "inserted": 0, "processed": 0, "path": path or ADULT_SYNC_TXT_PATH}

    async with _TXT_SYNC_LOCK:
        txt_path = Path((path or ADULT_SYNC_TXT_PATH).strip() or ADULT_SYNC_TXT_PATH).expanduser()
        if not txt_path.exists():
            stats = {
                "status": "missing_file",
                "inserted": 0,
                "processed": 0,
                "path": str(txt_path),
            }
            await _upsert_sync_state(status="missing_file", stats=stats, version="txt-missing", last_watermark=None)
            await _emit_progress(progress_cb, phase="error", progress_current=0, progress_total=0, progress_percent=100.0, message=f"File not found: {txt_path}", status="missing_file")
            return stats

        started_at = datetime.now(timezone.utc)
        version = f"txt-{started_at.strftime('%Y%m%dT%H%M%SZ')}"
        total_bytes = max(0, int(txt_path.stat().st_size or 0))
        await _upsert_sync_state(
            status="running",
            stats={"status": "running", "path": str(txt_path), "started_at": started_at.isoformat()},
            version=version,
            last_watermark=str(started_at),
        )
        await _emit_progress(progress_cb, phase="read", progress_current=0, progress_total=total_bytes, progress_percent=1.0, message="Reading TXT list")

        domain_map: dict[str, int] = {}
        bucket_map: dict[str, set[str]] = {name: set() for name in ADULT_BUCKET_FILE_TO_TABLE}
        skipped_invalid = 0
        bytes_read = 0

        with txt_path.open("r", encoding="utf-8", errors="replace") as fp:
            for line in fp:
                bytes_read += len(line)
                token = line.strip()
                if not token:
                    continue
                domain = normalize_remnawave_domain(token)
                if not domain or not _is_domain_db_safe(domain):
                    skipped_invalid += 1
                    bucket_map["old.txt"].add(token)
                    continue
                domain_map[domain] = int(domain_map.get(domain, 0) | SOURCE_TXT_IMPORT)
                bucket_map[_bucket_file_for_domain(domain)].add(domain)
                if total_bytes > 0 and bytes_read % (256 * 1024) == 0:
                    await _emit_progress(
                        progress_cb,
                        phase="read",
                        progress_current=min(bytes_read, total_bytes),
                        progress_total=total_bytes,
                        progress_percent=1.0 + (bytes_read / total_bytes) * 70.0,
                        message=f"Parsed {len(domain_map)} domains",
                    )

        bucket_files = _write_bucket_files(bucket_map)
        processed_domains = len(domain_map)

        async with async_session() as db:
            await _prepare_adult_staging_table(db)
            await _emit_progress(progress_cb, phase="stage", progress_current=processed_domains, progress_total=max(processed_domains, 1), progress_percent=78.0, message="Refreshing bucket tables")

            bucket_table_rows = await _sync_bucket_tables(
                db,
                version=version,
                checked_at=started_at,
                bucket_files=bucket_files,
            )

            items = list(domain_map.items())
            for start_idx, end_idx in _iter_batch_slices(len(items), ADULT_SYNC_DB_CHUNK_SIZE):
                chunk = items[start_idx:end_idx]
                if not chunk:
                    continue
                rows = [
                    {
                        "domain": domain,
                        "category": "adult",
                        "source_mask": int(mask),
                        "source_text": ["txt_import"],
                        "list_version": version,
                        "checked_at": started_at,
                        "is_enabled": True,
                    }
                    for domain, mask in chunk
                ]
                await _bulk_insert_staging_rows(db, rows)
                await db.commit()

            inserted_total = await _merge_staging_into_catalog(db, version=version)
            disabled_total = await _disable_stale_catalog_rows(db, version=version)
            await db.commit()

            await _mark_matching_domains_for_recheck(db=db, version=version)

        stats = {
            "status": "ok",
            "path": str(txt_path),
            "processed": processed_domains,
            "inserted": inserted_total,
            "skipped_invalid": skipped_invalid,
            "version": version,
            "bucket_counts": {name: len(values) for name, values in bucket_map.items()},
            "bucket_table_rows": bucket_table_rows,
            "bucket_files": bucket_files,
            "disabled_stale": disabled_total,
        }
        await _upsert_sync_state(status="ok", stats=stats, version=version, last_watermark=str(started_at))
        done_msg = f"TXT sync finished ({processed_domains} domains)"
        if skipped_invalid:
            done_msg += f"; skipped invalid: {skipped_invalid}"
        await _emit_progress(progress_cb, phase="done", progress_current=processed_domains, progress_total=max(processed_domains, 1), progress_percent=100.0, message=done_msg, status="ok")
        return stats


async def _upsert_adult_catalog_rows(db: AsyncSession, rows: list[dict]) -> int:
    if not rows:
        return 0
    stmt = pg_insert(AdultDomainCatalog).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[AdultDomainCatalog.domain],
        set_={
            "source_mask": AdultDomainCatalog.source_mask.op("|")(stmt.excluded.source_mask),
            "source_text": stmt.excluded.source_text,
            "list_version": stmt.excluded.list_version,
            "checked_at": stmt.excluded.checked_at,
            "is_enabled": stmt.excluded.is_enabled,
            "category": stmt.excluded.category,
        },
        where=or_(
            AdultDomainCatalog.is_enabled.is_(False),
            AdultDomainCatalog.list_version.is_distinct_from(stmt.excluded.list_version),
            AdultDomainCatalog.source_mask.is_distinct_from(
                AdultDomainCatalog.source_mask.op("|")(stmt.excluded.source_mask)
            ),
            cast(AdultDomainCatalog.source_text, String).is_distinct_from(cast(stmt.excluded.source_text, String)),
            AdultDomainCatalog.category.is_distinct_from(stmt.excluded.category),
        ),
    )
    result = await db.execute(stmt)
    return int(result.rowcount or 0)


async def _mark_matching_domains_for_recheck(db: AsyncSession, version: str) -> int:
    """Mark matching dns_unique rows in bounded SKIP LOCKED batches.

    This avoids long-running updates that block ingest upserts and cause lock timeout.
    """
    total_marked = 0
    while True:
        if total_marked >= ADULT_MARK_RECHECK_MAX_ROWS_PER_SYNC:
            break
        chunk_size = min(
            ADULT_MARK_RECHECK_CHUNK_SIZE,
            max(1, ADULT_MARK_RECHECK_MAX_ROWS_PER_SYNC - total_marked),
        )
        res = await db.execute(
            text(
                """
                WITH picked AS (
                    SELECT u.dns_root
                    FROM remnawave_dns_unique AS u
                    WHERE
                        COALESCE(u.need_recheck, FALSE) = FALSE
                        AND (
                            u.mark_version IS NULL
                            OR u.mark_version <> :version
                            OR COALESCE(u.is_adult, FALSE) = FALSE
                        )
                    ORDER BY u.last_seen NULLS LAST, u.dns_root
                    LIMIT :chunk_size
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE remnawave_dns_unique AS u
                SET need_recheck = TRUE
                FROM picked
                WHERE u.dns_root = picked.dns_root
                RETURNING u.dns_root
                """
            ),
            {
                "version": version,
                "chunk_size": chunk_size,
            },
        )
        marked = len(res.fetchall())
        if marked <= 0:
            await db.commit()
            break
        total_marked += marked
        await db.commit()

    return total_marked


async def get_dns_unique_stats_cached(db: AsyncSession, *, force_refresh: bool = False) -> dict[str, int]:
    """Return cached dns_unique counters to avoid heavy COUNT queries on each UI request."""
    now = datetime.now(timezone.utc)
    cached: AdultSyncState | None = None
    try:
        cached = await db.get(AdultSyncState, 'dns_unique_stats')
    except Exception:
        cached = None

    if not force_refresh and cached and cached.last_run_at and cached.stats_json:
        age = (now - cached.last_run_at).total_seconds()
        if age < ADULT_UNIQUE_STATS_CACHE_TTL_SEC:
            stats = cached.stats_json or {}
            return {
                'unique_total': int(stats.get('unique_total', 0) or 0),
                'adult_total': int(stats.get('adult_total', 0) or 0),
                'need_recheck': int(stats.get('need_recheck', 0) or 0),
            }

    row = await db.execute(
        select(
            func.count(RemnawaveDNSUnique.dns_root).label('unique_total'),
            func.count(RemnawaveDNSUnique.dns_root).filter(RemnawaveDNSUnique.is_adult.is_(True)).label('adult_total'),
            func.count(RemnawaveDNSUnique.dns_root).filter(RemnawaveDNSUnique.need_recheck.is_(True)).label('need_recheck'),
        )
    )
    data = row.mappings().first() or {}
    stats = {
        'unique_total': int(data.get('unique_total', 0) or 0),
        'adult_total': int(data.get('adult_total', 0) or 0),
        'need_recheck': int(data.get('need_recheck', 0) or 0),
    }

    if cached is None:
        cached = AdultSyncState(job_name='dns_unique_stats')
        db.add(cached)
    cached.last_run_at = now
    cached.status = 'ok'
    cached.stats_json = stats
    await db.commit()
    return stats


async def cleanup_adult_catalog_garbage(progress_cb: ProgressCallback | None = None) -> dict:
    """Delete stale catalog rows and run VACUUM ANALYZE for hot tables."""
    if not await _ensure_adult_schema():
        return {"status": "schema_missing", "deleted": 0, "vacuumed_tables": []}

    async with _MAINTENANCE_LOCK:
        await _emit_progress(progress_cb, phase="delete", progress_current=0, progress_total=1, progress_percent=10.0, message="Deleting stale rows")
        cutoff = datetime.now(timezone.utc) - timedelta(days=ADULT_SYNC_GARBAGE_RETENTION_DAYS)
        deleted_rows = 0
        async with async_session() as db:
            delete_stmt = delete(AdultDomainCatalog).where(
                AdultDomainCatalog.is_enabled.is_(False),
                AdultDomainCatalog.checked_at.is_not(None),
                AdultDomainCatalog.checked_at < cutoff,
            )
            result = await db.execute(delete_stmt)
            deleted_rows = int(result.rowcount or 0)
            await db.commit()
        await _emit_progress(progress_cb, phase="vacuum", progress_current=0, progress_total=len(ADULT_SYNC_CLEANUP_TABLES), progress_percent=30.0, message=f"Deleted {deleted_rows} rows")

        vacuumed: list[str] = []
        async with engine.connect() as conn:
            auto_conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
            total_tables = len(ADULT_SYNC_CLEANUP_TABLES)
            for idx, table_name in enumerate(ADULT_SYNC_CLEANUP_TABLES, start=1):
                try:
                    await auto_conn.exec_driver_sql(f"VACUUM (ANALYZE) {table_name}")
                    vacuumed.append(table_name)
                except Exception:
                    logger.warning("adult cleanup vacuum failed for %s", table_name, exc_info=True)
                await _emit_progress(
                    progress_cb,
                    phase="vacuum",
                    progress_current=idx,
                    progress_total=total_tables,
                    progress_percent=30.0 + (idx / max(total_tables, 1)) * 70.0,
                    message=f"Vacuumed {idx}/{total_tables}",
                )

        result = {
            "status": "ok",
            "deleted": deleted_rows,
            "vacuumed_tables": vacuumed,
            "retention_days": ADULT_SYNC_GARBAGE_RETENTION_DAYS,
        }
        await _emit_progress(progress_cb, phase="done", progress_current=len(vacuumed), progress_total=len(ADULT_SYNC_CLEANUP_TABLES), progress_percent=100.0, message="Cleanup finished", status="ok")
        return result


async def _process_dns_unique_recheck_batch(db: AsyncSession, limit: int) -> int:
    if not await _ensure_adult_schema():
        return 0

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            rows = (
                await db.execute(
                    select(RemnawaveDNSUnique)
                    .where(RemnawaveDNSUnique.need_recheck.is_(True))
                    .order_by(RemnawaveDNSUnique.last_seen, RemnawaveDNSUnique.dns_root)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).scalars().all()
        except SQLAlchemyError as exc:
            await db.rollback()
            global _adult_schema_ready
            if _msg_has_undefined_table(exc):
                logger.warning("adult recheck skipped: remnawave_dns_unique table not ready yet")
                _adult_schema_ready = False  # force refresh on next attempt
                return 0
            if _msg_has_retryable_db_error(exc) and attempt < max_attempts:
                logger.warning("adult recheck select retryable db error: retry %s/%s", attempt, max_attempts)
                await asyncio.sleep(0.1 * attempt)
                continue
            if _msg_has_retryable_db_error(exc):
                logger.warning("adult recheck select skipped due to retryable db error: %s", exc)
                return 0
            logger.exception("adult recheck query failed")
            return 0

        if not rows:
            return 0

        now = datetime.now(timezone.utc)
        processed = 0

        # Bulk-resolve candidates for this batch to avoid per-row expensive queries.
        row_candidates: dict[str, list[str]] = {}
        all_candidates: set[str] = set()
        for row in rows:
            candidates = _match_candidate_domains(row.dns_root)
            row_candidates[row.dns_root] = candidates
            all_candidates.update(candidates)

        excluded_domains: set[str] = set()
        bucket_hits: dict[str, tuple[int, str | None]] = {}
        catalog_hits: dict[str, tuple[int, str | None]] = {}

        if all_candidates:
            candidate_list = list(all_candidates)

            excluded_domains = set(
                (
                    await db.execute(
                        select(AdultDomainExclusion.domain).where(AdultDomainExclusion.domain.in_(candidate_list))
                    )
                ).scalars().all()
            )

            grouped_by_bucket: dict[str, set[str]] = {}
            for candidate in all_candidates:
                table_name = _bucket_table_name_for_domain(candidate)
                grouped_by_bucket.setdefault(table_name, set()).add(candidate)

            for table_name, domains in grouped_by_bucket.items():
                model = _ADULT_BUCKET_TABLE_MODEL_BY_NAME.get(table_name)
                if model is None or not domains:
                    continue
                table_rows = (
                    await db.execute(
                        select(model.domain, model.source_mask, model.list_version).where(model.domain.in_(list(domains)))
                    )
                ).all()
                for domain, source_mask, list_version in table_rows:
                    bucket_hits.setdefault(domain, (int(source_mask or 0), list_version))

            # Fallback bucket for domains that may be stored in old.txt regardless of first character.
            unresolved_after_primary = all_candidates - set(bucket_hits.keys()) - excluded_domains
            if unresolved_after_primary:
                old_rows = (
                    await db.execute(
                        select(AdultDomainBucketOld.domain, AdultDomainBucketOld.source_mask, AdultDomainBucketOld.list_version)
                        .where(AdultDomainBucketOld.domain.in_(list(unresolved_after_primary)))
                    )
                ).all()
                for domain, source_mask, list_version in old_rows:
                    bucket_hits.setdefault(domain, (int(source_mask or 0), list_version))

            unresolved_for_catalog = all_candidates - set(bucket_hits.keys()) - excluded_domains
            if unresolved_for_catalog:
                catalog_rows = (
                    await db.execute(
                        select(
                            AdultDomainCatalog.domain,
                            AdultDomainCatalog.source_mask,
                            AdultDomainCatalog.list_version,
                            AdultDomainCatalog.checked_at,
                        )
                        .where(
                            and_(
                                AdultDomainCatalog.is_enabled.is_(True),
                                AdultDomainCatalog.domain.in_(list(unresolved_for_catalog)),
                            )
                        )
                        .order_by(AdultDomainCatalog.domain, desc(AdultDomainCatalog.checked_at), desc(AdultDomainCatalog.list_version))
                    )
                ).all()
                for domain, source_mask, list_version, _checked_at in catalog_rows:
                    catalog_hits.setdefault(domain, (int(source_mask or 0), list_version))

        for row in rows:
            candidates = row_candidates.get(row.dns_root, [])
            if not candidates:
                row.is_adult = False
                row.mark_source = []
                row.mark_version = None
                row.last_marked_at = now
                row.need_recheck = False
                processed += 1
                continue

            if any(candidate in excluded_domains for candidate in candidates):
                row.is_adult = False
                row.mark_source = ["manual_exclude"]
                row.mark_version = "manual_exclude"
                row.last_marked_at = now
                row.need_recheck = False
                processed += 1
                continue

            matched = None
            for candidate in candidates:
                matched = bucket_hits.get(candidate)
                if matched is not None:
                    break
            if matched is None:
                for candidate in candidates:
                    matched = catalog_hits.get(candidate)
                    if matched is not None:
                        break

            if matched is None:
                row.is_adult = False
                row.mark_source = []
                row.mark_version = None
            else:
                source_mask, list_version = matched
                row.is_adult = True
                row.mark_source = _sources_from_mask(source_mask)
                row.mark_version = list_version

            if not row.is_adult:
                row.is_adult = False
                row.mark_source = []
                row.mark_version = None

            row.last_marked_at = now
            row.need_recheck = False
            processed += 1

        try:
            await db.commit()
            return processed
        except SQLAlchemyError as exc:
            await db.rollback()
            if _msg_has_retryable_db_error(exc) and attempt < max_attempts:
                logger.warning("adult recheck commit retryable db error: retry %s/%s", attempt, max_attempts)
                await asyncio.sleep(0.1 * attempt)
                continue
            if _msg_has_retryable_db_error(exc):
                logger.warning("adult recheck commit skipped due to retryable db error: %s", exc)
                return 0
            logger.exception("adult recheck commit failed")
            return 0

    return 0

async def _sleep_with_stop(stop_event: asyncio.Event, seconds: int) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


def _next_weekly_sync(now: datetime, *, weekday: int, hour: int, minute: int) -> datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    days_ahead = (weekday - now.weekday()) % 7
    if target <= now:
        days_ahead = 7 if days_ahead == 0 else days_ahead
        target = target + timedelta(days=days_ahead)
    elif days_ahead:
        target = target + timedelta(days=days_ahead)
    return target


async def background_remnawave_adult_tasks(stop_event: asyncio.Event) -> None:
    _bg_runtime_state["running"] = True
    _bg_runtime_state["last_error"] = None
    active_schedule: dict | None = None
    next_sync: datetime | None = None
    while True:
        if stop_event.is_set() or services_killed():
            break
        if not services_enabled():
            _bg_runtime_state["last_loop_at"] = datetime.now(timezone.utc).isoformat()
            _bg_runtime_state["next_sync_at"] = None
            await _sleep_with_stop(stop_event, 1)
            continue

        try:
            now = datetime.now(timezone.utc)
            schedule = await get_adult_sync_schedule()
            if active_schedule != schedule or next_sync is None:
                active_schedule = dict(schedule)
                next_sync = _next_weekly_sync(
                    now,
                    weekday=int(schedule["weekday"]),
                    hour=int(schedule["hour"]),
                    minute=int(schedule["minute"]),
                )
                logger.info(
                    "adult sync scheduler configured: weekday=%s hour=%s minute=%s next=%s",
                    schedule["weekday"],
                    schedule["hour"],
                    schedule["minute"],
                    next_sync.isoformat(),
                )
            _bg_runtime_state["schedule"] = dict(schedule)
            _bg_runtime_state["next_sync_at"] = next_sync.isoformat() if next_sync else None
            _bg_runtime_state["last_loop_at"] = now.isoformat()
            if now >= next_sync:
                try:
                    stats = await sync_adult_catalog()
                    logger.info("adult sync finished: %s", stats)
                except Exception:
                    logger.exception("adult sync failed")
                finally:
                    next_sync = _next_weekly_sync(
                        datetime.now(timezone.utc) + timedelta(minutes=1),
                        weekday=int(schedule["weekday"]),
                        hour=int(schedule["hour"]),
                        minute=int(schedule["minute"]),
                    )
                    _bg_runtime_state["next_sync_at"] = next_sync.isoformat()

            if _CATALOG_SYNC_LOCK.locked() or _TXT_SYNC_LOCK.locked():
                await _sleep_with_stop(stop_event, 1)
                continue

            for _ in range(ADULT_RECHECK_MAX_BATCHES_PER_LOOP):
                if stop_event.is_set():
                    break
                changed = await process_dns_unique_recheck_batch(limit=ADULT_RECHECK_BATCH_LIMIT)
                if changed == 0:
                    break

            await _sleep_with_stop(stop_event, ADULT_RECHECK_LOOP_SLEEP_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            _bg_runtime_state["last_error"] = datetime.now(timezone.utc).isoformat()
            logger.exception("adult background task error")
            await _sleep_with_stop(stop_event, 30)
    _bg_runtime_state["running"] = False
