import asyncio
import ipaddress
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import httpx
from sqlalchemy import inspect
from sqlalchemy import and_, exists, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import async_session, engine
from app.services.schema_bootstrap import ensure_base_schema_ready
from app.models.remnawave_log import AdultDomainCatalog, AdultSyncState, RemnawaveDNSUnique

logger = logging.getLogger(__name__)

try:  # optional; fallback when dependency is not installed
    import tldextract
except Exception:  # pragma: no cover
    tldextract = None


ADULT_SYNC_JOB = "adult_domain_sync"
SOURCE_BLOCKLIST = 1
SOURCE_OISD = 2
SOURCE_V2FLY = 4
SOURCE_LABELS = {
    SOURCE_BLOCKLIST: "blocklistproject",
    SOURCE_OISD: "oisd",
    SOURCE_V2FLY: "v2fly",
}

BLOCKLIST_URL = "https://blocklistproject.github.io/Lists/alt-version/porn-nl.txt"
OISD_ROOT_URL = "https://oisd.nl/includedlists/nsfw"
V2FLY_ROOT_URL = "https://raw.githubusercontent.com/v2fly/domain-list-community/refs/heads/master/data/category-porn"

ADULT_SYNC_HTTP_MAX_CONCURRENCY = max(1, int(os.getenv("ADULT_SYNC_HTTP_MAX_CONCURRENCY", "1")))
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
_SCHEMA_READY_LOCK = asyncio.Lock()
_CATALOG_STARTUP_CHECK_DAYS = 7
_ADULT_TABLES = {
    "adult_domain_catalog",
    "remnawave_dns_unique",
    "adult_sync_state",
}
_adult_schema_ready = False


def _msg_has_undefined_table(err: Exception) -> bool:
    msg = str(err).lower()
    return "does not exist" in msg or "undefinedtable" in msg or "undefined table" in msg


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
        return True


async def ensure_adult_schema_ready() -> bool:
    """Public helper for ensuring adult detection tables exist before write/reads."""
    return await _ensure_adult_schema()



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

    if tldextract is not None:
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


def _sources_from_mask(mask: int) -> list[str]:
    out: list[str] = []
    for bit, name in sorted(SOURCE_LABELS.items()):
        if mask & bit:
            out.append(name)
    return out


def _extract_domains_from_text(text_value: str) -> set[str]:
    domains: set[str] = set()
    for raw_line in text_value.splitlines():
        raw = raw_line.strip()
        if not raw or raw[0] in {"#", "!", ";"}:
            continue

        if re.match(r"^(?:0\.0\.0\.0|127\.0\.0\.1|::1)\s+", raw):
            tokens = raw.split()
            for token in tokens[1:]:
                root = normalize_remnawave_domain(token)
                if root:
                    domains.add(root)
            continue

        for token in re.split(r"[\s,]+", raw):
            root = normalize_remnawave_domain(token)
            if root:
                domains.add(root)

    return domains


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


async def collect_adult_domain_map(timeout: int = 25, max_retries: int = 4) -> dict[str, int]:
    domain_map: dict[str, int] = {}

    async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "NanoRed/RemnawaveAuditor"}) as client:
        logger.info("adult sync: http concurrency limit=%s", ADULT_SYNC_HTTP_MAX_CONCURRENCY)
        fetch_semaphore = asyncio.Semaphore(ADULT_SYNC_HTTP_MAX_CONCURRENCY)

        async def _fetch_text(url: str) -> str:
            last_error: Exception | None = None
            for _ in range(max_retries):
                try:
                    async with fetch_semaphore:
                        resp = await client.get(url)
                    if resp.status_code >= 400:
                        raise RuntimeError(f"{url} -> HTTP {resp.status_code}")
                    return resp.text
                except Exception as exc:  # pragma: no cover
                    last_error = exc
                    await asyncio.sleep(0.7)
            raise RuntimeError(f"failed to fetch {url}: {last_error}")

        def _add_domains(raw: str, source_mask: int) -> None:
            for domain in _extract_domains_from_text(raw):
                domain_map[domain] = domain_map.get(domain, 0) | source_mask

        try:
            _add_domains(await _fetch_text(BLOCKLIST_URL), SOURCE_BLOCKLIST)
            logger.info("adult sync: blocklistproject collected")
        except Exception as exc:
            logger.warning("adult sync: blocklistproject failed: %s", exc)

        try:
            queue = [V2FLY_ROOT_URL]
            seen = set(queue)
            while queue:
                url = queue.pop(0)
                txt = await _fetch_text(url)
                _add_domains(txt, SOURCE_V2FLY)
                for inc in _extract_v2fly_includes(txt):
                    if inc not in seen:
                        seen.add(inc)
                        queue.append(inc)
            logger.info("adult sync: v2fly collected")
        except Exception as exc:
            logger.warning("adult sync: v2fly failed: %s", exc)

        try:
            html = await _fetch_text(OISD_ROOT_URL)
            urls = _extract_oisd_txt_links(html, OISD_ROOT_URL)
            for u in urls:
                try:
                    txt = await _fetch_text(u)
                    _add_domains(txt, SOURCE_OISD)
                except Exception as exc:
                    logger.warning("adult sync: oisd source failed %s: %s", u, exc)
            logger.info("adult sync: oisd collected (%s txt links)", len(urls))
        except Exception as exc:
            logger.warning("adult sync: oisd failed: %s", exc)

    return domain_map


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


async def sync_adult_catalog() -> dict:
    if not await _ensure_adult_schema():
        return {
            "version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "domains": 0,
            "updated": 0,
            "status": "schema_missing",
        }

    async with _CATALOG_SYNC_LOCK:
        return await _sync_adult_catalog_internal()


async def _sync_adult_catalog_internal() -> dict:
    start = datetime.now(timezone.utc)
    version = start.strftime("%Y%m%dT%H%M%SZ")
    domain_map = await collect_adult_domain_map()

    if not domain_map:
        stats = {"version": version, "domains": 0, "updated": 0, "status": "empty"}
        await _upsert_sync_state(status="warn", stats=stats, version=version)
        return stats

    items = list(domain_map.items())
    chunk_size = 1000

    async with async_session() as db:
        for i in range(0, len(items), chunk_size):
            chunk = items[i : i + chunk_size]
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
            )
            await db.execute(stmt)
            await db.commit()

        stale_stmt = (
            update(AdultDomainCatalog)
            .where(and_(AdultDomainCatalog.is_enabled.is_(True), AdultDomainCatalog.list_version != version))
            .values({
                "is_enabled": False,
                "source_mask": 0,
                "source_text": [],
            })
        )
        await db.execute(stale_stmt)

        recheck_stmt = (
            update(RemnawaveDNSUnique)
            .where(
                exists(
                    select(1)
                    .select_from(AdultDomainCatalog)
                    .where(
                        and_(
                            AdultDomainCatalog.domain == RemnawaveDNSUnique.dns_root,
                            AdultDomainCatalog.is_enabled.is_(True),
                            AdultDomainCatalog.list_version == version,
                        )
                    )
                ),
                RemnawaveDNSUnique.mark_version != version,
            )
            .values(need_recheck=True)
        )
        res = await db.execute(recheck_stmt)
        await db.commit()

        stats = {
            "version": version,
            "domains": len(domain_map),
            "updated": res.rowcount or 0,
            "status": "ok",
        }
        await _upsert_sync_state(status="ok", stats=stats, version=version, last_watermark=str(start))
        return stats


async def _adult_catalog_needs_bootstrap_sync() -> bool:
    if not await _ensure_adult_schema():
        # Try to bootstrap sync once at startup so schema can be created automatically.
        return True

    async with async_session() as db:
        state = await db.get(AdultSyncState, ADULT_SYNC_JOB)
    if not state:
        return True
    if state.status != "ok" or state.last_run_at is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=_CATALOG_STARTUP_CHECK_DAYS)
    return state.last_run_at < cutoff


async def _run_startup_adult_catalog_sync(stop_event: asyncio.Event) -> None:
    if stop_event.is_set():
        return
    try:
        if not await _adult_catalog_needs_bootstrap_sync():
            return
        logger.info("adult sync: startup bootstrap triggered (stale/missing catalog)")
        stats = await sync_adult_catalog()
        logger.info("adult sync: startup bootstrap finished: %s", stats)
    except Exception:
        logger.exception("adult sync: startup bootstrap failed")


async def process_dns_unique_recheck_batch(limit: int = 5000, session: AsyncSession | None = None) -> int:
    if session is None:
        async with async_session() as db:
            return await _process_dns_unique_recheck_batch(db, limit=limit)
    return await _process_dns_unique_recheck_batch(session, limit=limit)


async def _process_dns_unique_recheck_batch(db: AsyncSession, limit: int) -> int:
    if not await _ensure_adult_schema():
        return 0

    try:
        rows = (
            await db.execute(
                select(RemnawaveDNSUnique)
                .where(RemnawaveDNSUnique.need_recheck.is_(True))
                .order_by(RemnawaveDNSUnique.last_seen)
                .limit(limit)
            )
        ).scalars().all()
    except SQLAlchemyError as exc:
        global _adult_schema_ready
        if _msg_has_undefined_table(exc):
            logger.warning("adult recheck skipped: remnawave_dns_unique table not ready yet")
            _adult_schema_ready = False  # force refresh on next attempt
            return 0
        logger.exception("adult recheck query failed")
        return 0

    if not rows:
        return 0

    roots = [row.dns_root for row in rows]
    catalog_rows = (
        await db.execute(
            select(AdultDomainCatalog.domain, AdultDomainCatalog.source_mask, AdultDomainCatalog.list_version)
            .where(and_(AdultDomainCatalog.domain.in_(roots), AdultDomainCatalog.is_enabled.is_(True)))
        )
    ).all()

    catalog_map: dict[str, tuple[int, str | None]] = {
        domain: (int(mask), list_version)
        for domain, mask, list_version in catalog_rows
    }

    now = datetime.now(timezone.utc)
    processed = 0
    for row in rows:
        item = catalog_map.get(row.dns_root)
        if item is None:
            row.is_adult = False
            row.mark_source = []
            row.mark_version = None
        else:
            source_mask, list_version = item
            row.is_adult = True
            row.mark_source = _sources_from_mask(source_mask)
            row.mark_version = list_version

        row.last_marked_at = now
        row.need_recheck = False
        processed += 1

    await db.commit()
    return processed


async def _sleep_with_stop(stop_event: asyncio.Event, seconds: int) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


def _next_sunday_3utc(now: datetime) -> datetime:
    target = now.replace(hour=3, minute=0, second=0, microsecond=0)
    days_ahead = (6 - now.weekday()) % 7
    if target <= now:
        days_ahead = 7 if days_ahead == 0 else days_ahead
        target = target + timedelta(days=days_ahead)
    elif days_ahead:
        target = target + timedelta(days=days_ahead)
    return target


async def background_remnawave_adult_tasks(stop_event: asyncio.Event) -> None:
    asyncio.create_task(_run_startup_adult_catalog_sync(stop_event))
    next_sync = _next_sunday_3utc(datetime.now(timezone.utc))
    while True:
        if stop_event.is_set():
            break

        try:
            now = datetime.now(timezone.utc)
            if now >= next_sync:
                try:
                    stats = await sync_adult_catalog()
                    logger.info("adult sync finished: %s", stats)
                except Exception:
                    logger.exception("adult sync failed")
                next_sync = _next_sunday_3utc(now + timedelta(days=1))

            for _ in range(6):
                if stop_event.is_set():
                    break
                changed = await process_dns_unique_recheck_batch(limit=5000)
                if changed == 0:
                    break

            await _sleep_with_stop(stop_event, 20)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("adult background task error")
            await _sleep_with_stop(stop_event, 30)
