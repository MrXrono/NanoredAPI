from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import inspect, text

from app.core.database import Base, engine
from app.services.schema_bootstrap import ensure_base_schema_ready, reset_schema_bootstrap_state

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RECOMMENDED_INDEX_SQL: dict[str, str] = {
    "ix_rnw_queries_account_requested_at": """
        CREATE INDEX IF NOT EXISTS ix_rnw_queries_account_requested_at
        ON remnawave_dns_queries (account_login, requested_at DESC)
    """,
    "ix_rnw_queries_account_dns_requested_at": """
        CREATE INDEX IF NOT EXISTS ix_rnw_queries_account_dns_requested_at
        ON remnawave_dns_queries (account_login, dns, requested_at DESC)
    """,
    "ix_rnw_accounts_total_requests": """
        CREATE INDEX IF NOT EXISTS ix_rnw_accounts_total_requests
        ON remnawave_accounts (total_requests DESC)
    """,
    "ix_rnw_nodes_last_seen_at": """
        CREATE INDEX IF NOT EXISTS ix_rnw_nodes_last_seen_at
        ON remnawave_nodes (last_seen_at DESC)
    """,
    "ix_adult_catalog_enabled_domain": """
        CREATE INDEX IF NOT EXISTS ix_adult_catalog_enabled_domain
        ON adult_domain_catalog (domain)
        WHERE is_enabled IS TRUE
    """,
    "ix_rnw_unique_is_adult_true": """
        CREATE INDEX IF NOT EXISTS ix_rnw_unique_is_adult_true
        ON remnawave_dns_unique (dns_root)
        WHERE is_adult IS TRUE
    """,
    "ix_rnw_unique_adult_last_seen": """
        CREATE INDEX IF NOT EXISTS ix_rnw_unique_adult_last_seen
        ON remnawave_dns_unique (last_seen DESC)
        WHERE is_adult IS TRUE
    """,
}


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return f'"{name}"'


def _required_tables() -> set[tuple[str, str]]:
    required: set[tuple[str, str]] = set()
    for table in Base.metadata.sorted_tables:
        required.add(((table.schema or "public"), table.name))
    return required


async def _existing_tables() -> set[tuple[str, str]]:
    async with engine.connect() as conn:
        return await conn.run_sync(
            lambda c: {
                ((schema or "public"), name)
                for schema in {tbl.schema for tbl in Base.metadata.sorted_tables}
                for name in inspect(c).get_table_names(schema=(schema or None))
            }
        )


async def _invalid_indexes() -> list[dict[str, str]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    """
                    SELECT ns.nspname AS schema_name, cls.relname AS index_name
                    FROM pg_index idx
                    JOIN pg_class cls ON cls.oid = idx.indexrelid
                    JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                    WHERE NOT idx.indisvalid
                    ORDER BY ns.nspname, cls.relname
                    """
                )
            )
        ).mappings().all()
    return [
        {"schema": str(r.get("schema_name") or "public"), "index": str(r.get("index_name") or "")}
        for r in rows
        if r.get("index_name")
    ]


async def _existing_public_indexes() -> set[str]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    """
                    SELECT c.relname AS index_name
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind = 'i'
                      AND n.nspname = 'public'
                    """
                )
            )
        ).mappings().all()
    return {str(r.get("index_name")) for r in rows if r.get("index_name")}


async def _ensure_recommended_indexes() -> dict[str, list[dict[str, str]]]:
    existing = await _existing_public_indexes()
    created: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []

    for index_name, create_sql in _RECOMMENDED_INDEX_SQL.items():
        if index_name in existing:
            continue
        try:
            async with engine.begin() as conn:
                await conn.execute(text(create_sql))
            created.append({"index": index_name, "result": "created"})
        except Exception as exc:
            logger.warning("create index failed for %s: %s", index_name, exc)
            failed.append({"index": index_name, "error": str(exc)})

    return {"created": created, "failed": failed}


async def check_and_repair_database_integrity() -> dict[str, Any]:
    report: dict[str, Any] = {
        "checked": True,
        "repairs_attempted": [],
        "repairs_failed": [],
        "checks": {},
        "ok": True,
    }

    required = _required_tables()
    existing = await _existing_tables()
    missing = sorted(required - existing)
    report["checks"]["missing_tables"] = [f"{s}.{t}" for s, t in missing]

    invalid_indexes = await _invalid_indexes()
    report["checks"]["invalid_indexes"] = [f"{r['schema']}.{r['index']}" for r in invalid_indexes]

    existing_indexes = await _existing_public_indexes()
    missing_recommended = sorted([idx for idx in _RECOMMENDED_INDEX_SQL.keys() if idx not in existing_indexes])
    report["checks"]["missing_recommended_indexes"] = missing_recommended

    try:
        async with engine.begin() as conn:
            await conn.execute(text("ALTER TABLE remnawave_accounts ADD COLUMN IF NOT EXISTS total_requests BIGINT NOT NULL DEFAULT 0"))
            await conn.execute(
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
        report["repairs_attempted"].append({"action": "ensure_remnawave_summary_schema", "result": "ok"})
    except Exception as exc:
        report["repairs_failed"].append({"action": "ensure_remnawave_summary_schema", "error": str(exc)})


    if missing:
        reset_schema_bootstrap_state()
        created = await ensure_base_schema_ready(force=True)
        if created:
            report["repairs_attempted"].append({
                "action": "create_missing_tables",
                "result": "ok",
                "count": len(missing),
            })
            existing_after = await _existing_tables()
            missing_after = sorted(required - existing_after)
            report["checks"]["missing_tables_after"] = [f"{s}.{t}" for s, t in missing_after]
            if missing_after:
                report["repairs_failed"].append({
                    "action": "create_missing_tables",
                    "error": f"Still missing tables: {len(missing_after)}",
                })
        else:
            report["repairs_failed"].append({
                "action": "create_missing_tables",
                "error": "ensure_base_schema_ready(force=True) returned false",
            })

    if invalid_indexes:
        for item in invalid_indexes:
            schema = item["schema"]
            index = item["index"]
            try:
                schema_q = _safe_ident(schema)
                index_q = _safe_ident(index)
                async with engine.begin() as conn:
                    await conn.execute(text(f"REINDEX INDEX {schema_q}.{index_q}"))
                report["repairs_attempted"].append({
                    "action": "reindex",
                    "target": f"{schema}.{index}",
                    "result": "ok",
                })
            except Exception as exc:
                logger.warning("reindex failed for %s.%s: %s", schema, index, exc)
                report["repairs_failed"].append({
                    "action": "reindex",
                    "target": f"{schema}.{index}",
                    "error": str(exc),
                })

        invalid_after = await _invalid_indexes()
        report["checks"]["invalid_indexes_after"] = [f"{r['schema']}.{r['index']}" for r in invalid_after]

    if missing_recommended:
        recommended_result = await _ensure_recommended_indexes()
        if recommended_result["created"]:
            report["repairs_attempted"].append({
                "action": "create_recommended_indexes",
                "result": "ok",
                "created": [x["index"] for x in recommended_result["created"]],
            })
        if recommended_result["failed"]:
            report["repairs_failed"].append({
                "action": "create_recommended_indexes",
                "error": "; ".join(f"{x['index']}: {x['error']}" for x in recommended_result["failed"]),
            })
        rec_after = await _existing_public_indexes()
        report["checks"]["missing_recommended_indexes_after"] = sorted(
            [idx for idx in _RECOMMENDED_INDEX_SQL.keys() if idx not in rec_after]
        )

    report["ok"] = (
        not report["checks"].get("missing_tables_after", report["checks"].get("missing_tables"))
        and not report["checks"].get("invalid_indexes_after", report["checks"].get("invalid_indexes"))
        and not report["checks"].get("missing_recommended_indexes_after", report["checks"].get("missing_recommended_indexes"))
        and not report["repairs_failed"]
    )
    return report
