from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import inspect, text

from app.core.database import Base, engine
from app.services.schema_bootstrap import ensure_base_schema_ready, reset_schema_bootstrap_state

logger = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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

    report["ok"] = not report["checks"].get("missing_tables_after", report["checks"].get("missing_tables")) and not report["checks"].get("invalid_indexes_after", report["checks"].get("invalid_indexes")) and not report["repairs_failed"]
    return report
