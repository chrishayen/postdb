from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Connection, Engine

from app.config import POSTDB_APPS_ROOT
from app.manifest import ManifestError, ManifestSpec


AUDIT_TABLE_NAME = "manifest_deployments"
QUERY_TABLE_NAME = "app_queries"
QUERY_KEY_COLUMNS = ("app_id", "func_name", "query_name")


def apply_manifest(engine: Engine, manifest: ManifestSpec, raw_yaml: str) -> dict[str, Any]:
    payload_sha = hashlib.sha256(raw_yaml.encode("utf-8")).hexdigest()
    query_rows, function_count = flatten_query_rows(manifest)

    with engine.begin() as conn:
        ensure_audit_table(conn)
        table_created, columns_added, warnings = ensure_query_table(conn)
        rows_inserted, rows_updated, rows_unchanged, function_actions = upsert_query_rows(
            conn,
            query_rows,
        )

        result = {
            "payload_sha256": payload_sha,
            "applied_at": datetime.now(timezone.utc).isoformat(),
            "app_id": manifest.app.app_id,
            "apps_processed": 1,
            "functions_processed": function_count,
            "queries_processed": len(query_rows),
            "table_name": QUERY_TABLE_NAME,
            "table_created": table_created,
            "columns_added": columns_added,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_unchanged": rows_unchanged,
            "function_actions": function_actions,
            "warnings": warnings,
        }
        log_deployment(conn, manifest, payload_sha, result)
        return result


def flatten_query_rows(manifest: ManifestSpec) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    function_count = len(manifest.app.functions)
    for function in manifest.app.functions:
        for query in function.queries:
            query_text = resolve_query_text(
                app_id=manifest.app.app_id,
                func_name=function.func_name,
                query_name=query.name,
                query_source=query.query_source,
            )
            rows.append(
                {
                    "app_name": manifest.app.app_name,
                    "app_id": manifest.app.app_id,
                    "func_name": function.func_name,
                    "query_name": query.name,
                    "query_type": query.type_name,
                    "query_source": query.query_source,
                    "query": query_text,
                    "meta": query.meta,
                }
            )
    return rows, function_count


def resolve_query_text(
    *,
    app_id: str,
    func_name: str,
    query_name: str,
    query_source: str,
) -> str:
    apps_root = Path(POSTDB_APPS_ROOT).resolve()
    app_dir = (apps_root / app_id).resolve()
    if not app_dir.is_dir():
        raise ManifestError(
            f"App '{app_id}' directory not found under '{apps_root}' while resolving query "
            f"for function '{func_name}', query '{query_name}'."
        )

    relative_path = Path(query_source)
    if relative_path.is_absolute():
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' must use a relative query_source path."
        )

    query_path = (app_dir / relative_path).resolve()
    try:
        query_path.relative_to(app_dir)
    except ValueError as exc:
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' path escapes app directory."
        ) from exc

    if not query_path.is_file():
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' file not found: {relative_path}"
        )

    try:
        query_text = query_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' must be UTF-8 text."
        ) from exc
    except OSError as exc:
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' could not be read: {exc}"
        ) from exc

    if not query_text.strip():
        raise ManifestError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' file is empty."
        )
    return query_text


def ensure_audit_table(conn: Connection) -> None:
    metadata = sa.MetaData()
    sa.Table(
        AUDIT_TABLE_NAME,
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("application", sa.String(255), nullable=True),
        sa.Column("function", sa.String(255), nullable=True),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("result_json", sa.JSON, nullable=False),
    )
    metadata.create_all(conn)


def ensure_query_table(conn: Connection) -> tuple[bool, int, list[str]]:
    inspector = sa.inspect(conn)
    warnings: list[str] = []

    if not inspector.has_table(QUERY_TABLE_NAME):
        metadata = sa.MetaData()
        sa.Table(
            QUERY_TABLE_NAME,
            metadata,
            sa.Column("app_name", sa.String(255), nullable=False),
            sa.Column("app_id", sa.String(255), nullable=False),
            sa.Column("func_name", sa.String(255), nullable=False),
            sa.Column("query_name", sa.String(255), nullable=False),
            sa.Column("query_type", sa.String(100), nullable=False),
            sa.Column("query_source", sa.String(1024), nullable=False),
            sa.Column("query", sa.Text(), nullable=False),
            sa.Column("meta", sa.JSON(), nullable=False),
            sa.PrimaryKeyConstraint(*QUERY_KEY_COLUMNS, name="pk_app_queries"),
        )
        metadata.create_all(conn)
        return True, 0, warnings

    existing_columns = {col["name"] for col in inspector.get_columns(QUERY_TABLE_NAME)}
    required_columns = {
        "app_name": sa.String(255),
        "app_id": sa.String(255),
        "func_name": sa.String(255),
        "query_name": sa.String(255),
        "query_type": sa.String(100),
        "query_source": sa.String(1024),
        "query": sa.Text(),
        "meta": sa.JSON(),
    }

    columns_added = 0
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        type_sql = str(column_type.compile(dialect=conn.dialect))
        conn.execute(
            sa.text(
                f'ALTER TABLE "{QUERY_TABLE_NAME}" ADD COLUMN "{column_name}" {type_sql}'
            )
        )
        columns_added += 1
        warnings.append(
            f"Added missing column '{column_name}' to existing '{QUERY_TABLE_NAME}' table."
        )

    pk_info = inspector.get_pk_constraint(QUERY_TABLE_NAME) or {}
    constrained_columns = set(pk_info.get("constrained_columns") or [])
    if constrained_columns and constrained_columns != set(QUERY_KEY_COLUMNS):
        warnings.append(
            f"Existing primary key on '{QUERY_TABLE_NAME}' is {sorted(constrained_columns)}; "
            f"logical upserts use {list(QUERY_KEY_COLUMNS)}."
        )

    return False, columns_added, warnings


def upsert_query_rows(
    conn: Connection,
    rows: list[dict[str, Any]],
) -> tuple[int, int, int, list[dict[str, Any]]]:
    metadata = sa.MetaData()
    table = sa.Table(QUERY_TABLE_NAME, metadata, autoload_with=conn)

    inserted = 0
    updated = 0
    unchanged = 0
    function_stats: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"created_count": 0, "updated_count": 0, "unchanged_count": 0}
    )

    for row in rows:
        stat_key = (row["app_name"], row["app_id"], row["func_name"])
        stats = function_stats[stat_key]

        where_clause = sa.and_(
            table.c.app_id == row["app_id"],
            table.c.func_name == row["func_name"],
            table.c.query_name == row["query_name"],
        )
        existing = conn.execute(
            sa.select(
                table.c.query_type,
                table.c.query_source,
                table.c.query,
                table.c.meta,
            )
            .where(where_clause)
            .limit(1)
        ).mappings().first()
        if existing:
            if not row_has_mutation(row, existing):
                unchanged += 1
                stats["unchanged_count"] += 1
                continue

            conn.execute(sa.update(table).where(where_clause).values(**row))
            updated += 1
            stats["updated_count"] += 1
            continue

        conn.execute(sa.insert(table).values(**row))
        inserted += 1
        stats["created_count"] += 1

    function_actions: list[dict[str, Any]] = []
    for (app_name, app_id, func_name), stats in sorted(function_stats.items()):
        created_count = int(stats["created_count"])
        updated_count = int(stats["updated_count"])
        unchanged_count = int(stats["unchanged_count"])
        if created_count > 0:
            status = "create"
        elif updated_count > 0:
            status = "update"
        else:
            status = "no_change"
        function_actions.append(
            {
                "app_name": app_name,
                "app_id": app_id,
                "func_name": func_name,
                "created_count": created_count,
                "updated_count": updated_count,
                "unchanged_count": unchanged_count,
                "status": status,
            }
        )

    return inserted, updated, unchanged, function_actions


def row_has_mutation(incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
    for field in ("query_type", "query_source", "query", "meta"):
        if normalize_value(incoming.get(field)) != normalize_value(existing.get(field)):
            return True
    return False


def normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return value


def log_deployment(
    conn: Connection,
    manifest: ManifestSpec,
    payload_sha: str,
    result: dict[str, Any],
) -> None:
    metadata = sa.MetaData()
    log_table = sa.Table(AUDIT_TABLE_NAME, metadata, autoload_with=conn)

    conn.execute(
        sa.insert(log_table).values(
            applied_at=datetime.now(timezone.utc),
            application=manifest.app.app_id,
            function=None,
            payload_sha256=payload_sha,
            result_json=result,
        )
    )
