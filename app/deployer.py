from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.manifest import ManifestSpec


AUDIT_TABLE_NAME = "manifest_deployments"
QUERY_TABLE_NAME = "app_queries"
QUERY_KEY_COLUMNS = ("app_id", "func_name", "query_name")


async def apply_manifest(
    conn: AsyncConnection[Any],
    manifest: ManifestSpec,
    raw_yaml: str,
) -> dict[str, Any]:
    payload_sha = hashlib.sha256(raw_yaml.encode("utf-8")).hexdigest()
    query_rows, function_count = flatten_query_rows(manifest)

    async with conn.transaction():
        await ensure_audit_table(conn)
        table_created, columns_added, warnings = await ensure_query_table(conn)
        rows_inserted, rows_updated, rows_unchanged, function_actions = await upsert_query_rows(
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
        await log_deployment(conn, manifest, payload_sha, result)
        return result


def flatten_query_rows(manifest: ManifestSpec) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    function_count = len(manifest.app.functions)
    for function in manifest.app.functions:
        for query in function.queries:
            rows.append(
                {
                    "app_name": manifest.app.app_name,
                    "app_id": manifest.app.app_id,
                    "func_name": function.func_name,
                    "query_name": query.name,
                    "query_type": query.type_name,
                    "query": query.query,
                    "meta": query.meta,
                }
            )
    return rows, function_count


async def ensure_audit_table(conn: AsyncConnection[Any]) -> None:
    await conn.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL,
                application VARCHAR(255),
                "function" VARCHAR(255),
                payload_sha256 CHAR(64) NOT NULL,
                result_json JSONB NOT NULL
            )
            """
        ).format(sql.Identifier(AUDIT_TABLE_NAME))
    )


async def ensure_query_table(conn: AsyncConnection[Any]) -> tuple[bool, int, list[str]]:
    warnings: list[str] = []

    if not await table_exists(conn, QUERY_TABLE_NAME):
        await conn.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    app_name VARCHAR(255) NOT NULL,
                    app_id VARCHAR(255) NOT NULL,
                    func_name VARCHAR(255) NOT NULL,
                    query_name VARCHAR(255) NOT NULL,
                    query_type VARCHAR(100) NOT NULL,
                    query JSONB NOT NULL,
                    meta JSONB NOT NULL,
                    CONSTRAINT pk_app_queries PRIMARY KEY (app_id, func_name, query_name)
                )
                """
            ).format(sql.Identifier(QUERY_TABLE_NAME))
        )
        return True, 0, warnings

    existing_columns = await get_table_columns(conn, QUERY_TABLE_NAME)
    required_columns = {
        "app_name": "VARCHAR(255)",
        "app_id": "VARCHAR(255)",
        "func_name": "VARCHAR(255)",
        "query_name": "VARCHAR(255)",
        "query_type": "VARCHAR(100)",
        "query": "JSONB",
        "meta": "JSONB",
    }

    columns_added = 0
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        await conn.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(QUERY_TABLE_NAME),
                sql.Identifier(column_name),
                sql.SQL(column_type),
            )
        )
        columns_added += 1
        warnings.append(
            f"Added missing column '{column_name}' to existing '{QUERY_TABLE_NAME}' table."
        )

    constrained_columns = await get_primary_key_columns(conn, QUERY_TABLE_NAME)
    if constrained_columns and set(constrained_columns) != set(QUERY_KEY_COLUMNS):
        warnings.append(
            f"Existing primary key on '{QUERY_TABLE_NAME}' is {sorted(constrained_columns)}; "
            f"logical upserts use {list(QUERY_KEY_COLUMNS)}."
        )

    return False, columns_added, warnings


async def table_exists(conn: AsyncConnection[Any], table_name: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = %s
            )
            """,
            (table_name,),
        )
        row = await cur.fetchone()
    return bool(row and row[0])


async def get_table_columns(conn: AsyncConnection[Any], table_name: str) -> set[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            """,
            (table_name,),
        )
        rows = await cur.fetchall()
    return {str(row[0]) for row in rows}


async def get_primary_key_columns(conn: AsyncConnection[Any], table_name: str) -> list[str]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY AS key(attnum, ord)
              ON TRUE
            JOIN pg_attribute a
              ON a.attrelid = t.oid
             AND a.attnum = key.attnum
            WHERE i.indisprimary
              AND n.nspname = current_schema()
              AND t.relname = %s
            ORDER BY key.ord
            """,
            (table_name,),
        )
        rows = await cur.fetchall()
    return [str(row[0]) for row in rows]


async def upsert_query_rows(
    conn: AsyncConnection[Any],
    rows: list[dict[str, Any]],
) -> tuple[int, int, int, list[dict[str, Any]]]:
    inserted = 0
    updated = 0
    unchanged = 0
    function_stats: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"created_count": 0, "updated_count": 0, "unchanged_count": 0}
    )

    async with conn.cursor(row_factory=dict_row) as cur:
        for row in rows:
            stat_key = (row["app_name"], row["app_id"], row["func_name"])
            stats = function_stats[stat_key]

            await cur.execute(
                sql.SQL(
                    """
                    SELECT query_type, query, meta
                    FROM {}
                    WHERE app_id = %s
                      AND func_name = %s
                      AND query_name = %s
                    LIMIT 1
                    """
                ).format(sql.Identifier(QUERY_TABLE_NAME)),
                (row["app_id"], row["func_name"], row["query_name"]),
            )
            existing = await cur.fetchone()
            if existing:
                if not row_has_mutation(row, existing):
                    unchanged += 1
                    stats["unchanged_count"] += 1
                    continue

                await cur.execute(
                    sql.SQL(
                        """
                        UPDATE {}
                        SET app_name = %s,
                            query_type = %s,
                            query = %s,
                            meta = %s
                        WHERE app_id = %s
                          AND func_name = %s
                          AND query_name = %s
                        """
                    ).format(sql.Identifier(QUERY_TABLE_NAME)),
                    (
                        row["app_name"],
                        row["query_type"],
                        Jsonb(row["query"]),
                        Jsonb(row["meta"]),
                        row["app_id"],
                        row["func_name"],
                        row["query_name"],
                    ),
                )
                updated += 1
                stats["updated_count"] += 1
                continue

            await cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} (
                        app_name,
                        app_id,
                        func_name,
                        query_name,
                        query_type,
                        query,
                        meta
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                ).format(sql.Identifier(QUERY_TABLE_NAME)),
                (
                    row["app_name"],
                    row["app_id"],
                    row["func_name"],
                    row["query_name"],
                    row["query_type"],
                    Jsonb(row["query"]),
                    Jsonb(row["meta"]),
                ),
            )
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
    for field in ("query_type", "query", "meta"):
        if normalize_value(incoming.get(field)) != normalize_value(existing.get(field)):
            return True
    return False


def normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return value


async def log_deployment(
    conn: AsyncConnection[Any],
    manifest: ManifestSpec,
    payload_sha: str,
    result: dict[str, Any],
) -> None:
    await conn.execute(
        sql.SQL(
            """
            INSERT INTO {} (applied_at, application, "function", payload_sha256, result_json)
            VALUES (%s, %s, %s, %s, %s)
            """
        ).format(sql.Identifier(AUDIT_TABLE_NAME)),
        (
            datetime.now(timezone.utc),
            manifest.app.app_id,
            None,
            payload_sha,
            Jsonb(result),
        ),
    )
