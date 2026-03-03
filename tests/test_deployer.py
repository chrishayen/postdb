from __future__ import annotations

import os
import unittest
import uuid

import psycopg
from psycopg import AsyncConnection, sql

import app.deployer as deployer
from app.manifest import parse_manifest_yaml


TEST_DATABASE_URL = os.getenv(
    "POSTDB_TEST_DATABASE_URL",
    "postgresql://postdb:postdb@127.0.0.1:5432/postdb?connect_timeout=2",
)


def probe_database(url: str) -> str | None:
    try:
        conn = psycopg.connect(url, connect_timeout=2)
    except Exception as exc:
        return str(exc)
    conn.close()
    return None


class DeployerBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.schema_name = f"postdb_test_{uuid.uuid4().hex}"

        probe_error = probe_database(TEST_DATABASE_URL)
        if probe_error:
            raise unittest.SkipTest(
                f"Postgres test database unavailable at {TEST_DATABASE_URL}: {probe_error}"
            )

        self.admin_conn: AsyncConnection | None = None
        self.conn: AsyncConnection | None = None
        try:
            self.admin_conn = await AsyncConnection.connect(TEST_DATABASE_URL, autocommit=True)
            await self.admin_conn.execute(
                sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema_name))
            )
            self.conn = await AsyncConnection.connect(TEST_DATABASE_URL)
            await self.conn.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema_name))
            )
        except Exception as exc:
            if self.conn is not None:
                await self.conn.close()
            if self.admin_conn is not None:
                await self.admin_conn.close()
            raise unittest.SkipTest(
                f"Postgres test database unavailable at {TEST_DATABASE_URL}: {exc}"
            ) from exc

    async def asyncTearDown(self) -> None:
        if self.conn is not None:
            await self.conn.close()

        if self.admin_conn is not None:
            try:
                await self.admin_conn.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(self.schema_name)
                    )
                )
            finally:
                await self.admin_conn.close()

    @staticmethod
    def _manifest_yaml(query_text: str) -> str:
        return f"""
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
        query_source: queries/active_lead_scores.sql
        query: {query_text}
        meta:
          owner_team: revenue_ops
"""

    async def test_second_apply_reports_no_change(self) -> None:
        assert self.conn is not None
        raw = self._manifest_yaml("SELECT account_id FROM lead_scores;")
        manifest = parse_manifest_yaml(raw)

        first = await deployer.apply_manifest(self.conn, manifest, raw)
        second = await deployer.apply_manifest(self.conn, manifest, raw)

        self.assertEqual(1, first["rows_inserted"])
        self.assertEqual(0, first["rows_updated"])
        self.assertEqual(0, first["rows_unchanged"])
        self.assertEqual("create", first["function_actions"][0]["status"])

        self.assertEqual(0, second["rows_inserted"])
        self.assertEqual(0, second["rows_updated"])
        self.assertEqual(1, second["rows_unchanged"])
        self.assertEqual("no_change", second["function_actions"][0]["status"])

    async def test_changed_query_reports_update(self) -> None:
        assert self.conn is not None
        raw = self._manifest_yaml("SELECT account_id FROM lead_scores;")
        manifest = parse_manifest_yaml(raw)
        await deployer.apply_manifest(self.conn, manifest, raw)

        raw_changed = self._manifest_yaml("SELECT account_id, score FROM lead_scores;")
        manifest_changed = parse_manifest_yaml(raw_changed)
        second = await deployer.apply_manifest(self.conn, manifest_changed, raw_changed)

        self.assertEqual(0, second["rows_inserted"])
        self.assertEqual(1, second["rows_updated"])
        self.assertEqual(0, second["rows_unchanged"])
        self.assertEqual("update", second["function_actions"][0]["status"])

    async def test_query_source_is_ignored_when_query_is_present(self) -> None:
        assert self.conn is not None
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: json
        query_source: ../../definitely/not/used.json
        query:
          weights: [1, 2, 3]
          enabled: true
        meta: {}
"""
        manifest = parse_manifest_yaml(raw)
        result = await deployer.apply_manifest(self.conn, manifest, raw)

        self.assertEqual(1, result["rows_inserted"])
        self.assertEqual(0, result["rows_updated"])
        self.assertEqual(0, result["rows_unchanged"])

    async def test_legacy_query_source_column_is_compatible(self) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """
            CREATE TABLE app_queries (
                app_name VARCHAR(255) NOT NULL,
                app_id VARCHAR(255) NOT NULL,
                func_name VARCHAR(255) NOT NULL,
                query_name VARCHAR(255) NOT NULL,
                query_type VARCHAR(100) NOT NULL,
                query_source VARCHAR(1024) NOT NULL,
                query TEXT NOT NULL,
                meta JSONB NOT NULL,
                CONSTRAINT pk_app_queries PRIMARY KEY (app_id, func_name, query_name)
            )
            """
        )

        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: json
        query:
          version: 2
        meta: {}
"""
        manifest = parse_manifest_yaml(raw)
        result = await deployer.apply_manifest(self.conn, manifest, raw)

        self.assertEqual(1, result["rows_inserted"])
        self.assertIn("Converted column 'query' to JSONB", " ".join(result["warnings"]))


if __name__ == "__main__":
    unittest.main()
