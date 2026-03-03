from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
import uuid

import psycopg
from psycopg import AsyncConnection, sql

import app.deployer as deployer
from app.manifest import ManifestError, parse_manifest_yaml


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
        self._orig_apps_root = deployer.POSTDB_APPS_ROOT
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        deployer.POSTDB_APPS_ROOT = str(self.tmp_path / "apps")
        self.schema_name = f"postdb_test_{uuid.uuid4().hex}"

        probe_error = probe_database(TEST_DATABASE_URL)
        if probe_error:
            deployer.POSTDB_APPS_ROOT = self._orig_apps_root
            self._tmp.cleanup()
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
            deployer.POSTDB_APPS_ROOT = self._orig_apps_root
            if self.conn is not None:
                await self.conn.close()
            if self.admin_conn is not None:
                await self.admin_conn.close()
            self._tmp.cleanup()
            raise unittest.SkipTest(
                f"Postgres test database unavailable at {TEST_DATABASE_URL}: {exc}"
            ) from exc

    async def asyncTearDown(self) -> None:
        deployer.POSTDB_APPS_ROOT = self._orig_apps_root

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

        self._tmp.cleanup()

    def _write_app_query(self, app_id: str, rel_query_path: str, sql_text: str) -> None:
        query_path = self.tmp_path / "apps" / app_id / rel_query_path
        query_path.parent.mkdir(parents=True, exist_ok=True)
        query_path.write_text(sql_text, encoding="utf-8")

    @staticmethod
    def _manifest_yaml(query_path: str) -> str:
        return f"""
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
        query_source: {query_path}
        meta:
          owner_team: revenue_ops
"""

    async def test_second_apply_reports_no_change(self) -> None:
        assert self.conn is not None
        self._write_app_query(
            app_id="crm_platform",
            rel_query_path="queries/active_lead_scores.sql",
            sql_text="SELECT account_id FROM lead_scores;",
        )
        raw = self._manifest_yaml("queries/active_lead_scores.sql")
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

    async def test_changed_query_file_reports_update(self) -> None:
        assert self.conn is not None
        rel_path = "queries/active_lead_scores.sql"
        query_file = self.tmp_path / "apps" / "crm_platform" / rel_path
        self._write_app_query(
            app_id="crm_platform",
            rel_query_path=rel_path,
            sql_text="SELECT account_id FROM lead_scores;",
        )
        raw = self._manifest_yaml(rel_path)
        manifest = parse_manifest_yaml(raw)

        await deployer.apply_manifest(self.conn, manifest, raw)

        query_file.write_text(
            "SELECT account_id, score FROM lead_scores;",
            encoding="utf-8",
        )
        second = await deployer.apply_manifest(self.conn, manifest, raw)

        self.assertEqual(0, second["rows_inserted"])
        self.assertEqual(1, second["rows_updated"])
        self.assertEqual(0, second["rows_unchanged"])
        self.assertEqual("update", second["function_actions"][0]["status"])

    async def test_query_source_path_escape_is_rejected(self) -> None:
        assert self.conn is not None
        app_root = self.tmp_path / "apps" / "crm_platform"
        app_root.mkdir(parents=True, exist_ok=True)
        raw = self._manifest_yaml("../outside.sql")
        manifest = parse_manifest_yaml(raw)

        with self.assertRaises(ManifestError) as ctx:
            await deployer.apply_manifest(self.conn, manifest, raw)
        self.assertIn("path escapes app directory", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
