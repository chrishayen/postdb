from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import sqlalchemy as sa

import app.deployer as deployer
from app.manifest import ManifestError, parse_manifest_yaml


class DeployerBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_apps_root = deployer.POSTDB_APPS_ROOT
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        deployer.POSTDB_APPS_ROOT = str(self.tmp_path / "apps")
        self.engine = sa.create_engine("sqlite:///:memory:", future=True)

    def tearDown(self) -> None:
        deployer.POSTDB_APPS_ROOT = self._orig_apps_root
        self._tmp.cleanup()

    def _write_app_query(self, app_id: str, rel_query_path: str, sql: str) -> None:
        query_path = self.tmp_path / "apps" / app_id / rel_query_path
        query_path.parent.mkdir(parents=True, exist_ok=True)
        query_path.write_text(sql, encoding="utf-8")

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

    def test_second_apply_reports_no_change(self) -> None:
        self._write_app_query(
            app_id="crm_platform",
            rel_query_path="queries/active_lead_scores.sql",
            sql="SELECT account_id FROM lead_scores;",
        )
        raw = self._manifest_yaml("queries/active_lead_scores.sql")
        manifest = parse_manifest_yaml(raw)

        first = deployer.apply_manifest(self.engine, manifest, raw)
        second = deployer.apply_manifest(self.engine, manifest, raw)

        self.assertEqual(1, first["rows_inserted"])
        self.assertEqual(0, first["rows_updated"])
        self.assertEqual(0, first["rows_unchanged"])
        self.assertEqual("create", first["function_actions"][0]["status"])

        self.assertEqual(0, second["rows_inserted"])
        self.assertEqual(0, second["rows_updated"])
        self.assertEqual(1, second["rows_unchanged"])
        self.assertEqual("no_change", second["function_actions"][0]["status"])

    def test_changed_query_file_reports_update(self) -> None:
        rel_path = "queries/active_lead_scores.sql"
        query_file = self.tmp_path / "apps" / "crm_platform" / rel_path
        self._write_app_query(
            app_id="crm_platform",
            rel_query_path=rel_path,
            sql="SELECT account_id FROM lead_scores;",
        )
        raw = self._manifest_yaml(rel_path)
        manifest = parse_manifest_yaml(raw)

        deployer.apply_manifest(self.engine, manifest, raw)

        query_file.write_text(
            "SELECT account_id, score FROM lead_scores;",
            encoding="utf-8",
        )
        second = deployer.apply_manifest(self.engine, manifest, raw)

        self.assertEqual(0, second["rows_inserted"])
        self.assertEqual(1, second["rows_updated"])
        self.assertEqual(0, second["rows_unchanged"])
        self.assertEqual("update", second["function_actions"][0]["status"])

    def test_query_source_path_escape_is_rejected(self) -> None:
        app_root = self.tmp_path / "apps" / "crm_platform"
        app_root.mkdir(parents=True, exist_ok=True)
        raw = self._manifest_yaml("../outside.sql")
        manifest = parse_manifest_yaml(raw)

        with self.assertRaises(ManifestError) as ctx:
            deployer.apply_manifest(self.engine, manifest, raw)
        self.assertIn("path escapes app directory", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
