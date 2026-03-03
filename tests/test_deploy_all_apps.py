from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import yaml

from scripts.deploy_all_apps import materialize_manifest_payload


class DeployAllAppsMaterializationTests(unittest.TestCase):
    def test_materializes_sql_query_from_query_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app_dir = root / "crm_platform"
            (app_dir / "queries").mkdir(parents=True, exist_ok=True)
            (app_dir / "queries" / "active.sql").write_text(
                "SELECT account_id FROM lead_scores;\n",
                encoding="utf-8",
            )
            manifest_path = app_dir / "app.yaml"
            manifest_path.write_text(
                """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active
        type: sql
        query_source: queries/active.sql
        meta: {}
""",
                encoding="utf-8",
            )

            payload = materialize_manifest_payload(manifest_path)
            loaded = yaml.safe_load(payload)

        query = loaded["functions"][0]["queries"][0]
        self.assertEqual("queries/active.sql", query["query_source"])
        self.assertEqual("SELECT account_id FROM lead_scores;\n", query["query"])

    def test_materializes_json_query_as_structured_value(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app_dir = root / "crm_platform"
            (app_dir / "queries").mkdir(parents=True, exist_ok=True)
            (app_dir / "queries" / "rules.json").write_text(
                '{"enabled": true, "thresholds": [0.2, 0.9]}',
                encoding="utf-8",
            )
            manifest_path = app_dir / "app.yaml"
            manifest_path.write_text(
                """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: rules
        type: json
        query_source: queries/rules.json
        meta: {}
""",
                encoding="utf-8",
            )

            payload = materialize_manifest_payload(manifest_path)
            loaded = yaml.safe_load(payload)

        query = loaded["functions"][0]["queries"][0]
        self.assertEqual(True, query["query"]["enabled"])
        self.assertEqual([0.2, 0.9], query["query"]["thresholds"])

    def test_requires_query_or_query_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            app_dir = root / "crm_platform"
            app_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = app_dir / "app.yaml"
            manifest_path.write_text(
                """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active
        type: sql
        meta: {}
""",
                encoding="utf-8",
            )

            with self.assertRaises(ValueError) as ctx:
                materialize_manifest_payload(manifest_path)
        self.assertIn("either query_source or query", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
