from __future__ import annotations

import unittest

from app.manifest import ManifestError, parse_manifest_yaml


class ManifestParsingTests(unittest.TestCase):
    def test_parses_new_query_shape(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
        query: SELECT account_id FROM lead_scores;
        meta:
          owner_team: revenue_ops
"""
        manifest = parse_manifest_yaml(raw)
        query = manifest.app.functions[0].queries[0]
        self.assertEqual("active_lead_scores", query.name)
        self.assertEqual("sql", query.type_name)
        self.assertEqual("SELECT account_id FROM lead_scores;", query.query)

    def test_rejects_deprecated_query_keys(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - query_name: active_lead_scores
        query_type: sql
        query: SELECT 1;
"""
        with self.assertRaises(ManifestError) as ctx:
            parse_manifest_yaml(raw)
        self.assertIn("deprecated keys", str(ctx.exception))

    def test_rejects_missing_query(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
"""
        with self.assertRaises(ManifestError) as ctx:
            parse_manifest_yaml(raw)
        self.assertIn("query is required", str(ctx.exception))

    def test_accepts_query_source_as_ignored_metadata(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
        query_source: queries/active_lead_scores.sql
        query: SELECT account_id FROM lead_scores;
"""
        manifest = parse_manifest_yaml(raw)
        query = manifest.app.functions[0].queries[0]
        self.assertEqual("SELECT account_id FROM lead_scores;", query.query)


if __name__ == "__main__":
    unittest.main()
