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
        query_source: queries/active_lead_scores.sql
        meta:
          owner_team: revenue_ops
"""
        manifest = parse_manifest_yaml(raw)
        query = manifest.app.functions[0].queries[0]
        self.assertEqual("active_lead_scores", query.name)
        self.assertEqual("sql", query.type_name)
        self.assertEqual("queries/active_lead_scores.sql", query.query_source)

    def test_rejects_deprecated_query_keys(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - query_name: active_lead_scores
        query_type: sql
        query: queries/active_lead_scores.sql
"""
        with self.assertRaises(ManifestError) as ctx:
            parse_manifest_yaml(raw)
        self.assertIn("deprecated keys", str(ctx.exception))

    def test_rejects_multiline_query_source(self) -> None:
        raw = """
app_name: CRM Platform
app_id: crm_platform
functions:
  - func_name: lead_scoring
    queries:
      - name: active_lead_scores
        type: sql
        query_source: |
          queries/active_lead_scores.sql
          second_line.sql
"""
        with self.assertRaises(ManifestError) as ctx:
            parse_manifest_yaml(raw)
        self.assertIn("query_source must be a file path", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
