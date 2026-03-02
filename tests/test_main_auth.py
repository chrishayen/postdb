from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import sqlalchemy as sa
from fastapi import HTTPException
from starlette.requests import Request

import app.deployer as deployer
import app.main as main


def build_request(body: str | bytes, headers: list[tuple[str, str]]) -> Request:
    payload = body.encode("utf-8") if isinstance(body, str) else body
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/deploy/yaml",
        "raw_path": b"/deploy/yaml",
        "query_string": b"",
        "headers": [(k.lower().encode("utf-8"), v.encode("utf-8")) for k, v in headers],
    }
    consumed = False

    async def receive() -> dict:
        nonlocal consumed
        if consumed:
            return {"type": "http.request", "body": b"", "more_body": False}
        consumed = True
        return {"type": "http.request", "body": payload, "more_body": False}

    return Request(scope, receive)


class DeployEndpointAuthTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._orig_key = main.POSTDB_API_KEY
        self._orig_engine = main.engine
        self._orig_apps_root = deployer.POSTDB_APPS_ROOT
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)

    def tearDown(self) -> None:
        main.POSTDB_API_KEY = self._orig_key
        main.engine = self._orig_engine
        deployer.POSTDB_APPS_ROOT = self._orig_apps_root
        self._tmp.cleanup()

    async def test_missing_server_key_returns_500(self) -> None:
        main.POSTDB_API_KEY = None
        request = build_request("app_name: x\nfunctions: []\n", [])
        with self.assertRaises(HTTPException) as ctx:
            await main.deploy_yaml(request)
        self.assertEqual(500, ctx.exception.status_code)

    async def test_missing_or_invalid_header_returns_401(self) -> None:
        main.POSTDB_API_KEY = "secret"

        missing = build_request("app_name: x\nfunctions: []\n", [])
        with self.assertRaises(HTTPException) as ctx_missing:
            await main.deploy_yaml(missing)
        self.assertEqual(401, ctx_missing.exception.status_code)

        bad = build_request("app_name: x\nfunctions: []\n", [("X-API-Key", "wrong")])
        with self.assertRaises(HTTPException) as ctx_bad:
            await main.deploy_yaml(bad)
        self.assertEqual(401, ctx_bad.exception.status_code)

    async def test_valid_key_allows_deploy(self) -> None:
        app_id = "crm_platform"
        app_dir = self.tmp_path / "apps" / app_id / "queries"
        app_dir.mkdir(parents=True, exist_ok=True)
        (app_dir / "active.sql").write_text("SELECT 1;", encoding="utf-8")

        deployer.POSTDB_APPS_ROOT = str(self.tmp_path / "apps")
        main.engine = sa.create_engine("sqlite:///:memory:", future=True)
        main.POSTDB_API_KEY = "secret"

        body = f"""
app_name: CRM Platform
app_id: {app_id}
functions:
  - func_name: lead_scoring
    queries:
      - name: active
        type: sql
        query_source: queries/active.sql
        meta: {{}}
"""
        request = build_request(body, [("X-API-Key", "secret")])
        result = await main.deploy_yaml(request)

        self.assertEqual(app_id, result["app_id"])
        self.assertEqual(1, result["rows_inserted"])
        self.assertEqual(0, result["rows_updated"])
        self.assertEqual(0, result["rows_unchanged"])


if __name__ == "__main__":
    unittest.main()
