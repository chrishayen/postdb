from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from psycopg import AsyncConnection, Error as PsycopgError
import yaml

from app.config import DATABASE_URL, POSTDB_API_KEY
from app.deployer import apply_manifest
from app.manifest import ManifestSpec

app = FastAPI(
    title="postdb",
    version="0.1.0",
    description="YAML-driven deployment of app/function/query definitions into a static query table.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


async def open_db_connection() -> AsyncConnection[Any]:
    return await AsyncConnection.connect(DATABASE_URL)


async def deploy_manifest(manifest: ManifestSpec, raw_yaml: str) -> dict:
    async with await open_db_connection() as conn:
        return await apply_manifest(conn, manifest, raw_yaml)


@app.post("/deploy/yaml")
async def deploy_yaml(request: Request) -> dict:
    if not POSTDB_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Server misconfiguration: POSTDB_API_KEY is not set.",
        )

    provided_key = request.headers.get("X-API-Key")
    if (not provided_key) or (not secrets.compare_digest(provided_key, POSTDB_API_KEY)):
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Request body cannot be empty.")

    try:
        raw_yaml = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="Request body must be UTF-8 encoded YAML.") from exc

    try:
        loaded = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid YAML: {exc}") from exc

    try:
        manifest = ManifestSpec.model_validate(loaded)
        return await deploy_manifest(manifest, raw_yaml)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}") from exc
