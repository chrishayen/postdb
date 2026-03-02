from __future__ import annotations

import secrets

from fastapi import FastAPI, HTTPException, Request
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from app.config import DATABASE_URL, POSTDB_API_KEY
from app.deployer import apply_manifest
from app.manifest import ManifestError, parse_manifest_yaml


engine = sa.create_engine(DATABASE_URL, future=True)

app = FastAPI(
    title="postdb",
    version="0.1.0",
    description="YAML-driven deployment of app/function/query definitions into a static query table.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
        manifest = parse_manifest_yaml(raw_yaml)
        return apply_manifest(engine, manifest, raw_yaml)
    except ManifestError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=500, detail=f"Database error: {exc}") from exc
