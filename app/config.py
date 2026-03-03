from __future__ import annotations

import os


def normalize_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgresql+psycopg://"):
        return "postgresql://" + raw_url.removeprefix("postgresql+psycopg://")
    if raw_url.startswith("postgres+psycopg://"):
        return "postgresql://" + raw_url.removeprefix("postgres+psycopg://")
    return raw_url


DATABASE_URL = normalize_database_url(
    os.getenv("DATABASE_URL", "postgresql://postdb:postdb@127.0.0.1:5432/postdb")
)
POSTDB_API_KEY = os.getenv("POSTDB_API_KEY")
POSTDB_APPS_ROOT = os.getenv("POSTDB_APPS_ROOT", "apps")
