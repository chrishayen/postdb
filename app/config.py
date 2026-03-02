from __future__ import annotations

import os


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./postdb.db")
POSTDB_API_KEY = os.getenv("POSTDB_API_KEY")
POSTDB_APPS_ROOT = os.getenv("POSTDB_APPS_ROOT", "apps")
