SHELL := /bin/bash

POSTDB_API_KEY ?= dev-secret
DATABASE_URL ?= postgresql://postdb:postdb@127.0.0.1:5432/postdb
POSTDB_API_URL ?= http://127.0.0.1:8000/deploy/yaml
POSTDB_APPS_DIR ?= apps
HOST ?= 127.0.0.1
PORT ?= 8000

export POSTDB_API_KEY
export DATABASE_URL
export POSTDB_API_URL
export POSTDB_APPS_DIR

.PHONY: dev post dev-db-up dev-db-down dev-server dev-deploy

dev: dev-db-up dev-server
post: dev-deploy

dev-db-up:
	docker compose up -d postgres

dev-db-down:
	docker compose down

dev-server:
	poetry run uvicorn app.main:app --reload --host $(HOST) --port $(PORT)

dev-deploy:
	poetry run python scripts/deploy_all_apps.py --api-url "$(POSTDB_API_URL)" --apps-dir "$(POSTDB_APPS_DIR)" --api-key "$(POSTDB_API_KEY)"
