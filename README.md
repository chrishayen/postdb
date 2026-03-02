# postdb

`postdb` is a FastAPI service that deploys app/function/query definitions from YAML into one static database table.

## Static Table Contract

The service writes to `app_queries` with columns:
- `app_name`
- `app_id`
- `func_name`
- `query_name`
- `query_type`
- `query_source`
- `query`
- `meta`

Logical upsert key:
- (`app_id`, `func_name`, `query_name`)

## API Security

`POST /deploy/yaml` is protected by an API key:
- Request header: `X-API-Key`
- Server env var: `POSTDB_API_KEY`

If `POSTDB_API_KEY` is missing on server startup, deploy requests return `500`.
If `X-API-Key` is missing or invalid, deploy requests return `401`.

## App Directory Structure

Each app lives in its own folder under `apps/`:

```text
apps/
  crm_platform/
    app.yaml
    queries/
      active_lead_scores.sql
      churn_watch.sql
```

Default root is `apps/`. You can override with:
- `POSTDB_APPS_ROOT` (server-side file resolution)
- `POSTDB_APPS_DIR` (bulk deploy script scanning)

## YAML Contract

One app per file (`apps/<app_id>/app.yaml`):

```yaml
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
```

Notes:
- `query_source` is a query file path, not inline SQL text.
- Query file paths must be relative and must stay inside `apps/<app_id>/`.
- Query file content is loaded by the API and stored in `query` while the path is stored in `query_source`.

## Quick Start (Poetry)

```bash
poetry install
export POSTDB_API_KEY="dev-secret"
poetry run uvicorn app.main:app --reload
```

Default DB:
- `sqlite:///./postdb.db`

Use Postgres:

```bash
poetry add psycopg[binary]
export DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/postdb"
```

## Deploy One App

```bash
curl -X POST "http://localhost:8000/deploy/yaml" \
  -H "Content-Type: application/x-yaml" \
  -H "X-API-Key: ${POSTDB_API_KEY}" \
  --data-binary @apps/crm_platform/app.yaml
```

## Bulk Deploy All Apps

Run:

```bash
poetry run python scripts/deploy_all_apps.py
```

Script env defaults:
- `POSTDB_API_URL` (default `http://localhost:8000/deploy/yaml`)
- `POSTDB_API_KEY` (required)
- `POSTDB_APPS_DIR` (default `apps`)

Output format per function:
- `app_name | func_name | create|update|no_change`

## API

- `GET /health`
- `POST /deploy/yaml`
