from typing import Any

from pydantic import BaseModel, Field


class ApplicationQuery(BaseModel):
    app_name: str
    app_id: str
    func_name: str
    query_name: str
    query_type: str
    query: str
    enabled: bool
    meta: dict[str, Any]


class Query(BaseModel):
    name: str
    query_type: str
    query: str
    enabled: bool
    meta: dict[str, str]


class Function(BaseModel):
    name: str
    queries: list[Query] = Field(default_factory=list)


class Application(BaseModel):
    name: str
    id: str
    functions: list[Function] = Field(default_factory=list)


def parse_application_queries(rows: list[ApplicationQuery]) -> list[Application]:
    apps: dict[str, Application] = {}
    funcs_by_app: dict[str, dict[str, Function]] = {}

    for row in rows:
        if row.app_id not in apps:
            apps[row.app_id] = Application(name=row.app_name, id=row.app_id)
            funcs_by_app[row.app_id] = {}

        app = apps[row.app_id]
        app_functions = funcs_by_app[row.app_id]

        if row.func_name not in app_functions:
            function = Function(name=row.func_name)
            app_functions[row.func_name] = function
            app.functions.append(function)

        app_functions[row.func_name].queries.append(
            Query(
                name=row.query_name,
                query_type=row.query_type,
                query=row.query,
                enabled=row.enabled,
                meta={k: str(v) for k, v in (row.meta or {}).items()},
            )
        )

    return list(apps.values())


def main() -> None:
    raw = [
        {
            "app_name": "Sales",
            "app_id": "app-1",
            "func_name": "Forecasting",
            "query_name": "Monthly Revenue",
            "query_type": "sql",
            "query": "SELECT * FROM revenue",
            "enabled": True,
            "meta": {"owner": "data-team"},
        },
        {
            "app_name": "Sales",
            "app_id": "app-1",
            "func_name": "Forecasting",
            "query_name": "Quarterly Revenue",
            "query_type": "sql",
            "query": "SELECT * FROM quarterly_revenue",
            "enabled": True,
            "meta": {"owner": "finance"},
        },
        {
            "app_name": "Support",
            "app_id": "app-2",
            "func_name": "Ticket Metrics",
            "query_name": "Open Tickets",
            "query_type": "sql",
            "query": "SELECT * FROM tickets WHERE status='open'",
            "enabled": False,
            "meta": {"team": "support"},
        },
    ]

    rows = [ApplicationQuery.model_validate(item) for item in raw]
    parsed = parse_application_queries(rows)

    for app in parsed:
        print(app.model_dump())


if __name__ == "__main__":
    main()
