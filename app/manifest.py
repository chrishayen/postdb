from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

import yaml


SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class ManifestError(ValueError):
    """Raised when an incoming manifest is invalid."""


@dataclass(slots=True)
class QuerySpec:
    name: str
    type_name: str
    query: Any
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FunctionSpec:
    func_name: str
    queries: list[QuerySpec]


@dataclass(slots=True)
class AppSpec:
    app_name: str
    app_id: str
    functions: list[FunctionSpec]


@dataclass(slots=True)
class ManifestSpec:
    app: AppSpec


def parse_manifest_yaml(raw_yaml: str) -> ManifestSpec:
    try:
        loaded = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ManifestError(f"Invalid YAML: {exc}") from exc
    return parse_manifest_dict(loaded)


def parse_manifest_dict(raw: Any) -> ManifestSpec:
    if not isinstance(raw, dict):
        raise ManifestError("Manifest body must be a YAML mapping.")
    if "tables" in raw:
        raise ManifestError("Deprecated format: use top-level app_name/functions, not tables.")
    if "apps" in raw:
        raise ManifestError("Use one app per file: remove 'apps' array and keep one top-level app definition.")

    app = parse_app_spec(raw)
    return ManifestSpec(app=app)


def parse_app_spec(raw_app: dict[str, Any]) -> AppSpec:
    app_name = require_non_empty_str(raw_app.get("app_name"), "app_name")
    app_id_raw = raw_app.get("app_id")
    app_id = infer_or_validate_app_id(app_name, app_id_raw, "app_id")

    functions_raw = raw_app.get("functions")
    if not isinstance(functions_raw, list) or not functions_raw:
        raise ManifestError("App manifest must include a non-empty functions list.")
    functions = [
        parse_function_spec(item, app_name, idx)
        for idx, item in enumerate(functions_raw, start=1)
    ]

    func_names = [f.func_name for f in functions]
    if len(func_names) != len(set(func_names)):
        raise ManifestError(f"App '{app_name}' contains duplicate func_name values.")

    return AppSpec(app_name=app_name, app_id=app_id, functions=functions)


def parse_function_spec(raw_function: Any, app_name: str, func_idx: int) -> FunctionSpec:
    if not isinstance(raw_function, dict):
        raise ManifestError(f"App '{app_name}' functions[{func_idx}] must be a mapping.")

    func_name = require_non_empty_str(
        raw_function.get("func_name"),
        f"App '{app_name}' functions[{func_idx}].func_name",
    )

    queries_raw = raw_function.get("queries")
    if not isinstance(queries_raw, list) or not queries_raw:
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' must include a non-empty queries list."
        )
    queries = [
        parse_query_spec(item, app_name, func_name, idx)
        for idx, item in enumerate(queries_raw, start=1)
    ]

    query_names = [q.name for q in queries]
    if len(query_names) != len(set(query_names)):
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' contains duplicate query names."
        )

    return FunctionSpec(func_name=func_name, queries=queries)


def parse_query_spec(
    raw_query: Any,
    app_name: str,
    func_name: str,
    query_idx: int,
) -> QuerySpec:
    if not isinstance(raw_query, dict):
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' queries[{query_idx}] must be a mapping."
        )

    if any(k in raw_query for k in ("query_name", "query_type")):
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' queries[{query_idx}] uses deprecated keys. "
            "Use: name, type, query, meta."
        )

    query_name = require_non_empty_str(
        raw_query.get("name"),
        f"App '{app_name}' function '{func_name}' queries[{query_idx}].name",
    )
    query_type = require_non_empty_str(
        raw_query.get("type"),
        f"App '{app_name}' function '{func_name}' query '{query_name}' type",
    )
    sentinel = object()
    query_value = raw_query.get("query", sentinel)
    if query_value is sentinel:
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' query '{query_name}' query is required."
        )

    raw_meta = raw_query.get("meta", {})
    if raw_meta is None:
        raw_meta = {}
    if not isinstance(raw_meta, dict):
        raise ManifestError(
            f"App '{app_name}' function '{func_name}' query '{query_name}' meta must be a mapping."
        )
    meta = {str(k): v for k, v in raw_meta.items()}

    return QuerySpec(
        name=query_name,
        type_name=query_type,
        query=query_value,
        meta=meta,
    )


def infer_or_validate_app_id(app_name: str, app_id_raw: Any, path: str) -> str:
    if app_id_raw is None:
        return to_snake_case(app_name)
    if not isinstance(app_id_raw, str) or not app_id_raw:
        raise ManifestError(f"{path} must be a non-empty string if provided.")
    if not SNAKE_CASE_RE.match(app_id_raw):
        raise ManifestError(f"{path} must be snake_case (example: customer_portal).")
    return app_id_raw


def to_snake_case(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_")
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized).lower()
    normalized = re.sub(r"_+", "_", normalized)
    if not normalized:
        raise ManifestError("app_name cannot be empty.")
    if not SNAKE_CASE_RE.match(normalized):
        raise ManifestError(
            f"Could not infer a valid snake_case app_id from app_name '{value}'. Provide app_id explicitly."
        )
    return normalized


def require_non_empty_str(raw_value: Any, path: str) -> str:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ManifestError(f"{path} must be a non-empty string.")
    return raw_value.strip()
