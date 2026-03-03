#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any
import urllib.error
import urllib.request

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy all app manifests under apps/<app_id>/app.yaml to postdb API."
    )
    parser.add_argument(
        "--apps-dir",
        default=os.getenv("POSTDB_APPS_DIR", "apps"),
        help="Directory containing app folders (default: apps or POSTDB_APPS_DIR).",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("POSTDB_API_URL", "http://localhost:8000/deploy/yaml"),
        help="postdb deploy endpoint (default: http://localhost:8000/deploy/yaml or POSTDB_API_URL).",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("POSTDB_API_KEY"),
        help="API key for X-API-Key header (default: POSTDB_API_KEY env).",
    )
    return parser.parse_args()


def find_manifest_files(apps_dir: Path) -> list[Path]:
    manifests: list[Path] = []
    for app_dir in sorted(p for p in apps_dir.iterdir() if p.is_dir()):
        app_yaml = app_dir / "app.yaml"
        if app_yaml.is_file():
            manifests.append(app_yaml)
            continue

        app_yml = app_dir / "app.yml"
        if app_yml.is_file():
            manifests.append(app_yml)
    return manifests


def materialize_manifest_payload(manifest_path: Path) -> str:
    try:
        raw = manifest_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"{manifest_path}: read failed: {exc}") from exc

    try:
        loaded = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ValueError(f"{manifest_path}: invalid YAML: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"{manifest_path}: manifest body must be a mapping.")

    app_id_value = loaded.get("app_id")
    if not isinstance(app_id_value, str) or not app_id_value.strip():
        raise ValueError(f"{manifest_path}: app_id must be a non-empty string.")
    app_id = app_id_value.strip()
    app_dir = manifest_path.parent

    functions = loaded.get("functions")
    if not isinstance(functions, list):
        raise ValueError(f"{manifest_path}: functions must be a list.")

    for func_idx, func in enumerate(functions, start=1):
        if not isinstance(func, dict):
            raise ValueError(f"{manifest_path}: functions[{func_idx}] must be a mapping.")
        func_name = str(func.get("func_name", f"functions[{func_idx}]"))

        queries = func.get("queries")
        if not isinstance(queries, list):
            raise ValueError(
                f"{manifest_path}: function '{func_name}' queries must be a list."
            )

        for query_idx, query in enumerate(queries, start=1):
            if not isinstance(query, dict):
                raise ValueError(
                    f"{manifest_path}: function '{func_name}' queries[{query_idx}] must be a mapping."
                )

            query_name = str(query.get("name", f"queries[{query_idx}]"))
            query_type = str(query.get("type", "")).strip().lower()
            query_source = query.get("query_source")

            if isinstance(query_source, str) and query_source.strip():
                query["query"] = load_query_content(
                    app_dir=app_dir,
                    app_id=app_id,
                    func_name=func_name,
                    query_name=query_name,
                    query_type=query_type,
                    query_source=query_source.strip(),
                )
                continue

            if "query" not in query:
                raise ValueError(
                    f"{manifest_path}: function '{func_name}' query '{query_name}' must include "
                    "either query_source or query."
                )

    return yaml.safe_dump(loaded, sort_keys=False, allow_unicode=True)


def load_query_content(
    *,
    app_dir: Path,
    app_id: str,
    func_name: str,
    query_name: str,
    query_type: str,
    query_source: str,
) -> Any:
    source_path = Path(query_source)
    if source_path.is_absolute():
        raise ValueError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' query_source must be relative."
        )

    query_path = (app_dir / source_path).resolve()
    try:
        query_path.relative_to(app_dir.resolve())
    except ValueError as exc:
        raise ValueError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' query_source escapes app directory."
        ) from exc

    if not query_path.is_file():
        raise ValueError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' file not found: {query_source}"
        )

    try:
        raw = query_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' must be UTF-8 text."
        ) from exc
    except OSError as exc:
        raise ValueError(
            f"App '{app_id}' function '{func_name}' query '{query_name}' read failed: {exc}"
        ) from exc

    if query_type == "json":
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"App '{app_id}' function '{func_name}' query '{query_name}' is type=json "
                f"but file is not valid JSON: {exc}"
            ) from exc
    return raw


def post_manifest(api_url: str, api_key: str, payload: str) -> dict[str, Any]:
    request_obj = urllib.request.Request(
        url=api_url,
        data=payload.encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/x-yaml",
            "X-API-Key": api_key,
        },
    )
    with urllib.request.urlopen(request_obj, timeout=60) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def print_function_actions(response_json: dict[str, Any], manifest_path: Path) -> None:
    actions = response_json.get("function_actions")
    if not isinstance(actions, list):
        print(f"{manifest_path}: no function_actions found in response")
        return

    for action in actions:
        if not isinstance(action, dict):
            continue
        app_name = str(action.get("app_name", ""))
        func_name = str(action.get("func_name", ""))
        status = str(action.get("status", ""))
        print(f"{app_name} | {func_name} | {status}")


def main() -> int:
    args = parse_args()
    api_key = args.api_key
    if not api_key:
        print("Missing API key. Set POSTDB_API_KEY or pass --api-key.", file=sys.stderr)
        return 2

    apps_dir = Path(args.apps_dir)
    if not apps_dir.is_dir():
        print(f"Apps directory not found: {apps_dir}", file=sys.stderr)
        return 2

    manifests = find_manifest_files(apps_dir)
    if not manifests:
        print(f"No app manifest files found under {apps_dir}. Expected apps/<app_id>/app.yaml.", file=sys.stderr)
        return 2

    succeeded = 0
    failed = 0

    for manifest_path in manifests:
        try:
            payload = materialize_manifest_payload(manifest_path)
            response_json = post_manifest(args.api_url, api_key, payload)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            print(f"{manifest_path}: HTTP {exc.code}: {body}", file=sys.stderr)
            failed += 1
            continue
        except urllib.error.URLError as exc:
            print(f"{manifest_path}: request failed: {exc}", file=sys.stderr)
            failed += 1
            continue
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            failed += 1
            continue
        except json.JSONDecodeError as exc:
            print(f"{manifest_path}: invalid JSON response: {exc}", file=sys.stderr)
            failed += 1
            continue

        print_function_actions(response_json, manifest_path)
        succeeded += 1

    print(f"Finished: succeeded={succeeded} failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
