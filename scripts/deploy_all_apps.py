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


def post_manifest(api_url: str, api_key: str, manifest_path: Path) -> dict[str, Any]:
    payload = manifest_path.read_text(encoding="utf-8")
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
            response_json = post_manifest(args.api_url, api_key, manifest_path)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            print(f"{manifest_path}: HTTP {exc.code}: {body}", file=sys.stderr)
            failed += 1
            continue
        except urllib.error.URLError as exc:
            print(f"{manifest_path}: request failed: {exc}", file=sys.stderr)
            failed += 1
            continue
        except OSError as exc:
            print(f"{manifest_path}: read failed: {exc}", file=sys.stderr)
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
