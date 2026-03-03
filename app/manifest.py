from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
import yaml


SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class ManifestError(ValueError):
    """Raised when an incoming manifest is invalid."""


class QuerySpec(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str = Field(min_length=1)
    type_name: str = Field(alias="type", min_length=1)
    query: Any
    meta: dict[str, Any] = Field(default_factory=dict)
    query_source: str | None = None


class FunctionSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")

    func_name: str = Field(min_length=1)
    queries: list[QuerySpec] = Field(min_length=1)


class ManifestSpec(BaseModel):
    model_config = ConfigDict(extra="ignore")

    app_name: str = Field(min_length=1)
    app_id: str | None = None
    functions: list[FunctionSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def set_default_app_id(self) -> ManifestSpec:
        if not self.app_id:
            self.app_id = to_snake_case(self.app_name)
        return self


def parse_manifest_yaml(raw_yaml: str) -> ManifestSpec:
    try:
        loaded = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ManifestError(f"Invalid YAML: {exc}") from exc
    return parse_manifest_dict(loaded)


def parse_manifest_dict(raw: Any) -> ManifestSpec:
    if raw is None:
        raw = {}
    try:
        return ManifestSpec.model_validate(raw)
    except ValidationError as exc:
        raise ManifestError(str(exc)) from exc


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
