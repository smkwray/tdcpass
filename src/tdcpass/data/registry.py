from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List

from tdcpass.core.paths import repo_root
from tdcpass.core.yaml_utils import load_yaml


@dataclass
class SeriesSpec:
    key: str
    display_name: str
    role: str
    frequency: str
    units: str
    status: str
    preferred_source: str
    quality_tier: str
    notes: str


def config_path(name: str) -> Path:
    return repo_root() / "config" / name


def load_data_sources() -> Any:
    return load_yaml(config_path("data_sources.yml"))


def load_series_specs() -> List[SeriesSpec]:
    payload = load_yaml(config_path("series_registry.yml"))
    specs: List[SeriesSpec] = []
    for row in payload.get("series", []):
        specs.append(SeriesSpec(**row))
    return specs
