from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import yaml

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


@dataclass
class DatasetConfig:
    name: str
    format: str
    raw_path: str
    csv_options: dict
    schema_version: str
    expected_columns: list
    column_mapping: dict
    dtype_overrides: dict
    primary_key: list
    partition_by: list
    write_mode: str
    filter_expr: Optional[str]
    numeric_rules: dict
    transform_module: str

    @property
    def absolute_raw_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.raw_path)

    def bronze_path(self) -> str:
        return os.path.join(PROJECT_ROOT, "data", "delta", "bronze", self.name)

    def silver_path(self) -> str:
        return os.path.join(PROJECT_ROOT, "data", "delta", "silver", self.name)

    def quarantine_path(self) -> str:
        return os.path.join(PROJECT_ROOT, "data", "delta", "quarantine", self.name)


def _build_dataset_config(name: str, raw: dict) -> DatasetConfig:
    return DatasetConfig(
        name=name,
        format=raw["format"],
        raw_path=raw["raw_path"],
        csv_options=raw.get("csv_options", {}) or {},
        schema_version=str(raw["schema_version"]),
        expected_columns=raw.get("expected_columns", []),
        column_mapping=raw.get("column_mapping", {}) or {},
        dtype_overrides=raw.get("dtype_overrides", {}) or {},
        primary_key=raw.get("primary_key", []) or [],
        partition_by=raw.get("partition_by", []) or [],
        write_mode=raw.get("write_mode", "append"),
        filter_expr=raw.get("filter_expr"),
        numeric_rules=raw.get("numeric_rules", {}) or {},
        transform_module=raw["transform_module"],
    )


def load_dataset_configs(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        config_path = os.path.join(PROJECT_ROOT, "config", "datasets.yaml")
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return {name: _build_dataset_config(name, cfg) for name, cfg in raw["datasets"].items()}


def load_dataset_config(name: str, config_path: Optional[str] = None) -> DatasetConfig:
    configs = load_dataset_configs(config_path)
    if name not in configs:
        raise KeyError(f"Unknown dataset '{name}'. Known datasets: {sorted(configs)}")
    return configs[name]