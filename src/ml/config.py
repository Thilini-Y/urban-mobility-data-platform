from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml

from src.common.config_loader import PROJECT_ROOT

DEFAULT_ML_CONFIG = os.path.join(PROJECT_ROOT, "config", "ml.yaml")


@dataclass(frozen=True)
class SplitConfig:
    train_ratio: float = 0.70
    validation_ratio: float = 0.15
    train_end: Optional[str] = None
    validation_end: Optional[str] = None

    def __post_init__(self):
        if self.train_end is None and self.validation_end is None:
            if not (0 < self.train_ratio and 0 < self.validation_ratio
                    and self.train_ratio + self.validation_ratio < 1):
                raise ValueError("train_ratio and validation_ratio must be > 0 and sum to < 1")
        elif self.train_end is None or self.validation_end is None:
            raise ValueError("train_end and validation_end must be set together")
        elif self.train_end >= self.validation_end:
            raise ValueError("train_end must be before validation_end")


@dataclass(frozen=True)
class FeatureConfig:
    temporal: list
    cyclical: dict
    holiday_country: str
    holiday_subdivision: Optional[str]
    holiday_extra: list
    categorical: list
    numeric: list
    zero_fill: list
    group_mean_columns: list
    group_mean_by: list
    scale: bool = True

    def __post_init__(self):
        unknown = [c for c in self.cyclical if c not in self.temporal]
        if unknown:
            raise ValueError(f"cyclical features must also be listed under temporal: {unknown}")
        filled = [*self.zero_fill, *self.group_mean_columns]
        unknown = [c for c in filled if c not in self.numeric]
        if unknown:
            raise ValueError(f"imputed columns must be listed under numeric: {unknown}")

    @property
    def flagged_columns(self) -> list:
        return list(dict.fromkeys([*self.zero_fill, *self.group_mean_columns]))


@dataclass(frozen=True)
class MLConfig:
    problem: str
    gold_table: str
    training_dataset: str
    manifest: str
    target: str
    entity_column: str
    time_column: str
    local_timezone: str
    exclude_zone_ids: list
    min_zone_trips: int
    lag_hours: list
    rolling_mean_hours: list
    drop_incomplete_history: bool
    data_start: Optional[str]
    data_end: Optional[str]
    weather_columns: list
    air_quality_columns: list
    zone_attribute_columns: list
    split: SplitConfig
    features: FeatureConfig
    feature_dataset: str
    feature_pipeline: str
    feature_summary: str
    model: ModelConfig
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def gold_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.gold_table)

    @property
    def training_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.training_dataset)

    @property
    def manifest_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.manifest)

    @property
    def feature_dataset_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.feature_dataset)

    @property
    def feature_pipeline_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.feature_pipeline)

    @property
    def feature_summary_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.feature_summary)

    @property
    def lag_columns(self) -> list:
        return [f"demand_lag_{h}h" for h in self.lag_hours]

    @property
    def rolling_columns(self) -> list:
        return [f"demand_rolling_mean_{h}h" for h in self.rolling_mean_hours]

    @property
    def max_history_hours(self) -> int:
        return max([*self.lag_hours, *self.rolling_mean_hours, 0])

    @property
    def model_dir(self) -> str:
        return os.path.join(PROJECT_ROOT, self.model.directory)

    @property
    def model_report_path(self) -> str:
        return os.path.join(PROJECT_ROOT, self.model.metrics_report)


@dataclass(frozen=True)
class ModelConfig:
    algorithm: str
    params: dict
    directory: str
    metrics_report: str
    feature_sources: dict


def load_ml_config(path: Optional[str] = None) -> MLConfig:
    path = path or DEFAULT_ML_CONFIG
    with open(path, "r") as f:
        raw = yaml.safe_load(f)["ml"]

    context = raw.get("context", {}) or {}
    split = raw.get("split", {}) or {}
    paths = raw["paths"]
    feat = raw.get("features", {}) or {}
    imputation = feat.get("imputation", {}) or {}
    group_fill = imputation.get("group_mean_fill", {}) or {}
    model_raw = raw.get("model", {}) or {}

    return MLConfig(
        problem=raw["problem"],
        gold_table=paths["gold_table"],
        training_dataset=paths["training_dataset"],
        manifest=paths["manifest"],
        target=raw["target"],
        entity_column=raw["entity_column"],
        time_column=raw["time_column"],
        local_timezone=raw.get("local_timezone", "UTC"),
        exclude_zone_ids=list(raw.get("exclude_zone_ids", []) or []),
        min_zone_trips=int(raw.get("min_zone_trips", 0)),
        lag_hours=list(raw.get("lag_hours", []) or []),
        rolling_mean_hours=list(raw.get("rolling_mean_hours", []) or []),
        drop_incomplete_history=bool(raw.get("drop_incomplete_history", True)),
        data_start=str(raw["data_start"]) if raw.get("data_start") else None,
        data_end=str(raw["data_end"]) if raw.get("data_end") else None,
        weather_columns=list(context.get("weather", []) or []),
        air_quality_columns=list(context.get("air_quality", []) or []),
        zone_attribute_columns=list(context.get("zone_attributes", []) or []),
        split=SplitConfig(
            train_ratio=float(split.get("train_ratio", 0.70)),
            validation_ratio=float(split.get("validation_ratio", 0.15)),
            train_end=str(split["train_end"]) if split.get("train_end") else None,
            validation_end=str(split["validation_end"]) if split.get("validation_end") else None,
        ),
        features=FeatureConfig(
            temporal=list(feat.get("temporal", []) or []),
            cyclical=dict(feat.get("cyclical", {}) or {}),
            holiday_country=str((feat.get("holiday_calendar", {}) or {}).get("country", "US")),
            holiday_subdivision=(feat.get("holiday_calendar", {}) or {}).get("subdivision"),
            holiday_extra=[str(d) for d in ((feat.get("holiday_calendar", {}) or {}).get("extra_dates", []) or [])],
            categorical=list(feat.get("categorical", []) or []),
            numeric=list(feat.get("numeric", []) or []),
            zero_fill=list(imputation.get("zero_fill", []) or []),
            group_mean_columns=list(group_fill.get("columns", []) or []),
            group_mean_by=list(group_fill.get("group_by", []) or [raw["time_column"]]),
            scale=bool(feat.get("scale", True)),
        ),
        feature_dataset=paths.get("feature_dataset", "data/ml/feature_dataset"),
        feature_pipeline=paths.get("feature_pipeline", "models/feature_pipeline"),
        feature_summary=paths.get("feature_summary", "reports/week4/feature_pipeline_summary.json"),
        model=ModelConfig(
            algorithm=str(model_raw.get("algorithm", "gbt")),
            params=dict(model_raw.get("params", {}) or {}),
            directory=model_raw.get("directory", "models/demand_model"),
            metrics_report=model_raw.get("metrics_report", "reports/week4/model_metrics.json"),
            feature_sources=dict(model_raw.get("feature_sources", {}) or {}),
        ),
        raw=raw,
    )