# load raw data files into Spark DataFrames
from __future__ import annotations

import glob
from typing import List

from src.common.config_loader import DatasetConfig


def find_source_files(cfg: DatasetConfig) -> List[str]:
    matched = sorted(glob.glob(cfg.absolute_raw_path))
    if not matched:
        raise FileNotFoundError(f"No files matched '{cfg.raw_path}' for dataset '{cfg.name}'")
    return matched


def load_source_file(spark, cfg: DatasetConfig, path: str):
    if cfg.format == "csv":
        options = {"header": "true", "inferSchema": "false"}
        options.update({k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in cfg.csv_options.items()})
        return spark.read.options(**options).csv(path)
    elif cfg.format == "parquet":
        return spark.read.parquet(path)
    else:
        raise ValueError(f"Unsupported format '{cfg.format}' for dataset '{cfg.name}'")