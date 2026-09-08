from __future__ import annotations

from pyspark.sql import DataFrame


# schema validation according to config/datasets.yaml
def validate_schema(df: DataFrame, expected_columns: list) -> None:
    missing = sorted(set(expected_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")