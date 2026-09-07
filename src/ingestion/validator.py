# validate the input data
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


# raise an error if a required column is missing
def validate_schema(df: DataFrame, expected_columns: list) -> None:
    missing = sorted(set(expected_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


# convert dataframe column types into defined types in dataset.yaml
def change_datatype(df: DataFrame, dtype_overrides: dict) -> DataFrame:
    for column, dtype in dtype_overrides.items():
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found")
        df = df.withColumn(column, F.col(column).cast(dtype))
    return df


# attach tracking variables
def add_lineage_columns(df: DataFrame, source_file: str, schema_version: str) -> DataFrame:
    return (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("schema_version", F.lit(schema_version))
    )


def run_quality_checks(df: DataFrame, pk_columns: list, numeric_rules: dict) -> DataFrame:
    checks = []

    for column in pk_columns:
        if column not in df.columns:
            raise ValueError(f"Primary key column '{column}' not found")
        checks.append((F.col(column).isNull(), f"missing_pk:{column}"))

    if pk_columns:
        window = Window.partitionBy(*pk_columns)
        df = df.withColumn("_pk_count", F.count(F.lit(1)).over(window))
        checks.append((F.col("_pk_count") > 1, "duplicate_pk"))

    if "event_timestamp" in df.columns:
        checks.append((F.col("event_timestamp").isNull(), "invalid_timestamp"))

    for column, bounds in numeric_rules.items():
        if column not in df.columns:
            raise ValueError(f"Numeric rule column '{column}' not found")
        value = F.col(column)
        allow_null = bool(bounds.get("allow_null", False))
        cond = F.lit(False) if allow_null else value.isNull()
        if "min" in bounds:
            cond = cond | (value < F.lit(bounds["min"]))
        if "max" in bounds:
            cond = cond | (value > F.lit(bounds["max"]))
        checks.append((cond, f"invalid_numeric:{column}"))

    reasons = [F.when(cond, F.lit(reason)) for cond, reason in checks]
    df = df.withColumn("rejection_reason", F.concat_ws(", ", *reasons))
    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("rejection_reason") == "", None).otherwise(F.col("rejection_reason")),
    )
    df = df.withColumn("is_valid", F.col("rejection_reason").isNull())

    if "_pk_count" in df.columns:
        df = df.drop("_pk_count")
    return df