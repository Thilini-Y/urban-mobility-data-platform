"""
Prediction problem: hourly taxi demand per pickup zone
    target  = number of valid taxi trips that start in a zone during one hour
    grain   = pickup_location_id, hour_ts

The dataset is generated automatically from the integrated Gold Delta table

 1. keep valid trips only with a usable pickup zone
 2. aggregate trips to zone & hour
 3. build a dense zone x hour grid
 4. attach context from the Gold table: weather (per hour), air quality (per borough and hour),
    and zone attributes
 5. add history features (lagged / rolling demand of the same zone, past hours only)
 6. assign a chronological train / validation / test split
 7. write the result as a Delta table plus a manifest describing exactly what was built

"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.delta_writer import get_delta_version, read_delta_spark, table_exists, write_delta
from src.ml.config import MLConfig, load_ml_config

SPLIT_COLUMN = "split"
TRAIN, VALIDATION, TEST = "train", "validation", "test"
SPLITS = (TRAIN, VALIDATION, TEST)

BOROUGH_COLUMN = "pickup_borough"
SECONDS_PER_HOUR = 3600


def _to_epoch(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())


def _epoch_to_text(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def compute_split_boundaries(start_epoch: int, end_epoch: int, cfg: MLConfig) -> tuple:
    split = cfg.split
    if split.train_end is not None:
        return _to_epoch(split.train_end), _to_epoch(split.validation_end)

    total_hours = (end_epoch - start_epoch) // SECONDS_PER_HOUR + 1
    train_hours = int(total_hours * split.train_ratio)
    validation_hours = int(total_hours * split.validation_ratio)
    train_end = start_epoch + train_hours * SECONDS_PER_HOUR
    return train_end, train_end + validation_hours * SECONDS_PER_HOUR


def _check_columns(df: DataFrame, required: list, source: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{source} is missing columns required by config/ml.yaml: {missing}")



def load_valid_trips(spark: SparkSession, cfg: MLConfig) -> DataFrame:
    # Read the Gold table, keep valid trips with a usable pickup zone, add the hour bucket
    if not table_exists(cfg.gold_path):
        raise FileNotFoundError(
            f"No integrated Gold table at {cfg.gold_path} - run the Week 1 integration first")

    gold = read_delta_spark(spark, cfg.gold_path)
    needed = [cfg.entity_column, "event_timestamp", BOROUGH_COLUMN,
              *cfg.weather_columns, *cfg.air_quality_columns, *cfg.zone_attribute_columns]
    _check_columns(gold, needed, "Gold table")

    if "is_valid" in gold.columns:
        gold = gold.filter(F.col("is_valid"))

    trips = gold.filter(F.col(cfg.entity_column).isNotNull() & F.col("event_timestamp").isNotNull())
    if cfg.exclude_zone_ids:
        trips = trips.filter(~F.col(cfg.entity_column).isin(cfg.exclude_zone_ids))

    if cfg.data_start:
        trips = trips.filter(F.col("event_timestamp") >= F.lit(cfg.data_start).cast("timestamp"))
    if cfg.data_end:
        trips = trips.filter(F.col("event_timestamp") < F.lit(cfg.data_end).cast("timestamp"))

    keep = list(dict.fromkeys(needed))
    return (
        trips.select(*[c for c in keep if c != "event_timestamp"], "event_timestamp")
        .withColumn(cfg.time_column, F.date_trunc("hour", F.col("event_timestamp")))
        .drop("event_timestamp")
    )


def aggregate_demand(trips: DataFrame, cfg: MLConfig) -> DataFrame:
    # One row per (zone, hour) that had at least one trip
    return trips.groupBy(cfg.entity_column, cfg.time_column).agg(F.count(F.lit(1)).alias(cfg.target))


def _first_per(trips: DataFrame, keys: list, columns: list) -> DataFrame:
    # Weather is joined to trips on (date, hour) and air quality on (borough, date, hour) in the
    return trips.groupBy(*keys).agg(*[F.first(c, ignorenulls=True).alias(c) for c in columns])


def build_dense_grid(spark: SparkSession, zones: DataFrame, start_epoch: int, end_epoch: int,
                     cfg: MLConfig) -> DataFrame:
    hours = spark.range(start_epoch, end_epoch + 1, SECONDS_PER_HOUR).select(
        F.timestamp_seconds(F.col("id")).alias(cfg.time_column))
    return zones.select(cfg.entity_column).crossJoin(hours)


def add_history_features(df: DataFrame, cfg: MLConfig) -> DataFrame:
    # Lagged and rolling mean demand of the same zone.
    ordered = Window.partitionBy(cfg.entity_column).orderBy(cfg.time_column)
    target = F.col(cfg.target)

    for hours in cfg.lag_hours:
        df = df.withColumn(f"demand_lag_{hours}h", F.lag(target, hours).over(ordered))

    for hours in cfg.rolling_mean_hours:
        window = ordered.rowsBetween(-hours, -1)
        full = F.count(target).over(window) == hours
        df = df.withColumn(f"demand_rolling_mean_{hours}h", F.when(full, F.avg(target).over(window)))

    return df


#assign early records for training and later record for validation
def assign_split(df: DataFrame, cfg: MLConfig, train_end: int, validation_end: int) -> DataFrame:
    epoch = F.col(cfg.time_column).cast("long")
    return df.withColumn(
        SPLIT_COLUMN,
        F.when(epoch < train_end, F.lit(TRAIN))
        .when(epoch < validation_end, F.lit(VALIDATION))
        .otherwise(F.lit(TEST)),
    )


def build_training_dataset(spark: SparkSession, cfg: MLConfig) -> tuple:
    trips = load_valid_trips(spark, cfg)

    demand = aggregate_demand(trips, cfg).persist()
    bounds = demand.agg(
        F.min(F.col(cfg.time_column).cast("long")).alias("start"),
        F.max(F.col(cfg.time_column).cast("long")).alias("end"),
    ).first()
    if bounds["start"] is None:
        raise ValueError("The Gold table contains no valid trips - nothing to build a dataset from")
    start_epoch, end_epoch = int(bounds["start"]), int(bounds["end"])
    train_end, validation_end = compute_split_boundaries(start_epoch, end_epoch, cfg)

    # zones that are active in the TRAIN period only
    # taxi zones that have enough training trip
    active_zones = (
        demand.filter(F.col(cfg.time_column).cast("long") < train_end)
        .groupBy(cfg.entity_column).agg(F.sum(cfg.target).alias("n"))
        .filter(F.col("n") >= cfg.min_zone_trips)
        .select(cfg.entity_column)
    )

    # sets missing demand to 0 instead of dropping that hour
    grid = build_dense_grid(spark, active_zones, start_epoch, end_epoch, cfg)
    dataset = (
        grid.join(demand, on=[cfg.entity_column, cfg.time_column], how="left")
        .withColumn(cfg.target, F.coalesce(F.col(cfg.target), F.lit(0)).cast("int"))
    )

    # static zone attributes (zone name, borough)
    zone_attrs = _first_per(trips, [cfg.entity_column],
                            list(dict.fromkeys([*cfg.zone_attribute_columns, BOROUGH_COLUMN])))
    dataset = dataset.join(zone_attrs, on=cfg.entity_column, how="left")

    # hourly weather and borough-hourly air quality
    if cfg.weather_columns:
        weather = _first_per(trips, [cfg.time_column], cfg.weather_columns)
        dataset = dataset.join(weather, on=cfg.time_column, how="left")
    if cfg.air_quality_columns:
        air = _first_per(trips.filter(F.col(BOROUGH_COLUMN).isNotNull()),
                         [BOROUGH_COLUMN, cfg.time_column], cfg.air_quality_columns)
        dataset = dataset.join(air, on=[BOROUGH_COLUMN, cfg.time_column], how="left")

    dataset = add_history_features(dataset, cfg)

    if cfg.drop_incomplete_history and cfg.max_history_hours > 0:
        for name in [*cfg.lag_columns, *cfg.rolling_columns]:
            dataset = dataset.filter(F.col(name).isNotNull())

    dataset = assign_split(dataset, cfg, train_end, validation_end)

    ordered_columns = [
        cfg.entity_column, cfg.time_column,
        *[c for c in cfg.zone_attribute_columns],
        *([BOROUGH_COLUMN] if BOROUGH_COLUMN not in cfg.zone_attribute_columns else []),
        *cfg.weather_columns, *cfg.air_quality_columns,
        *cfg.lag_columns, *cfg.rolling_columns,
        cfg.target, SPLIT_COLUMN,
    ]
    dataset = dataset.select(*dict.fromkeys(ordered_columns))

    info = {
        "start": _epoch_to_text(start_epoch),
        "end": _epoch_to_text(end_epoch),
        "train_end": _epoch_to_text(train_end),
        "validation_end": _epoch_to_text(validation_end),
    }
    return dataset, info, demand

# save the build dataset
def generate_training_dataset(spark: SparkSession, cfg: Optional[MLConfig] = None) -> dict:
    cfg = cfg or load_ml_config()
    dataset, info, demand = build_training_dataset(spark, cfg)
    try:
        write_delta(dataset, cfg.training_path, mode="overwrite",
                    partition_by=[SPLIT_COLUMN], schema_mode="overwrite")
    finally:
        demand.unpersist()

    # validates the round trip and gives the manifest its numbers
    written = load_training_dataset(spark, cfg)
    per_split = {r[SPLIT_COLUMN]: r for r in written.groupBy(SPLIT_COLUMN).agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(cfg.entity_column).alias("zones"),
        F.avg(cfg.target).alias("mean_target"),
        F.min(cfg.time_column).cast("string").alias("first_hour"),
        F.max(cfg.time_column).cast("string").alias("last_hour"),
    ).collect()}

    manifest = {
        "problem": cfg.problem,
        "target": cfg.target,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source_gold_table": cfg.gold_table,
        "source_gold_version": get_delta_version(cfg.gold_path),
        "training_dataset_path": cfg.training_dataset,
        "training_dataset_version": get_delta_version(cfg.training_path),
        "time_range_utc": {"start": info["start"], "end": info["end"]},
        "split_boundaries_utc": {"train_end": info["train_end"], "validation_end": info["validation_end"]},
        "splits": {
            name: {
                "rows": int(per_split[name]["rows"]) if name in per_split else 0,
                "zones": int(per_split[name]["zones"]) if name in per_split else 0,
                "mean_target": round(float(per_split[name]["mean_target"]), 4) if name in per_split else None,
                "first_hour": per_split[name]["first_hour"] if name in per_split else None,
                "last_hour": per_split[name]["last_hour"] if name in per_split else None,
            }
            for name in SPLITS
        },
        "columns": written.columns,
        "config": cfg.raw,
    }
    os.makedirs(os.path.dirname(cfg.manifest_path), exist_ok=True)
    with open(cfg.manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    return manifest


def load_training_dataset(spark: SparkSession, cfg: Optional[MLConfig] = None,
                          split: Optional[str] = None) -> DataFrame:
    # Read the generated training dataset
    cfg = cfg or load_ml_config()
    df = read_delta_spark(spark, cfg.training_path)
    return df.filter(F.col(SPLIT_COLUMN) == split) if split else df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the ML training dataset from the Gold table")
    parser.add_argument("--config", default=None, help="path to ml.yaml (default config/ml.yaml)")
    args = parser.parse_args()

    from src.common.spark_session import get_spark_session
    spark = get_spark_session("ml-training-dataset")
    try:
        manifest = generate_training_dataset(spark, load_ml_config(args.config))
        print(json.dumps({k: manifest[k] for k in
                          ("time_range_utc", "split_boundaries_utc", "splits", "source_gold_version")}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()