"""
Task 5 - Integration Pipeline.

Creates the integrated_taxi_trips Gold table with one row per taxi trip,
enriched with pickup-time weather, air quality, and pickup/dropoff geography.

Weather is matched by hour, while air quality is matched by borough and hour.
Multiple air-quality observations are averaged. Left joins preserve all
trips, leaving unavailable contextual information as null.

Limitations include hourly resolution, spatial averaging, pickup-time-only
conditions, and full-table rebuilding instead of incremental updates.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from deltalake import DeltaTable
from src.common.delta_writer import table_exists, write_delta


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

TRIPDATA_SILVER = os.path.join(PROJECT_ROOT, "data", "delta", "silver", "tripdata")
WEATHER_SILVER = os.path.join(PROJECT_ROOT, "data", "delta", "silver", "weather")
AIR_QUALITY_SILVER = os.path.join(PROJECT_ROOT, "data", "delta", "silver", "air_quality")
ZONE_LOOKUP_SILVER = os.path.join(PROJECT_ROOT, "data", "delta", "silver", "taxi_zone_lookup")
GOLD_PATH = os.path.join(PROJECT_ROOT, "data", "delta", "gold", "integrated_taxi_trips")


# county names in the Air Quality dataset into the borough names in the Tripdata dataset
COUNTY_TO_BOROUGH = {
    "Bronx": "Bronx",
    "Kings": "Brooklyn",
    "New York": "Manhattan",
    "Queens": "Queens",
    "Richmond": "Staten Island",
}


def _read_delta(spark: SparkSession, path: str) -> DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No silver data found at {path} - run ingestion for this dataset first")
    return spark.read.parquet(path)


def _map_county_to_borough(df: DataFrame, county_column: str, output_column: str) -> DataFrame:
    mapping = F.create_map(*[F.lit(x) for pair in COUNTY_TO_BOROUGH.items() for x in pair])
    return df.withColumn(output_column, mapping.getItem(F.col(county_column)))


def _prepare_air_quality(air_quality_df: DataFrame) -> DataFrame:
    ny_only = air_quality_df.filter(F.col("state_code") == "36")
    with_borough = _map_county_to_borough(ny_only, "county_name", "borough")
    with_borough = with_borough.filter(F.col("borough").isNotNull())

    return with_borough.groupBy("borough", "event_date", "event_hour").agg(
        F.avg("pm25_concentration").alias("air_quality_pm25"),
        F.count(F.lit(1)).alias("air_quality_station_count"),
    )

def _prepare_zone_lookup(zone_df: DataFrame) -> DataFrame:
    return zone_df.select(
        F.col("location_id"),
        F.col("zone"),
        F.col("borough"),
    )


def build_integrated_taxi_trips(spark: SparkSession) -> DataFrame:
    trips = _read_delta(spark, TRIPDATA_SILVER)
    weather = _read_delta(spark, WEATHER_SILVER)
    air_quality = _read_delta(spark, AIR_QUALITY_SILVER)
    zone_lookup = _prepare_zone_lookup(_read_delta(spark, ZONE_LOOKUP_SILVER))
    air_quality_agg = _prepare_air_quality(air_quality)

    weather_cols = weather.select(
        F.col("event_date"), F.col("event_hour"),
        F.col("temperature").alias("weather_temperature"),
        F.col("relative_humidity").alias("weather_relative_humidity"),
        F.col("precipitation").alias("weather_precipitation"),
        F.col("wind_speed").alias("weather_wind_speed"),
        F.col("pressure").alias("weather_pressure"),
        F.col("condition_code").alias("weather_condition_code"),
    )

    pickup_zone = zone_lookup.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zone = zone_lookup.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )

    enriched = (
        trips
        .join(pickup_zone, on="pickup_location_id", how="left")
        .join(dropoff_zone, on="dropoff_location_id", how="left")
        .join(weather_cols, on=["event_date", "event_hour"], how="left")
        .join(
            air_quality_agg,
            (trips["event_date"] == air_quality_agg["event_date"])
            & (trips["event_hour"] == air_quality_agg["event_hour"])
            & (F.col("pickup_borough") == air_quality_agg["borough"]),
            how="left",
        )
        .drop(air_quality_agg["event_date"])
        .drop(air_quality_agg["event_hour"])
        .drop(air_quality_agg["borough"])
    )

    return enriched

# found only the trips not already present in Gold delta table
def _new_trips_since_last_gold_build(spark: SparkSession, trips_df: DataFrame) -> DataFrame:
    if not table_exists(GOLD_PATH):
        return trips_df

    existing_ids = spark.read.parquet(GOLD_PATH).select("trip_id").distinct()
    return trips_df.join(existing_ids, on="trip_id", how="left_anti")


# Join only newly ingested trips
def build_integrated_taxi_trips_incremental(spark: SparkSession):
    trips = _read_delta(spark, TRIPDATA_SILVER)
    new_trips = _new_trips_since_last_gold_build(spark, trips)

    if new_trips.limit(1).count() == 0:
        return None, 0

    weather = _read_delta(spark, WEATHER_SILVER)
    air_quality = _read_delta(spark, AIR_QUALITY_SILVER)
    zone_lookup = _prepare_zone_lookup(_read_delta(spark, ZONE_LOOKUP_SILVER))
    air_quality_agg = _prepare_air_quality(air_quality)

    weather_cols = weather.select(
        F.col("event_date"), F.col("event_hour"),
        F.col("temperature").alias("weather_temperature"),
        F.col("relative_humidity").alias("weather_relative_humidity"),
        F.col("precipitation").alias("weather_precipitation"),
        F.col("wind_speed").alias("weather_wind_speed"),
        F.col("pressure").alias("weather_pressure"),
        F.col("condition_code").alias("weather_condition_code"),
    )

    pickup_zone = zone_lookup.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff_zone = zone_lookup.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )

    enriched = (
        new_trips
        .join(pickup_zone, on="pickup_location_id", how="left")
        .join(dropoff_zone, on="dropoff_location_id", how="left")
        .join(weather_cols, on=["event_date", "event_hour"], how="left")
        .join(
            air_quality_agg,
            (new_trips["event_date"] == air_quality_agg["event_date"])
            & (new_trips["event_hour"] == air_quality_agg["event_hour"])
            & (F.col("pickup_borough") == air_quality_agg["borough"]),
            how="left",
        )
        .drop(air_quality_agg["event_date"])
        .drop(air_quality_agg["event_hour"])
        .drop(air_quality_agg["borough"])
    )

    row_count = enriched.count()
    return enriched, row_count