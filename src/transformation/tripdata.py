from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.data_model import EVENT_TIMESTAMP, add_event_time, normalize_timestamp
from src.common.primary_key import add_hash_key

_DERIVED_COLUMNS = {
    "event_timestamp", "event_date", "event_hour",
    "pickup_year", "pickup_month", "trip_duration_minutes",
}


def transform(df: DataFrame, spark=None) -> DataFrame:
    instant = normalize_timestamp(F.col("pickup_timestamp"), "America/New_York")
    df = add_event_time(df, instant)

    # Partition columns used by the silver and gold Delta tables
    df = df.withColumn("pickup_year", F.year(F.col(EVENT_TIMESTAMP)))
    df = df.withColumn("pickup_month", F.month(F.col(EVENT_TIMESTAMP)))

    df = df.withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_timestamp").cast("long") - F.col("pickup_timestamp").cast("long")) / F.lit(60.0),
    )

    # Fingerprint every original column
    original_columns = [c for c in df.columns if c not in _DERIVED_COLUMNS]
    df = add_hash_key(df, source_columns=original_columns, generated_column="trip_id")
    return df