from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.primary_key import add_hash_key

_DERIVED_COLUMNS = {
    "event_timestamp", "event_date", "event_hour",
    "pickup_year", "pickup_month", "trip_duration_minutes",
}



def transform(df: DataFrame, spark=None) -> DataFrame:
    df = df.withColumn("event_timestamp", F.col("pickup_timestamp"))
    df = df.withColumn("event_date", F.to_date(F.col("event_timestamp")))
    df = df.withColumn("event_hour", F.hour(F.col("event_timestamp")))

    # Partition columns used by the silver and gold Delta tables
    df = df.withColumn("pickup_year", F.year(F.col("event_timestamp")))
    df = df.withColumn("pickup_month", F.month(F.col("event_timestamp")))

    df = df.withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_timestamp").cast("long") - F.col("pickup_timestamp").cast("long")) / F.lit(60.0),
    )

    # generate fingerprints
    original_columns = [c for c in df.columns if c not in _DERIVED_COLUMNS]
    df = add_hash_key(df, source_columns=original_columns, generated_column="trip_id")
    return df