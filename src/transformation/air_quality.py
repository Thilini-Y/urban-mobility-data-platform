from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame, spark=None) -> DataFrame:
    df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.concat_ws(" ", F.col("date_local"), F.col("time_local")), "yyyy-MM-dd HH:mm"),
    )
    df = df.withColumn("event_date", F.to_date(F.col("date_local")))
    df = df.withColumn("event_hour", F.hour(F.col("event_timestamp")))
    df = df.withColumn("measurement_year", F.year(F.col("event_timestamp")))
    return df