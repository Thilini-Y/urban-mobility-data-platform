from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform(df: DataFrame, spark=None) -> DataFrame:
    df = df.withColumn(
        "event_timestamp",
        F.expr("make_timestamp(year, month, day, hour, 0, cast(0 as decimal(8,6)))"),
    )
    df = df.withColumn("event_date", F.to_date(F.col("event_timestamp")))
    df = df.withColumn("event_hour", F.col("hour").cast("int"))
    return df