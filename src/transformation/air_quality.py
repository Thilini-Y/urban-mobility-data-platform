from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.data_model import EVENT_TIMESTAMP, add_event_time, parse_timestamp


def transform(df: DataFrame, spark=None) -> DataFrame:
    combined = F.concat_ws(" ", F.col("date_gmt"), F.col("time_gmt"))
    df = add_event_time(df, parse_timestamp(combined))
    df = df.withColumn("measurement_year", F.year(F.col(EVENT_TIMESTAMP)))
    return df