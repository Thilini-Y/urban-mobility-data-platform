from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.data_model import add_event_time, timestamp_from_parts


def transform(df: DataFrame, spark=None) -> DataFrame:
    instant = timestamp_from_parts(F.col("year"), F.col("month"), F.col("day"), F.col("hour"))
    df = add_event_time(df, instant)
    return df