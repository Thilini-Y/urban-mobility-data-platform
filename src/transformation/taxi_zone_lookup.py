from pyspark.sql import DataFrame


def transform(df: DataFrame, spark=None) -> DataFrame:
    return df