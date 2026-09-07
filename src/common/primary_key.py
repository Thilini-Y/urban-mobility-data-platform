from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_hash_key(df: DataFrame, source_columns: list, generated_column: str) -> DataFrame:
    values = F.struct(*[F.col(c) for c in source_columns])
    return df.withColumn(
        generated_column,
        F.sha2(F.to_json(values, options={"ignoreNullFields": "false"}), 256),
    )