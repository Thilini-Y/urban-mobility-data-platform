import os
import sys

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "urban-mobility-data-platform", shuffle_partitions: int = 8) -> SparkSession:
    # Python workers must run the same interpreter as the driver (the venv), not the system python3
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "6g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
