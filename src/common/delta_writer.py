from __future__ import annotations

import shutil
import tempfile
from typing import Optional

import pyarrow.dataset as ds
from deltalake import DeltaTable, write_deltalake
from pyspark.sql import DataFrame


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[list] = None,
    schema_mode: Optional[str] = None,
) -> int:

    partition_by = partition_by or []
    staging_path = tempfile.mkdtemp(prefix="delta_stage_")
    try:

        df.write.mode("overwrite").parquet(staging_path)

        arrow_dataset = ds.dataset(staging_path, format="parquet")
        row_count = arrow_dataset.count_rows()

        reader = arrow_dataset.scanner(batch_size=65_536).to_reader()

        write_kwargs = dict(mode=mode, partition_by=partition_by or None)
        if schema_mode is not None:
            write_kwargs["schema_mode"] = schema_mode
        write_deltalake(path, reader, **write_kwargs)

        return row_count
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)


def table_exists(path: str) -> bool:
    return DeltaTable.is_deltatable(path)


def read_delta_spark(spark, path: str) -> DataFrame:
    # read only the files of the current table version: a directory read would also pick up
    # files removed by overwrites or restores that are still on disk
    if not table_exists(path):
        raise FileNotFoundError(f"No Delta table at {path}")
    files = DeltaTable(path).file_uris()
    if not files:
        raise ValueError(f"Delta table at {path} is empty")
    return spark.read.option("basePath", path).parquet(*files)


def read_delta_as_arrow(path: str):
    if not table_exists(path):
        return None
    return DeltaTable(path).to_pyarrow_dataset()


def read_delta_as_pandas(path: str):
    if not table_exists(path):
        return None
    return DeltaTable(path).to_pandas()


def get_delta_version(path: str) -> Optional[int]:
    if not table_exists(path):
        return None
    return DeltaTable(path).version()