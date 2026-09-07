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
) -> int:

    partition_by = partition_by or []
    staging_path = tempfile.mkdtemp(prefix="delta_stage_")
    try:

        df.write.mode("overwrite").parquet(staging_path)

        arrow_dataset = ds.dataset(staging_path, format="parquet")
        row_count = arrow_dataset.count_rows()

        reader = arrow_dataset.scanner(batch_size=65_536).to_reader()
        write_deltalake(path, reader, mode=mode, partition_by=partition_by or None)

        return row_count
    finally:
        shutil.rmtree(staging_path, ignore_errors=True)


def table_exists(path: str) -> bool:
    return DeltaTable.is_deltatable(path)


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