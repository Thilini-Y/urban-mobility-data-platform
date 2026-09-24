# keep records on ingestion metadata
from __future__ import annotations

import time
import uuid
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import write_deltalake

from src.common.config_loader import PROJECT_ROOT

METADATA_PATH = os.path.join(PROJECT_ROOT, "data", "delta", "metadata", "ingestion_runs")


@dataclass
class IngestionRunMetadata:
    run_id: str
    dataset_name: str
    source_file: str
    format: str
    schema_version: str
    status: str
    records_read: int = 0
    records_bronze: int = 0
    records_valid_silver: int = 0
    records_quarantined: int = 0
    records_duplicate_ignored: int = 0
    error_message: str = ""
    started_at: str = ""
    ended_at: str = ""
    execution_time_seconds: float = 0.0


# to track the ingestion operation time length
class RunTimer:

    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.ended_at = datetime.now(timezone.utc)
        self.elapsed_seconds = time.perf_counter() - self._t0


def new_run_id() -> str:
    return str(uuid.uuid4())


def save_metadata(metadata: IngestionRunMetadata) -> None:
    pdf = pd.DataFrame([asdict(metadata)])
    write_deltalake(METADATA_PATH, pdf, mode="append", schema_mode="merge")


def read_ingestion_runs() -> Optional[pd.DataFrame]:
    from deltalake import DeltaTable
    if not DeltaTable.is_deltatable(METADATA_PATH):
        return None
    return DeltaTable(METADATA_PATH).to_pandas()