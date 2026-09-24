from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from src.common.config_loader import PROJECT_ROOT
import os

MONITORING_PATH = os.path.join(PROJECT_ROOT, "data", "delta", "metadata", "pipeline_runs")


@dataclass
class PipelineRunMetadata:
    run_id: str
    pipeline_name: str        # e.g. "ingestion", "integration", "product_refresh"
    dataset_name: str
    schema_version: str
    status: str                # SUCCESS / FAILED
    records_processed: int = 0
    records_inserted: int = 0
    records_rejected: int = 0
    validation_failures: str = "{}"
    storage_overhead_bytes: int = 0
    error_message: str = ""
    started_at: str = ""
    ended_at: str = ""
    execution_time_seconds: float = 0.0


class PipelineTimer:
    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.ended_at = datetime.now(timezone.utc)
        self.elapsed_seconds = time.perf_counter() - self._t0


def new_run_id() -> str:
    return str(uuid.uuid4())


def record_pipeline_run(metadata: PipelineRunMetadata) -> None:
    pdf = pd.DataFrame([asdict(metadata)])
    write_deltalake(MONITORING_PATH, pdf, mode="append")


def read_pipeline_runs() -> Optional[pd.DataFrame]:
    if not DeltaTable.is_deltatable(MONITORING_PATH):
        return None
    return DeltaTable(MONITORING_PATH).to_pandas()