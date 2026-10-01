from __future__ import annotations

import ast
import hashlib
import json
import os
import subprocess
import threading
import time
import urllib.request
from contextlib import contextmanager
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config_loader import PROJECT_ROOT

try:
    import psutil
except ImportError:
    psutil = None


_STAGE_FIELDS = {
    "numTasks": "tasks",
    "inputBytes": "input_bytes",
    "outputBytes": "output_bytes",
    "shuffleReadBytes": "shuffle_read_bytes",
    "shuffleWriteBytes": "shuffle_write_bytes",
    "memoryBytesSpilled": "memory_spilled_bytes",
    "diskBytesSpilled": "disk_spilled_bytes",
}


def spark_totals(spark) -> Optional[dict]:
    try:
        base = spark.sparkContext.uiWebUrl
        if not base:
            return None
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))   # local call, no proxy
        url = f"{base}/api/v1/applications/{spark.sparkContext.applicationId}/stages"
        with opener.open(url, timeout=5) as response:
            stages = json.load(response)
    except Exception:    # noqa: BLE001 - metrics must never break a run
        return None

    totals = {"stages": 0, "executor_run_seconds": 0.0, **{name: 0 for name in _STAGE_FIELDS.values()}}
    for stage in stages:
        if stage.get("status") not in ("COMPLETE", "ACTIVE"):    # skipped stages did no work
            continue
        totals["stages"] += 1
        totals["executor_run_seconds"] += (stage.get("executorRunTime") or 0) / 1000.0
        for source, name in _STAGE_FIELDS.items():
            totals[name] += stage.get(source) or 0
    return totals


class _MemorySampler(threading.Thread):

    def __init__(self, interval: float = 0.2):
        super().__init__(daemon=True)
        self.interval = interval
        self.peak_bytes = 0
        self._halt = threading.Event()

    def run(self):
        process = psutil.Process()
        while not self._halt.is_set():
            try:
                rss = process.memory_info().rss + sum(
                    child.memory_info().rss for child in process.children(recursive=True))
            except psutil.Error:
                rss = 0
            self.peak_bytes = max(self.peak_bytes, rss)
            self._halt.wait(self.interval)

    def stop(self) -> int:
        self._halt.set()
        self.join(timeout=2)
        return self.peak_bytes


# ---------------------------------------------------------------- run tracker

class RunTracker:

    def __init__(self, spark):
        self.spark = spark
        self.stages: dict = {}
        self._start = time.time()

    @contextmanager
    def stage(self, name: str):
        before = spark_totals(self.spark)
        sampler = _MemorySampler() if psutil is not None else None
        if sampler:
            sampler.start()
        start = time.time()
        try:
            yield
        finally:
            seconds = time.time() - start
            peak = sampler.stop() if sampler else None
            after = spark_totals(self.spark)
            record = {
                "seconds": round(seconds, 2),
                "peak_memory_mb": round(peak / 1e6, 1) if peak else None,
            }
            if before is not None and after is not None:
                record["spark"] = {key: round(after[key] - before[key], 2) for key in after}
            self.stages[name] = record

    def timings(self) -> dict:
        flat = {f"{name}_seconds": record["seconds"] for name, record in self.stages.items()}
        flat["total_seconds"] = round(time.time() - self._start, 2)
        return flat


def _python_files(paths: list) -> list:
    files = []
    for path in paths:
        full = path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)
        if os.path.isdir(full):
            for folder, _, names in os.walk(full):
                files += [os.path.join(folder, n) for n in names if n.endswith(".py")]
        elif os.path.exists(full):
            files.append(full)
    return sorted(set(files))


def code_stats(paths: list) -> dict:
    stats = {"files": 0, "lines_of_code": 0, "functions": 0, "classes": 0}
    for file in _python_files(paths):
        with open(file, encoding="utf-8") as f:
            text = f.read()
        stats["files"] += 1
        stats["lines_of_code"] += sum(1 for line in text.splitlines()
                                      if line.strip() and not line.strip().startswith("#"))
        for node in ast.walk(ast.parse(text)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                stats["functions"] += 1
            elif isinstance(node, ast.ClassDef):
                stats["classes"] += 1
    return stats


def config_lines(path: str) -> int:
    full = path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)
    with open(full, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip() and not line.strip().startswith("#"))


def null_shares(df: DataFrame, columns: list) -> dict:
    # Share of rows with a missing value (NULL or NaN) per column
    dtypes = dict(df.dtypes)
    expressions = []
    for column in columns:
        missing = F.col(column).isNull()
        if dtypes[column] in ("double", "float"):
            missing = missing | F.isnan(F.col(column))
        expressions.append(F.round(F.avg(missing.cast("int")), 4).alias(column))
    row = df.agg(*expressions).first()
    return {column: row[column] for column in columns}


def dataframe_checksum(df: DataFrame, columns: list) -> int:
    # Order-independent fingerprint of the selected columns
    row = df.select(F.sum(F.xxhash64(*[F.col(c).cast("string") for c in columns])).alias("checksum")).first()
    return int(row["checksum"]) if row["checksum"] is not None else 0


def config_hash(config: dict) -> str:
    return hashlib.sha256(json.dumps(config, sort_keys=True, default=str).encode()).hexdigest()[:16]


def git_commit() -> Optional[str]:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:    # noqa: BLE001
        return None