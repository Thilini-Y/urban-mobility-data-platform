"""
Ingestion pipeline:
1. Load data
2. Validate schema
3. Standardizing and normalizing
4. Data transformation
5. Generate primary key
6. Data Quality check
7. Store Delta tables
8. Write metadata
"""
from __future__ import annotations

from typing import List, Optional

from src.common.config_loader import DatasetConfig
from src.common.data_model import (
    IS_VALID,
    QUARANTINED_AT,
    add_lineage_columns,
    apply_quality_checks,
    change_datatype,
    normalize_missing_values,
    standardize_columns,
    validate_common_schema,
)
from src.common.delta_writer import write_delta
from src.ingestion.loader import find_source_files, load_source_file
from src.ingestion.metadata import (
    IngestionRunMetadata,
    RunTimer,
    new_run_id,
    save_metadata,
)
from src.ingestion.validator import validate_schema
from src.transformation import get_transform

from pyspark.sql import functions as F


def ingest_dataset(spark, cfg: DatasetConfig, source_paths: Optional[List[str]] = None) -> List[IngestionRunMetadata]:
    paths = source_paths if source_paths is not None else find_source_files(cfg)
    return [_ingest_one_file(spark, cfg, path) for path in paths]


def _empty_metadata(run_id: str, cfg: DatasetConfig, source_path: str, **overrides) -> IngestionRunMetadata:
    base = dict(
        run_id=run_id,
        dataset_name=cfg.name,
        source_file=source_path,
        format=cfg.format,
        schema_version=cfg.schema_version,
        status="FAILED",
    )
    base.update(overrides)
    return IngestionRunMetadata(**base)


def _ingest_one_file(spark, cfg: DatasetConfig, source_path: str) -> IngestionRunMetadata:
    run_id = new_run_id()
    timer = RunTimer()
    metadata: Optional[IngestionRunMetadata] = None

    with timer:
        try:
            # load data
            raw_df = load_source_file(spark, cfg, source_path)

            if cfg.filter_expr:
                raw_df = raw_df.filter(cfg.filter_expr)

            # validate schema
            try:
                validate_schema(raw_df, cfg.expected_columns)
                schema_ok = True
            except ValueError as exc:
                metadata = _empty_metadata(run_id, cfg, source_path, status="SCHEMA_REJECTED", error_message=str(exc))
                schema_ok = False

            if schema_ok:
                records_read = raw_df.count()

                # first Delta layer
                # preserves the source data as it was loaded
                bronze_df = add_lineage_columns(raw_df, source_path, cfg.schema_version)
                # store delta table
                records_bronze = write_delta(bronze_df, cfg.bronze_path(), mode="append", partition_by=[])

                # standardize column names
                processed_df = standardize_columns(raw_df, cfg.column_mapping)

                # normalize data types
                processed_df = normalize_missing_values(processed_df)
                processed_df = change_datatype(processed_df, cfg.dtype_overrides)

                # silver processing
                # data transformation
                transform_fn = get_transform(cfg.transform_module)
                silver_df = transform_fn(processed_df, spark)
                silver_df = add_lineage_columns(silver_df, source_path, cfg.schema_version)

                # generate primary keys
                # data quality check
                silver_df = apply_quality_checks(silver_df, cfg.primary_key, cfg.numeric_rules)

                # common data model schema check
                validate_common_schema(silver_df, cfg)

                valid_df = silver_df.filter(F.col(IS_VALID))
                invalid_df = silver_df.filter(~F.col(IS_VALID)).withColumn(
                    QUARANTINED_AT, F.current_timestamp()
                )

                # store delta tables
                records_valid = write_delta(valid_df, cfg.silver_path(), mode=cfg.write_mode, partition_by=cfg.partition_by)
                records_quarantined = write_delta(invalid_df, cfg.quarantine_path(), mode="append", partition_by=[])

                # generate metadata
                metadata = _empty_metadata(
                    run_id, cfg, source_path,
                    status="SUCCESS",
                    records_read=records_read,
                    records_bronze=records_bronze,
                    records_valid_silver=records_valid,
                    records_quarantined=records_quarantined,
                )
        except Exception as exc:  # noqa: BLE001
            metadata = _empty_metadata(run_id, cfg, source_path, status="FAILED", error_message=str(exc))

    # save metadata
    metadata.started_at = timer.started_at.isoformat()
    metadata.ended_at = timer.ended_at.isoformat()
    metadata.execution_time_seconds = round(timer.elapsed_seconds, 3)
    save_metadata(metadata)
    return metadata