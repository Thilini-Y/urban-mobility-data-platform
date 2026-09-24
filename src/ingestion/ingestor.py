"""
Ingestion pipeline:
1. Load data
2. Validate schema
3. Standardizing and normalizing
4. Data transformation
5. Generate primary key
6. Deduplicate against existing Silver data     <-- NEW
7. Data Quality check
8. Store Delta tables (with schema evolution)    <-- UPDATED
9. Write metadata
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
from src.ingestion.loader import find_source_files, load_source_file
from src.ingestion.metadata import (
    IngestionRunMetadata,
    RunTimer,
    new_run_id,
    save_metadata,
)
from src.ingestion.validator import validate_schema
from src.transformation import get_transform
from src.common.delta_writer import table_exists, write_delta
from deltalake import DeltaTable
from src.monitoring.storage_utils import get_dir_size_bytes

from pyspark.sql import functions as F

import json


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


from deltalake import DeltaTable


def _drop_existing_pks(spark, cfg, silver_df):
    if not table_exists(cfg.silver_path()):
        return silver_df, 0

    existing_pks_pdf = (
        DeltaTable(cfg.silver_path())
        .to_pyarrow_dataset()
        .to_table(columns=list(cfg.primary_key))
        .to_pandas()
        .drop_duplicates()
    )

    if existing_pks_pdf.empty:
        return silver_df, 0

    existing_pks = spark.createDataFrame(existing_pks_pdf)

    before_count = silver_df.count()
    deduped_df = silver_df.join(existing_pks, on=cfg.primary_key, how="left_anti")
    after_count = deduped_df.count()
    return deduped_df, before_count - after_count


def _ingest_one_file(spark, cfg: DatasetConfig, source_path: str) -> IngestionRunMetadata:
    run_id = new_run_id()
    timer = RunTimer()
    metadata: Optional[IngestionRunMetadata] = None
    silver_before = get_dir_size_bytes(cfg.silver_path())

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
                # store delta table (schema_mode="merge" lets a new source column,
                # e.g. humidity/aqi, get added to Bronze without a full rewrite)
                records_bronze = write_delta(
                    bronze_df, cfg.bronze_path(), mode="append", partition_by=[], schema_mode="merge"
                )

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

                # deduplicate against records already in Silver (primary keys
                # like trip_id/event_timestamp only exist after transform_fn runs,
                # so this must happen here, not on the raw dataframe)
                silver_df, records_duplicate_ignored = _drop_existing_pks(spark, cfg, silver_df)

                # generate primary keys
                # data quality check
                silver_df = apply_quality_checks(silver_df, cfg.primary_key, cfg.numeric_rules)

                # common data model schema check
                validate_common_schema(silver_df, cfg)

                valid_df = silver_df.filter(F.col(IS_VALID))
                invalid_df = silver_df.filter(~F.col(IS_VALID)).withColumn(
                    QUARANTINED_AT, F.current_timestamp()
                )

                # store delta tables (schema_mode="merge" on Silver too, for the
                # same reason as Bronze above)
                records_valid = write_delta(
                    valid_df, cfg.silver_path(), mode=cfg.write_mode, partition_by=cfg.partition_by,
                    schema_mode="merge",
                )
                records_quarantined = write_delta(
                    invalid_df, cfg.quarantine_path(), mode="append", partition_by=[], schema_mode="merge"
                )

                storage_overhead = get_dir_size_bytes(cfg.silver_path()) - silver_before

                # breakdown of rejection reasons for monitoring
                if records_quarantined:
                    reason_counts = (
                        invalid_df
                        .withColumn("reason", F.explode(F.split(F.col("rejection_reason"), ", ")))
                        .groupBy("reason").count()
                        .collect()
                    )
                    validation_failures = json.dumps({r["reason"]: r["count"] for r in reason_counts})
                else:
                    validation_failures = "{}"

                # generate metadata
                metadata = _empty_metadata(
                    run_id, cfg, source_path,
                    status="SUCCESS",
                    records_read=records_read,
                    records_bronze=records_bronze,
                    records_valid_silver=records_valid,
                    records_quarantined=records_quarantined,
                    records_duplicate_ignored=records_duplicate_ignored,
                    storage_overhead_bytes=storage_overhead,
                    validation_failures=validation_failures,
                )
        except Exception as exc:  # noqa: BLE001
            metadata = _empty_metadata(run_id, cfg, source_path, status="FAILED", error_message=str(exc))

    # save metadata
    metadata.started_at = timer.started_at.isoformat()
    metadata.ended_at = timer.ended_at.isoformat()
    metadata.execution_time_seconds = round(timer.elapsed_seconds, 3)
    save_metadata(metadata)
    return metadata