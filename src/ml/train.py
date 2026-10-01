"""
eproducible ML pipeline.

1. load training dataset
2. feature engineering + regressor in ONE Spark ML Pipeline
3. fit on the TRAIN split, evaluate on validation and test
4. save a versioned model

It records stage timings, memory and Spark work, preprocessing
effort, implementation size and reproducibility fingerprints

"""
from __future__ import annotations

import argparse
import json
import os
import platform
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config_loader import PROJECT_ROOT
from src.ml.config import MLConfig, load_ml_config
from src.ml.features import (FEATURES_COLUMN, LABEL_COLUMN, build_feature_pipeline, categorical_columns,
                             feature_names, model_numeric_columns, numeric_input_columns)
from src.ml.run_metrics import (RunTracker, code_stats, config_hash, config_lines, dataframe_checksum,
                                git_commit, null_shares)
from src.ml.training_dataset import (SPLIT_COLUMN, TEST, TRAIN, VALIDATION, generate_training_dataset,
                                     load_training_dataset)

PREDICTION = "prediction"

ALGORITHMS = {
    "gbt": GBTRegressor,
    "random_forest": RandomForestRegressor,
    "linear": LinearRegression,
}

ML_CODE = ["src/ml"]
PLATFORM_CODE = ["src/common", "src/ingestion", "src/transformation", "src/integration"]


def _resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(PROJECT_ROOT, path)

def _deterministic_layout(df: DataFrame, cfg: MLConfig, n_partitions: int = 8) -> DataFrame:
    return (df.repartition(n_partitions, cfg.entity_column, cfg.time_column)
              .sortWithinPartitions(cfg.time_column, cfg.entity_column))


def build_regressor(cfg: MLConfig):
    name = cfg.model.algorithm
    if name not in ALGORITHMS:
        raise ValueError(f"Unknown model.algorithm '{name}'. Supported: {sorted(ALGORITHMS)}")
    return ALGORITHMS[name](featuresCol=FEATURES_COLUMN, labelCol=LABEL_COLUMN, **cfg.model.params)


def build_ml_pipeline(cfg: MLConfig) -> Pipeline:
    """Feature engineering stages (Task 2) followed by the regressor."""
    return Pipeline(stages=[*build_feature_pipeline(cfg).getStages(), build_regressor(cfg)])



def evaluate_predictions(df: DataFrame, label: str = LABEL_COLUMN, prediction: str = PREDICTION) -> dict:
    # RMSE, MAE, R2 and WAPE
    df = df.withColumn(prediction, F.greatest(F.col(prediction).cast("double"), F.lit(0.0))).cache()
    try:
        result = {
            metric: round(RegressionEvaluator(labelCol=label, predictionCol=prediction,
                                              metricName=metric).evaluate(df), 4)
            for metric in ("rmse", "mae", "r2")
        }
        totals = df.agg(F.sum(F.abs(F.col(label) - F.col(prediction))).alias("abs_error"),
                        F.sum(F.col(label)).alias("total")).first()
        # WAPE = total absolute error / total demand
        result["wape"] = round(totals["abs_error"] / totals["total"], 4) if totals["total"] else None
        result["rows"] = df.count()
        return result
    finally:
        df.unpersist()


def baseline_metrics(split_df: DataFrame, cfg: MLConfig) -> dict:
    # Naive forecasts: predict the demand of 1 hour, 1 day and 1 week ago
    out = {}
    for column in cfg.lag_columns:
        frame = split_df.select(F.col(cfg.target).cast("double").alias(LABEL_COLUMN),
                                F.col(column).cast("double").alias(PREDICTION))
        out[f"baseline_{column}"] = evaluate_predictions(frame)
    return out


def predictions_checksum(pred: DataFrame, cfg: MLConfig) -> int:
    # Fingerprint of the predictions
    frame = pred.select(cfg.entity_column, cfg.time_column,
                        F.round(F.col(PREDICTION), 4).alias(PREDICTION))
    return dataframe_checksum(frame, [cfg.entity_column, cfg.time_column, PREDICTION])



def source_of(name: str, feature_sources: dict) -> str:
    for prefix, dataset in feature_sources.items():
        if name.startswith(prefix):
            return dataset
    return "derived / other"


def feature_importance(regressor, names: list) -> list:
    if hasattr(regressor, "featureImportances"):
        values = regressor.featureImportances.toArray()
    else:
        coefficients = np.abs(regressor.coefficients.toArray())
        values = coefficients / coefficients.sum() if coefficients.sum() else coefficients
    return sorted(((n, float(v)) for n, v in zip(names, values)), key=lambda item: -item[1])


def importance_by_dataset(importances: list, cfg: MLConfig) -> dict:
    totals = {}
    for name, value in importances:
        dataset = source_of(name, cfg.model.feature_sources)
        totals[dataset] = totals.get(dataset, 0.0) + value
    return {k: round(v, 4) for k, v in sorted(totals.items(), key=lambda item: -item[1])}



def _new_run_id() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%S") + f"{now.microsecond // 1000:03d}Z"


def _training_dataset_info(cfg: MLConfig) -> Optional[dict]:
    if not os.path.exists(cfg.manifest_path):
        return None
    with open(cfg.manifest_path) as f:
        manifest = json.load(f)
    keep = ("source_gold_version", "training_dataset_version", "time_range_utc",
            "split_boundaries_utc", "splits")
    return {k: manifest.get(k) for k in keep}


def _preprocessing_profile(train: DataFrame, feature_model: PipelineModel, names: list, cfg: MLConfig) -> dict:
    used = (set(numeric_input_columns(cfg)) | set(categorical_columns(cfg))
            | {cfg.entity_column, cfg.time_column, SPLIT_COLUMN, cfg.target})
    return {
        "feature_pipeline_stages": len(feature_model.stages),
        "numeric_features": len(model_numeric_columns(cfg)),
        "categorical_features": len(categorical_columns(cfg)),
        "feature_vector_size": len(names),
        "imputed_columns": cfg.features.flagged_columns,
        "null_share_before_imputation": null_shares(train, numeric_input_columns(cfg)),
        "columns_not_used": sorted(set(train.columns) - used),
    }


def _implementation_profile() -> dict:
    # How much code the ML workflow needs on top of the platform
    return {
        "week4_ml_code": code_stats(ML_CODE),
        "reused_platform_code": code_stats(PLATFORM_CODE),
        "ml_config_lines": config_lines("config/ml.yaml"),
    }


def train_model(spark: SparkSession, cfg: Optional[MLConfig] = None,
                dataset: Optional[DataFrame] = None) -> dict:
    cfg = cfg or load_ml_config()
    tracker = RunTracker(spark)

    with tracker.stage("load_train"):
        data = dataset if dataset is not None else load_training_dataset(spark, cfg)
        data = _deterministic_layout(data, cfg)
        train = data.filter(F.col(SPLIT_COLUMN) == TRAIN).cache()
        training_rows = train.count()
    if training_rows == 0:
        raise ValueError("The train split is empty - generate the training dataset first")

    # 1. feature engineering: fit on the train split only, then materialise the features
    with tracker.stage("feature_engineering"):
        feature_model = build_feature_pipeline(cfg).fit(train)
        train_features = feature_model.transform(train).cache()
        train_features.count()

    # 2. model fitting
    with tracker.stage("model_fit"):
        regressor_model = build_regressor(cfg).fit(train_features)
    train_features.unpersist()

    # one saved pipeline: feature stages + fitted regressor
    model = PipelineModel([*feature_model.stages, regressor_model])
    names = feature_names(model, cfg)

    # 3. evaluation (model vs. naive baselines)
    metrics = {}
    test_checksum = None
    with tracker.stage("evaluation"):
        metrics[TRAIN] = {"model": evaluate_predictions(model.transform(train))}
        for split in (VALIDATION, TEST):
            part = data.filter(F.col(SPLIT_COLUMN) == split)
            if part.limit(1).count() == 0:
                continue
            pred = model.transform(part)
            metrics[split] = {"model": evaluate_predictions(pred), **baseline_metrics(part, cfg)}
            if split == TEST:
                test_checksum = predictions_checksum(pred, cfg)

    with tracker.stage("profiling"):
        preprocessing = _preprocessing_profile(train, feature_model, names, cfg)
        train_data_checksum = dataframe_checksum(train, [cfg.entity_column, cfg.time_column, cfg.target])
    train.unpersist()

    # 5. save
    importances = feature_importance(model.stages[-1], names)
    run_id = _new_run_id()
    run_dir = os.path.join(_resolve(cfg.model_dir), run_id)
    with tracker.stage("save_model"):
        model.write().overwrite().save(os.path.join(run_dir, "pipeline"))

    from src.monitoring.storage_utils import get_dir_size_bytes
    timings = tracker.timings()
    summary = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "problem": cfg.problem,
        "algorithm": cfg.model.algorithm,
        "params": cfg.model.params,
        "model_path": os.path.join(run_dir, "pipeline"),
        "model_size_bytes": get_dir_size_bytes(os.path.join(run_dir, "pipeline")),
        "training_rows": training_rows,
        "rows_per_split": {s: m["model"]["rows"] for s, m in metrics.items()},
        "timings": timings,
        "stages": tracker.stages,
        "fit_seconds": round(timings["feature_engineering_seconds"] + timings["model_fit_seconds"], 2),
        "preprocessing": preprocessing,
        "implementation": _implementation_profile(),
        "metrics": metrics,
        "feature_vector_size": len(names),
        "top_features": [{"feature": n, "importance": round(v, 4)} for n, v in importances[:15]],
        "importance_by_dataset": importance_by_dataset(importances, cfg),
        "reproducibility": {
            "seed": cfg.model.params.get("seed"),
            "config_hash": config_hash(cfg.raw),
            "code_version": git_commit(),
            "train_data_checksum": train_data_checksum,
            "predictions_checksum": test_checksum,
            "training_dataset": _training_dataset_info(cfg),
        },
        "environment": {
            "spark_version": spark.version,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        },
        "config": cfg.raw,
    }
    with open(os.path.join(run_dir, "run.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    with open(os.path.join(_resolve(cfg.model_dir), "latest.json"), "w") as f:
        json.dump({"run_id": run_id, "path": run_dir}, f, indent=2)

    report = _resolve(cfg.model.metrics_report)
    os.makedirs(os.path.dirname(report), exist_ok=True)
    with open(report, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    return summary


def load_model(cfg: Optional[MLConfig] = None, run_id: Optional[str] = None) -> PipelineModel:
    # Load a saved model
    cfg = cfg or load_ml_config()
    base = _resolve(cfg.model_dir)
    if run_id is None:
        with open(os.path.join(base, "latest.json")) as f:
            run_id = json.load(f)["run_id"]
    return PipelineModel.load(os.path.join(base, run_id, "pipeline"))


def predict(model: PipelineModel, df: DataFrame, cfg: Optional[MLConfig] = None) -> DataFrame:
    # Score rows in the training-dataset format
    cfg = cfg or load_ml_config()
    if cfg.target not in df.columns:
        df = df.withColumn(cfg.target, F.lit(None).cast("int"))
    if SPLIT_COLUMN not in df.columns:
        df = df.withColumn(SPLIT_COLUMN, F.lit("score"))
    return model.transform(df)


def retrain(spark: SparkSession, cfg: Optional[MLConfig] = None, refresh_dataset: bool = True) -> dict:
    # when a new data arrived, rebuild the training dataset from Gold
    cfg = cfg or load_ml_config()
    if refresh_dataset:
        generate_training_dataset(spark, cfg)
    return train_model(spark, cfg)


def list_runs(cfg: Optional[MLConfig] = None):
    import pandas as pd
    cfg = cfg or load_ml_config()
    base = _resolve(cfg.model_dir)
    rows = []
    for run_id in sorted(os.listdir(base)):
        path = os.path.join(base, run_id, "run.json")
        if not os.path.exists(path):
            continue
        with open(path) as f:
            run = json.load(f)
        metrics = run["metrics"]
        rows.append({
            "run_id": run_id, "algorithm": run["algorithm"], "training_rows": run["training_rows"],
            **run.get("timings", {}),
            "validation_rmse": metrics.get("validation", {}).get("model", {}).get("rmse"),
            "test_rmse": metrics.get("test", {}).get("model", {}).get("rmse"),
            "test_wape": metrics.get("test", {}).get("model", {}).get("wape"),
            "train_data_checksum": run.get("reproducibility", {}).get("train_data_checksum"),
            "predictions_checksum": run.get("reproducibility", {}).get("predictions_checksum"),
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate the demand model")
    parser.add_argument("--config", default=None, help="path to ml.yaml (default config/ml.yaml)")
    parser.add_argument("--refresh-dataset", action="store_true",
                        help="rebuild the training dataset from the Gold table first (retraining)")
    args = parser.parse_args()

    from src.common.spark_session import get_spark_session
    spark = get_spark_session("ml-train")
    try:
        cfg = load_ml_config(args.config)
        summary = retrain(spark, cfg, refresh_dataset=args.refresh_dataset)
        print(json.dumps({k: summary[k] for k in ("run_id", "algorithm", "timings", "metrics",
                                                  "importance_by_dataset", "reproducibility")},
                         indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()