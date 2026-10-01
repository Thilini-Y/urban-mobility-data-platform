"""
Input : the training dataset
Output: Spark ML features: features, label and the row keys

The pipeline is a standard Spark ML Pipeline that is generated from the features section of
config/ml.yaml.

  1. SQLTransformer: temporal features in local time (hour, weekday, month, weekend, holiday),
                      sin/cos encoding of cyclical ones, numeric casts, rule-based missing-value
                      handling
  2. Imputer: median fill for whatever is still missing
  3. StringIndexer:  one-hot encoding of categorical columns
  4. VectorAssembler: numeric block transform to StandardScaler

"""
from __future__ import annotations

import argparse
import json
import os
import time
import holidays as pyholidays
from typing import Optional

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (Imputer, OneHotEncoder, SQLTransformer, StandardScaler,
                                StringIndexer, StringIndexerModel, VectorAssembler)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.ml.config import MLConfig, load_ml_config
from src.ml.training_dataset import SPLIT_COLUMN, TRAIN, load_training_dataset

LABEL_COLUMN = "label"
FEATURES_COLUMN = "features"

# temporal features
TEMPORAL_EXPRESSIONS = {
    "hour_of_day": "hour({local})",
    "day_of_week": "(dayofweek({local}) + 5) % 7",
    "day_of_month": "dayofmonth({local})",
    "week_of_year": "weekofyear({local})",
    "month": "month({local})",
    "is_weekend": "CASE WHEN dayofweek({local}) IN (1, 7) THEN 1 ELSE 0 END",
}


def numeric_input_columns(cfg: MLConfig) -> list:
    # numeric features that are cleaned, imputed and scaled
    return list(dict.fromkeys([*cfg.features.numeric, *cfg.lag_columns, *cfg.rolling_columns]))


def model_numeric_columns(cfg: MLConfig) -> list:
    # columns of the numeric block, in the order they appear in the feature vector
    f = cfg.features
    cols = []
    for name in f.cyclical:
        cols += [f"{name}_sin", f"{name}_cos"]
    cols += [t for t in f.temporal if t not in f.cyclical]
    cols += [f"{c}_imp" for c in numeric_input_columns(cfg)]
    cols += [f"{c}_missing" for c in f.flagged_columns]
    return cols


def categorical_columns(cfg: MLConfig) -> list:
    return list(cfg.features.categorical)



def holiday_dates(cfg: MLConfig) -> list:
    #Public-holiday dates (YYYY-MM-DD) of the configured calendar, plus any extra dates.

    f = cfg.features
    if cfg.data_start and cfg.data_end:
        first, last = int(cfg.data_start[:4]), int(cfg.data_end[:4])
    else:
        first, last = 2010, 2035
    calendar = pyholidays.country_holidays(
        f.holiday_country, subdiv=f.holiday_subdivision, years=range(first, last + 1))
    return sorted({d.isoformat() for d in calendar} | set(f.holiday_extra))


def _temporal_expression(name: str, cfg: MLConfig) -> str:
    local = f"from_utc_timestamp(CAST({cfg.time_column} AS TIMESTAMP), '{cfg.local_timezone}')"
    if name == "is_holiday":
        days = ", ".join(f"'{d}'" for d in holiday_dates(cfg)) or "''"
        return f"CASE WHEN CAST(to_date({local}) AS STRING) IN ({days}) THEN 1 ELSE 0 END"
    if name not in TEMPORAL_EXPRESSIONS:
        raise ValueError(f"Unknown temporal feature '{name}'. "
                         f"Supported: {sorted([*TEMPORAL_EXPRESSIONS, 'is_holiday'])}")
    return TEMPORAL_EXPRESSIONS[name].format(local=local)


def _as_double(column: str) -> str:
    # NaN is treated like NULL
    return f"NULLIF(CAST({column} AS DOUBLE), CAST('NaN' AS DOUBLE))"


def _clean_expression(column: str, cfg: MLConfig) -> str:
    f = cfg.features
    value = _as_double(column)
    if column in f.zero_fill:
        return f"COALESCE({value}, 0.0)"
    if column in f.group_mean_columns:
        group = ", ".join(f.group_mean_by)
        return f"COALESCE({value}, AVG({value}) OVER (PARTITION BY {group}))"
    return value


def build_feature_sql(cfg: MLConfig) -> str:
    f = cfg.features
    parts = ["*"]

    for name in f.temporal:
        parts.append(f"{_temporal_expression(name, cfg)} AS {name}")
    for name, period in f.cyclical.items():
        angle = f"2 * pi() * ({_temporal_expression(name, cfg)}) / {float(period)}"
        parts.append(f"sin({angle}) AS {name}_sin")
        parts.append(f"cos({angle}) AS {name}_cos")

    for column in numeric_input_columns(cfg):
        parts.append(f"{_clean_expression(column, cfg)} AS {column}_clean")
    for column in f.flagged_columns:
        parts.append(f"CAST(CASE WHEN {_as_double(column)} IS NULL THEN 1 ELSE 0 END AS DOUBLE) AS {column}_missing")

    return "SELECT " + ",\n       ".join(parts) + "\nFROM __THIS__"


def build_output_sql(cfg: MLConfig) -> str:
    keep = [cfg.entity_column, cfg.time_column, SPLIT_COLUMN]
    return (f"SELECT {', '.join(keep)}, CAST({cfg.target} AS DOUBLE) AS {LABEL_COLUMN}, "
            f"{FEATURES_COLUMN}\nFROM __THIS__")


def build_feature_pipeline(cfg: Optional[MLConfig] = None) -> Pipeline:
    cfg = cfg or load_ml_config()
    f = cfg.features
    numeric = numeric_input_columns(cfg)

    stages = [
        SQLTransformer(statement=build_feature_sql(cfg)),
        Imputer(strategy="median",
                inputCols=[f"{c}_clean" for c in numeric],
                outputCols=[f"{c}_imp" for c in numeric]),
    ]

    categorical = categorical_columns(cfg)
    one_hot = [f"{c}_ohe" for c in categorical]
    if categorical:
        stages += [
            StringIndexer(inputCols=categorical, outputCols=[f"{c}_idx" for c in categorical],
                          handleInvalid="keep"),
            OneHotEncoder(inputCols=[f"{c}_idx" for c in categorical], outputCols=one_hot,
                          handleInvalid="keep"),
        ]

    stages.append(VectorAssembler(inputCols=model_numeric_columns(cfg), outputCol="numeric_features",
                                  handleInvalid="error"))
    numeric_block = "numeric_features"
    if f.scale:
        stages.append(StandardScaler(inputCol="numeric_features", outputCol="numeric_scaled",
                                     withMean=True, withStd=True))
        numeric_block = "numeric_scaled"

    stages.append(VectorAssembler(inputCols=[numeric_block, *one_hot], outputCol=FEATURES_COLUMN,
                                  handleInvalid="error"))
    stages.append(SQLTransformer(statement=build_output_sql(cfg)))
    return Pipeline(stages=stages)


def fit_feature_pipeline(train_df: DataFrame, cfg: Optional[MLConfig] = None) -> PipelineModel:
    return build_feature_pipeline(cfg).fit(train_df)


def feature_names(model: PipelineModel, cfg: Optional[MLConfig] = None) -> list:
    cfg = cfg or load_ml_config()
    indexer = next((s for s in model.stages if isinstance(s, StringIndexerModel)), None)
    names = list(model_numeric_columns(cfg))
    if indexer is not None:
        # StringIndexer(handleInvalid="keep") adds one bucket for labels unseen in training, and
        # the one-hot vector keeps it: one slot per known label + one "unseen" slot
        for column, labels in zip(indexer.getInputCols(), indexer.labelsArray):
            names += [f"{column}={label}" for label in labels] + [f"{column}=__unseen__"]
    return names


def save_feature_pipeline(model: PipelineModel, cfg: Optional[MLConfig] = None,
                          path: Optional[str] = None) -> str:
    cfg = cfg or load_ml_config()
    path = path or cfg.feature_pipeline_path
    model.write().overwrite().save(path)
    with open(os.path.join(path, "feature_config.json"), "w") as out:
        json.dump({"features": cfg.raw.get("features"), "feature_names": feature_names(model, cfg)},
                  out, indent=2, default=str)
    return path


def load_feature_pipeline(cfg: Optional[MLConfig] = None, path: Optional[str] = None) -> PipelineModel:
    cfg = cfg or load_ml_config()
    return PipelineModel.load(path or cfg.feature_pipeline_path)


def run_feature_pipeline(spark: SparkSession, cfg: Optional[MLConfig] = None) -> dict:
    # Fit on the train split, transform all splits, save the pipeline and the feature dataset
    cfg = cfg or load_ml_config()
    data = load_training_dataset(spark, cfg)

    t0 = time.time()
    model = fit_feature_pipeline(data.filter(F.col(SPLIT_COLUMN) == TRAIN), cfg)
    features = model.transform(data)
    features.write.mode("overwrite").partitionBy(SPLIT_COLUMN).parquet(cfg.feature_dataset_path)
    elapsed = time.time() - t0

    save_feature_pipeline(model, cfg)

    written = spark.read.parquet(cfg.feature_dataset_path)
    names = feature_names(model, cfg)
    summary = {
        "seconds_fit_transform_write": round(elapsed, 2),
        "rows_per_split": {r[SPLIT_COLUMN]: r["n"] for r in
                           written.groupBy(SPLIT_COLUMN).agg(F.count("*").alias("n")).collect()},
        "feature_vector_size": len(names),
        "numeric_features": model_numeric_columns(cfg),
        "categorical_features": categorical_columns(cfg),
        "feature_dataset": cfg.feature_dataset,
        "feature_pipeline": cfg.feature_pipeline,
    }
    os.makedirs(os.path.dirname(cfg.feature_summary_path), exist_ok=True)
    with open(cfg.feature_summary_path, "w") as out:
        json.dump(summary, out, indent=2)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Fit the feature pipeline and build the feature dataset")
    parser.add_argument("--config", default=None, help="path to ml.yaml (default config/ml.yaml)")
    args = parser.parse_args()

    from src.common.spark_session import get_spark_session
    spark = get_spark_session("ml-feature-pipeline")
    try:
        print(json.dumps(run_feature_pipeline(spark, load_ml_config(args.config)), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()