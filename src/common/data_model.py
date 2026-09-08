# common data model
from __future__ import annotations

import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


#---standard format---

#timestamp convention
STANDARD_TIMEZONE = "UTC"

EVENT_TIMESTAMP = "event_timestamp"
EVENT_DATE = "event_date"
EVENT_HOUR = "event_hour"

STANDARD_TIMESTAMP_COLUMNS = (EVENT_TIMESTAMP, EVENT_DATE, EVENT_HOUR)

SOURCE_FILE = "source_file"
INGESTION_TIMESTAMP = "ingestion_timestamp"
SCHEMA_VERSION = "schema_version"

LINEAGE_COLUMNS = (SOURCE_FILE, INGESTION_TIMESTAMP, SCHEMA_VERSION)

IS_VALID = "is_valid"
REJECTION_REASON = "rejection_reason"
QUALITY_COLUMNS = (IS_VALID, REJECTION_REASON)

QUARANTINED_AT = "quarantined_at"

# for validating casts
CAST_ERRORS = "_cdm_cast_errors"


# type names allowed in config/datasets.yaml
ALLOWED_SPARK_TYPES = {
    "string", "boolean", "byte", "short", "int", "bigint",
    "float", "double", "date", "timestamp", "timestamp_ntz",
}

TYPE_ALIASES = {
    "integer": "int",
    "long": "bigint",
    "bool": "boolean",
}

# Spark's runtime type name
_RUNTIME_TYPE_NAMES = {"byte": "tinyint", "short": "smallint"}

_DECIMAL_RE = re.compile(r"decimal\((\d+),\s*(\d+)\)")

_INTEGER_TYPES = {"byte", "short", "int", "bigint"}


def normalize_spark_type(dtype: str) -> str:
    value = str(dtype).strip().lower()
    value = TYPE_ALIASES.get(value, value)

    if value in ALLOWED_SPARK_TYPES:
        return value

    match = _DECIMAL_RE.fullmatch(value)
    if match:
        precision, scale = map(int, match.groups())
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return f"decimal({precision},{scale})"

    raise ValueError(f"Unsupported common data type: {dtype}")


def is_valid_spark_type(dtype: str) -> bool:
    try:
        normalize_spark_type(dtype)
        return True
    except ValueError:
        return False


def _runtime_type_name(dtype: str) -> str:
    return _RUNTIME_TYPE_NAMES.get(dtype, dtype)


#---naming conventions---

_FIRST_CAP = re.compile(r"(.)([A-Z][a-z]+)")
_ALL_CAP = re.compile(r"([a-z0-9])([A-Z])")
_NON_WORD = re.compile(r"[^0-9a-zA-Z]+")
_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


def to_snake_case(name: str) -> str:
    name = name.strip()
    name = _NON_WORD.sub("_", name)
    name = _FIRST_CAP.sub(r"\1_\2", name)
    name = _ALL_CAP.sub(r"\1_\2", name)
    return re.sub(r"_+", "_", name).strip("_").lower()


def is_valid_column_name(name: str) -> bool:
    return bool(isinstance(name, str) and _SNAKE_CASE.fullmatch(name))


def standardize_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    names = [column_mapping.get(source, to_snake_case(source)) for source in df.columns]

    if len(names) != len(set(n.lower() for n in names)):
        raise ValueError("Column mapping produces duplicate names")

    for name in names:
        if not is_valid_column_name(name):
            raise ValueError(f"Invalid column name: {name}")

    return df.toDF(*names)


# ---handling missing values---

# Nulls are invalid by default unless allow_null is true.
# Weather precipitation and taxi passenger_count allow nulls.
# Primary keys must never be null.
DEFAULT_ALLOW_NULL = False


# converts empty strings into null values
def normalize_missing_values(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            value = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(value == "", F.lit(None)).otherwise(value))
    return df


def _nonfinite(value):
    number = value.cast("double")
    return F.isnan(number) | (F.abs(number) == F.lit(float("inf")))

# Convert configured columns to their target types, recording invalid values for quality checks
def change_datatype(df: DataFrame, dtype_overrides: dict) -> DataFrame:
    df = df.withColumn(CAST_ERRORS, F.array().cast("array<string>"))

    for name, dtype in dtype_overrides.items():
        if name not in df.columns:
            raise ValueError(f"Column '{name}' not found")

        target = normalize_spark_type(dtype)
        original = F.col(name)

        quoted = "`" + name.replace("`", "``") + "`"
        converted = F.expr(f"try_cast({quoted} AS {target})")

        bad = original.isNotNull() & converted.isNull()

        numeric = target in {"byte", "short", "int", "bigint", "float", "double"} or target.startswith("decimal(")

        if numeric:
            bad = bad | _nonfinite(converted)
            if target in _INTEGER_TYPES:
                # Reject fractional values instead of silently truncating them.
                number = F.expr(f"try_cast({quoted} AS double)")
                bad = bad | (number.isNotNull() & (number != F.floor(number)))
            reason = f"invalid_numeric:{name}"
        elif target in {"timestamp", "date", "timestamp_ntz"}:
            reason = f"invalid_timestamp:{name}"
        else:
            reason = f"invalid_type:{name}"

        bad = F.coalesce(bad, F.lit(False))

        df = df.withColumn(
            CAST_ERRORS,
            F.concat(F.col(CAST_ERRORS), F.array(F.when(bad, F.lit(reason)))),
        )
        df = df.withColumn(name, F.when(bad, F.lit(None).cast(target)).otherwise(converted))

    return df.withColumn(CAST_ERRORS, F.filter(F.col(CAST_ERRORS), lambda x: x.isNotNull()))


# timestamp construction and normalization

_TIMESTAMP_FORMATS = (
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss",
)


def parse_timestamp(value):
    text = value.cast("string")
    return F.coalesce(*[F.try_to_timestamp(text, F.lit(fmt)) for fmt in _TIMESTAMP_FORMATS])


def normalize_timestamp(value, source_timezone: str):
    if not source_timezone:
        raise ValueError("The source timezone must be configured and verified")
    try:
        ZoneInfo(source_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown source timezone: {source_timezone}") from exc

    parsed = parse_timestamp(value)
    if source_timezone == "UTC":
        return parsed
    return F.to_utc_timestamp(parsed, source_timezone)


def timestamp_from_parts(year, month, day, hour):
    valid = year.between(1, 9999) & month.between(1, 12) & day.between(1, 31) & hour.between(0, 23)
    text = F.format_string("%04d-%02d-%02d %02d:00:00", year, month, day, hour)
    return F.when(valid, parse_timestamp(text))


def add_event_time(df: DataFrame, instant) -> DataFrame:
    if df.sparkSession.conf.get("spark.sql.session.timeZone") != STANDARD_TIMEZONE:
        raise ValueError("Spark session timezone must be UTC")

    return (
        df.withColumn(EVENT_TIMESTAMP, instant)
        .withColumn(EVENT_DATE, F.to_date(F.col(EVENT_TIMESTAMP)))
        .withColumn(EVENT_HOUR, F.hour(F.col(EVENT_TIMESTAMP)))
    )


def has_temporal_dimension(columns) -> bool:
    return EVENT_TIMESTAMP in columns


def add_lineage_columns(df: DataFrame, source_file: str, schema_version: str) -> DataFrame:
    return (
        df.withColumn(SOURCE_FILE, F.lit(source_file))
        .withColumn(INGESTION_TIMESTAMP, F.current_timestamp())
        .withColumn(SCHEMA_VERSION, F.lit(schema_version))
    )


# row level quality and missing value rules
def common_quality_reasons(df: DataFrame, pk_columns: list, numeric_rules: dict,
                            required_columns=(), extra_checks=()):
    checks = list(extra_checks)
    required_columns = required_columns or []

    for name in dict.fromkeys([*pk_columns, *required_columns, *numeric_rules]):
        if name not in df.columns:
            raise ValueError(f"column '{name}' not found")

    for name in pk_columns:
        checks.append((F.col(name).isNull(), f"missing_pk:{name}"))

    if pk_columns:
        window = Window.partitionBy(*pk_columns)
        df = df.withColumn("_cdm_pk_count", F.count(F.lit(1)).over(window))
        checks.append((F.col("_cdm_pk_count") > 1, "duplicate_pk"))

    if EVENT_TIMESTAMP in df.columns:
        checks.append((F.col(EVENT_TIMESTAMP).isNull(), "invalid_timestamp"))

    for name in required_columns:
        if name not in pk_columns and name != EVENT_TIMESTAMP:
            checks.append((F.col(name).isNull(), f"missing_required:{name}"))

    for name, bounds in numeric_rules.items():
        value = F.col(name)
        allow_null = bounds.get("allow_null", DEFAULT_ALLOW_NULL)
        bad = F.lit(False) if allow_null else value.isNull()
        if "min" in bounds:
            bad = bad | (value < F.lit(bounds["min"]))
        if "max" in bounds:
            bad = bad | (value > F.lit(bounds["max"]))
        bad = bad | _nonfinite(value)
        checks.append((F.coalesce(bad, F.lit(False)), f"invalid_numeric:{name}"))

    reasons = (
        F.array(*[F.when(condition, F.lit(reason)) for condition, reason in checks])
        if checks else F.array().cast("array<string>")
    )

    if CAST_ERRORS in df.columns:
        reasons = F.concat(reasons, F.col(CAST_ERRORS))

    return df, F.array_distinct(F.filter(reasons, lambda x: x.isNotNull()))


def apply_quality_checks(df: DataFrame, pk_columns: list, numeric_rules: dict,
                          required_columns=(), extra_checks=()) -> DataFrame:
    df, reasons = common_quality_reasons(df, pk_columns, numeric_rules, required_columns, extra_checks)
    df = df.withColumn("_reasons", reasons)
    df = df.withColumn(
        REJECTION_REASON,
        F.when(F.size(F.col("_reasons")) == 0, None).otherwise(F.array_join(F.col("_reasons"), ", ")),
    )
    df = df.withColumn(IS_VALID, F.col(REJECTION_REASON).isNull())
    df = df.drop("_reasons")
    for helper_column in (CAST_ERRORS, "_cdm_pk_count"):
        if helper_column in df.columns:
            df = df.drop(helper_column)
    return df


# Runtime schema conformance check

COMMON_FIELD_TYPES = {
    SOURCE_FILE: "string",
    INGESTION_TIMESTAMP: "timestamp",
    SCHEMA_VERSION: "string",
    IS_VALID: "boolean",
    REJECTION_REASON: "string",
}

TEMPORAL_FIELD_TYPES = {
    EVENT_TIMESTAMP: "timestamp",
    EVENT_DATE: "date",
    EVENT_HOUR: "int",
}


def validate_common_schema(df: DataFrame, cfg) -> None:
    if len(df.columns) != len(set(df.columns)):
        raise ValueError(f"{cfg.name}: duplicate column names")

    actual = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    for name, dtype in actual.items():
        if not is_valid_column_name(name):
            raise ValueError(f"{cfg.name}: invalid column name '{name}'")
        if not is_valid_spark_type(dtype):
            raise ValueError(f"{cfg.name}: unsupported type '{dtype}' for '{name}'")

    expected = dict(COMMON_FIELD_TYPES)
    if has_temporal_dimension(df.columns):
        expected.update(TEMPORAL_FIELD_TYPES)
    expected.update({name: normalize_spark_type(dtype) for name, dtype in cfg.dtype_overrides.items()})

    for name, dtype in expected.items():
        if name not in actual:
            raise ValueError(f"{cfg.name}: missing model column '{name}'")
        if actual[name] != _runtime_type_name(dtype):
            raise ValueError(f"{cfg.name}: '{name}' must be {dtype}, but is {actual[name]}")