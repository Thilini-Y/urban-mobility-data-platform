import re

_first_cap_re = re.compile(r"(.)([A-Z][a-z]+)")
_all_cap_re = re.compile(r"([a-z0-9])([A-Z])")
_non_word_re = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    name = name.strip()
    name = _non_word_re.sub("_", name)
    name = _first_cap_re.sub(r"\1_\2", name)
    name = _all_cap_re.sub(r"\1_\2", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def standardize_columns(df, column_mapping: dict):
    for source_col in df.columns:
        target_col = column_mapping.get(source_col, to_snake_case(source_col))
        if target_col != source_col:
            df = df.withColumnRenamed(source_col, target_col)
    return df