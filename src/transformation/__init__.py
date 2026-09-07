# register data transformation functions
from importlib import import_module


def get_transform(module_name: str):
    module = import_module(f"src.transformation.{module_name}")
    if not hasattr(module, "transform"):
        raise AttributeError(f"src/transformation/{module_name}.py must define a transform(df, spark) function")
    return module.transform