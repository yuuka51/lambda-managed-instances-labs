from .duckdb_engine import run_compare as duckdb_compare
from .duckdb_engine_optimized import run_compare as duckdb_compare_optimized
from .fireducks_engine import run_compare as fireducks_compare
from .pandas_engine import run_compare as pandas_compare
from .pandas_engine_optimized import run_compare as pandas_compare_optimized
from .polars_engine import run_compare as polars_compare
from .polars_engine_optimized import run_compare as polars_compare_optimized
from .spark_engine import run_compare as spark_compare

ENGINE_MAP = {
    "duckdb": duckdb_compare,
    "duckdb-opt": duckdb_compare_optimized,
    "pandas": pandas_compare,
    "pandas-opt": pandas_compare_optimized,
    "fireducks": fireducks_compare,
    "polars": polars_compare,
    "polars-opt": polars_compare_optimized,
    "spark": spark_compare,
}
