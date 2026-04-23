"""Pytest configuration shared by all tests.

Responsibilities:
    * Make ``src/`` importable so tests can ``import item_ranker`` without
      installing the package.
    * Pin PySpark driver/worker Python to the current interpreter to avoid
      Windows venv mismatch issues.
    * Strip ``SPARK_HOME`` and ``PYTHONSTARTUP`` so workers always use the
      pyspark module shipped with the venv (not a system-wide install).
    * Provide a session-scoped local `SparkSession` fixture configured for
      fast test execution (2 cores, 2 shuffle partitions, UI off).
"""
import os
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

# Ensure Spark driver/workers use the same interpreter (Windows venv safety).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Avoid the venv-pyspark vs SPARK_HOME-bundled-pyspark mismatch
# (e.g. "Can't get attribute 'TimeType'") by forcing workers to use the
# pyspark module installed in this venv.
os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONSTARTUP", None)


@pytest.fixture(scope="session")
def spark():
    """Yield a local SparkSession reused across the whole test session."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("item-ranker-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
