import os
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

# Ensure Spark driver/workers use the same interpreter (Windows venv safety).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Avoid the venv-pyspark vs SPARK_HOME-bundled-pyspark mismatch
# (e.g. "Can't get attribute 'TimeType'") by forcing workers to use the
# pyspark module installed in this venv.
os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONSTARTUP", None)


@pytest.fixture(scope="session")
def spark():
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
