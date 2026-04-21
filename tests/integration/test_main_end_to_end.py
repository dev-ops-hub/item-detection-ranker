"""End-to-end test that drives main() via subprocess.

Using a subprocess isolates the SparkSession lifecycle (main() calls
spark.stop() in its finally block, which would otherwise tear down the
session-scoped fixture used by other Spark tests).
"""
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import pytest

from item_ranker.jobs.schema.mapping import DATASETA_SCHEMA, DATASETB_SCHEMA


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"


@pytest.fixture
def cli_inputs(spark, tmp_path):
    rows_a = [
        (1, 10, 1, "apple", 1),
        (1, 10, 1, "apple", 1),  # duplicate detection_oid
        (1, 10, 2, "banana", 1),
        (2, 20, 3, "cat", 1),
    ]
    rows_b = [(1, "Alpha"), (2, "Beta")]
    a = tmp_path / "A.parquet"
    b = tmp_path / "B.parquet"
    spark.createDataFrame(rows_a, schema=DATASETA_SCHEMA) \
        .write.parquet(str(a))
    spark.createDataFrame(rows_b, schema=DATASETB_SCHEMA) \
        .write.parquet(str(b))
    return a, b


def test_main_runs_full_pipeline_via_subprocess(spark, tmp_path, cli_inputs):
    a, b = cli_inputs
    out_base = tmp_path / "out"
    out_path = out_base / "result.parquet"

    env = {**os.environ, "PYTHONPATH": str(SRC_ROOT)}
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    result = subprocess.run(
        [
            sys.executable, "-m", "item_ranker.main",
            "--job", "task1_etl_job",
            "--dataset_a_path", str(a),
            "--dataset_b_path", str(b),
            "--output_path", str(out_path),
            "--top-x", "5",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        cwd=str(REPO_ROOT),
    )

    assert result.returncode == 0, (
        f"main exited non-zero.\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    # main() traps exceptions and logs FATAL — guard against silent failure.
    assert "FATAL" not in result.stdout, result.stdout
    assert "FATAL" not in result.stderr, result.stderr

    written = out_base / dt.date.today().isoformat() / "result.parquet"
    assert written.exists(), f"Expected output at {written}"

    df = spark.read.parquet(str(written))
    rows = df.collect()
    geo_oids = sorted({r["geographical_location_oid"] for r in rows})
    assert geo_oids == [1, 2]
    # geo 1 should have apple (1, dup collapsed) and banana (1) -> 2 rows
    geo1_rows = [r for r in rows if r["geographical_location_oid"] == 1]
    assert len(geo1_rows) == 2
    assert {r["item_name"] for r in geo1_rows} == {"apple", "banana"}
