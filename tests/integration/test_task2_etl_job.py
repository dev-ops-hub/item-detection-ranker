"""Integration tests for task2_etl_job.

task2_etl_job is functionally equivalent to task1_etl_job but adds:
  - data-skew detection (check_data_skew)
  - SaltedAggregatorTransform when skew is detected (otherwise plain
    AggregatorTransform), matching skew_factor to num_salts.

Final output must be identical to task1 for any given input — salting only
affects shuffle distribution, not result counts.
"""
import datetime as dt
from pathlib import Path

import pytest

from item_ranker.config import PipelineConfig
from item_ranker.jobs import task2_etl_job
from item_ranker.jobs.schema.mapping import (
    DATASETA_SCHEMA,
    DATASETB_SCHEMA,
    OUTPUT_SCHEMA,
)


def _today_dir(base: Path) -> Path:
    return base / dt.date.today().isoformat()


@pytest.fixture
def synthetic_inputs(spark, tmp_path):
    rows_a = [
        (1, 10, 1001, "apple", 1),
        (1, 10, 1001, "apple", 1),   # duplicate -> must collapse
        (1, 10, 1002, "apple", 1),
        (1, 10, 1003, "apple", 1),
        (1, 11, 1004, "banana", 1),
        (1, 11, 1005, "banana", 1),
        (1, 11, 1006, "cherry", 1),
        (2, 20, 2001, "dog", 1),
        (2, 20, 2002, "dog", 1),
        (2, 20, 2003, "cat", 1),
        (3, 30, 3001, "fox", 1),  # geo not in dataset B
    ]
    rows_b = [(1, "Alpha"), (2, "Beta")]

    a_path = tmp_path / "datasetA.parquet"
    b_path = tmp_path / "datasetB.parquet"
    spark.createDataFrame(rows_a, schema=DATASETA_SCHEMA) \
        .write.parquet(str(a_path))
    spark.createDataFrame(rows_b, schema=DATASETB_SCHEMA) \
        .write.parquet(str(b_path))
    return a_path, b_path


@pytest.fixture
def skewed_inputs(spark, tmp_path):
    """Force the skew branch by concentrating most rows on geo 1."""
    rows_a = [(1, 10, i, "hot", 1) for i in range(200)]
    rows_a += [(2, 20, 9001, "cold", 1), (2, 20, 9002, "cold", 1)]
    rows_b = [(1, "Alpha"), (2, "Beta")]

    a_path = tmp_path / "datasetA_skewed.parquet"
    b_path = tmp_path / "datasetB.parquet"
    spark.createDataFrame(rows_a, schema=DATASETA_SCHEMA) \
        .write.parquet(str(a_path))
    spark.createDataFrame(rows_b, schema=DATASETB_SCHEMA) \
        .write.parquet(str(b_path))
    return a_path, b_path


def test_task2_etl_job_end_to_end_balanced(spark, tmp_path, synthetic_inputs):
    a_path, b_path = synthetic_inputs
    out_base = tmp_path / "out"
    config = PipelineConfig(
        dataset_a_path=str(a_path),
        dataset_b_path=str(b_path),
        output_path=str(out_base / "result.parquet"),
        top_x=2,
    )
    task2_etl_job.run(spark, config)

    written = _today_dir(out_base) / "result.parquet"
    assert written.exists()

    df = spark.read.parquet(str(written))
    assert df.schema.fieldNames() == OUTPUT_SCHEMA.fieldNames()

    rows = [r.asDict() for r in df.collect()]
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geographical_location_oid"], []).append(r)

    geo1 = sorted(by_geo[1], key=lambda r: r["item_rank"])
    assert [r["item_name"] for r in geo1] == ["apple", "banana"]
    assert all(r["geographical_location"] == "Alpha" for r in geo1)

    geo2 = sorted(by_geo[2], key=lambda r: r["item_rank"])
    assert [r["item_name"] for r in geo2] == ["dog", "cat"]

    assert len(by_geo[3]) == 1
    assert by_geo[3][0]["geographical_location"] is None


def test_task2_etl_job_handles_skewed_data(spark, tmp_path, skewed_inputs):
    """Salted aggregation path must produce correct counts on skewed data."""
    a_path, b_path = skewed_inputs
    out_base = tmp_path / "out_skew"
    config = PipelineConfig(
        dataset_a_path=str(a_path),
        dataset_b_path=str(b_path),
        output_path=str(out_base / "result.parquet"),
        top_x=5,
    )
    task2_etl_job.run(spark, config)

    written = _today_dir(out_base) / "result.parquet"
    df = spark.read.parquet(str(written))
    rows = [r.asDict() for r in df.collect()]

    by_geo = {r["geographical_location_oid"]: r for r in rows}

    # geo 1: 200 unique detection_oids of "hot" -> rank 1
    assert by_geo[1]["item_name"] == "hot"
    assert by_geo[1]["item_rank"] == 1
    assert by_geo[1]["geographical_location"] == "Alpha"

    # geo 2: 2 unique detection_oids of "cold" -> rank 1
    assert by_geo[2]["item_name"] == "cold"
    assert by_geo[2]["item_rank"] == 1
