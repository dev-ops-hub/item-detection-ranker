import datetime as dt
from pathlib import Path

import pytest

from item_ranker.config import PipelineConfig
from item_ranker.jobs import task1_etl_job
from item_ranker.jobs.schema.mapping import (
    DATASETA_SCHEMA,
    DATASETB_SCHEMA,
    OUTPUT_SCHEMA,
)


@pytest.fixture
def synthetic_inputs(spark, tmp_path):
    rows_a = [
        # geo 1: apple x3 (one duplicated detection_oid),
        #        banana x2, cherry x1
        (1, 10, 1001, "apple", 1),
        (1, 10, 1001, "apple", 1),   # duplicate -> must collapse
        (1, 10, 1002, "apple", 1),
        (1, 10, 1003, "apple", 1),
        (1, 11, 1004, "banana", 1),
        (1, 11, 1005, "banana", 1),
        (1, 11, 1006, "cherry", 1),
        # geo 2: dog x2, cat x1
        (2, 20, 2001, "dog", 1),
        (2, 20, 2002, "dog", 1),
        (2, 20, 2003, "cat", 1),
        # geo 3: NOT in dataset B -> enrichment yields None
        (3, 30, 3001, "fox", 1),
    ]
    rows_b = [
        (1, "Alpha"),
        (2, "Beta"),
    ]

    a_path = tmp_path / "datasetA.parquet"
    b_path = tmp_path / "datasetB.parquet"
    spark.createDataFrame(rows_a, schema=DATASETA_SCHEMA) \
        .write.parquet(str(a_path))
    spark.createDataFrame(rows_b, schema=DATASETB_SCHEMA) \
        .write.parquet(str(b_path))

    return a_path, b_path


def _today_dir(base: Path) -> Path:
    return base / dt.date.today().isoformat()


def test_task1_etl_job_end_to_end(spark, tmp_path, synthetic_inputs):
    a_path, b_path = synthetic_inputs
    out_base = tmp_path / "out"
    out_path = out_base / "result.parquet"

    config = PipelineConfig(
        dataset_a_path=str(a_path),
        dataset_b_path=str(b_path),
        output_path=str(out_path),
        top_x=2,
    )

    task1_etl_job.run(spark, config)

    written = _today_dir(out_base) / "result.parquet"
    assert written.exists(), f"Expected output at {written}"

    df = spark.read.parquet(str(written))
    assert df.schema.fieldNames() == OUTPUT_SCHEMA.fieldNames()

    rows = [r.asDict() for r in df.collect()]
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geographical_location_oid"], []).append(r)

    # geo 1: top-2 -> apple (3, dup collapsed), banana (2)
    geo1 = sorted(by_geo[1], key=lambda r: r["item_rank"])
    assert [r["item_rank"] for r in geo1] == [1, 2]
    assert [r["item_name"] for r in geo1] == ["apple", "banana"]
    assert all(r["geographical_location"] == "Alpha" for r in geo1)

    # geo 2: top-2 -> dog (2), cat (1)
    geo2 = sorted(by_geo[2], key=lambda r: r["item_rank"])
    assert [r["item_name"] for r in geo2] == ["dog", "cat"]
    assert all(r["geographical_location"] == "Beta" for r in geo2)

    # geo 3: only one item, location name is None (not in dataset B)
    geo3 = by_geo[3]
    assert len(geo3) == 1
    assert geo3[0]["item_name"] == "fox"
    assert geo3[0]["item_rank"] == 1
    assert geo3[0]["geographical_location"] is None


def test_task1_etl_job_respects_top_x(spark, tmp_path, synthetic_inputs):
    a_path, b_path = synthetic_inputs
    out_base = tmp_path / "out_topx1"
    config = PipelineConfig(
        dataset_a_path=str(a_path),
        dataset_b_path=str(b_path),
        output_path=str(out_base / "result.parquet"),
        top_x=1,
    )
    task1_etl_job.run(spark, config)

    df = spark.read.parquet(
        str(_today_dir(out_base) / "result.parquet")
    )
    rows = df.collect()
    # one row per geo_oid (3 geos, top-1 each)
    assert sorted(r["geographical_location_oid"] for r in rows) == [1, 2, 3]
    assert all(r["item_rank"] == 1 for r in rows)
