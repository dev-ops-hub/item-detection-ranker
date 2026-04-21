import datetime as dt
from pathlib import Path

import pytest

from item_ranker.config import PipelineConfig
from item_ranker.jobs import task1_etl_job, task2_etl_job


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_A = REPO_ROOT / "data" / "input" / "datasetA.parquet"
DATA_B = REPO_ROOT / "data" / "input" / "datasetB.parquet"

REAL_FIXTURES_PRESENT = DATA_A.exists() and DATA_B.exists()


def _today_dir(base: Path) -> Path:
    return base / dt.date.today().isoformat()


def _assert_valid_ranked_output(rows, top_x: int):
    """Common invariants every ranked output must satisfy."""
    assert rows, "expected at least one ranked output row"
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geographical_location_oid"], []).append(
            r["item_rank"]
        )
    for geo, ranks in by_geo.items():
        ranks_sorted = sorted(ranks)
        assert ranks_sorted == list(range(1, len(ranks_sorted) + 1)), (
            f"non-contiguous ranks for geo {geo}: {ranks_sorted}"
        )
        assert len(ranks_sorted) <= top_x


@pytest.mark.slow
@pytest.mark.skipif(
    not REAL_FIXTURES_PRESENT,
    reason="real input fixtures not present in data/input/",
)
@pytest.mark.parametrize(
    "job_module,label",
    [
        (task1_etl_job, "task1"),
        (task2_etl_job, "task2"),
    ],
)
def test_pipeline_on_real_fixtures(spark, tmp_path, job_module, label):
    top_x = 10
    out_base = tmp_path / f"real_out_{label}"
    config = PipelineConfig(
        dataset_a_path=str(DATA_A),
        dataset_b_path=str(DATA_B),
        output_path=str(out_base / "result.parquet"),
        top_x=top_x,
    )
    job_module.run(spark, config)

    written = _today_dir(out_base) / "result.parquet"
    df = spark.read.parquet(str(written))
    rows = df.collect()
    _assert_valid_ranked_output(rows, top_x=top_x)


@pytest.mark.slow
@pytest.mark.skipif(
    not REAL_FIXTURES_PRESENT,
    reason="real input fixtures not present in data/input/",
)
def test_task1_and_task2_produce_identical_results(spark, tmp_path):
    """Salting must not change the final ranked output."""
    top_x = 10

    def _run(job_module, label):
        out_base = tmp_path / f"compare_{label}"
        config = PipelineConfig(
            dataset_a_path=str(DATA_A),
            dataset_b_path=str(DATA_B),
            output_path=str(out_base / "result.parquet"),
            top_x=top_x,
        )
        job_module.run(spark, config)
        df = spark.read.parquet(
            str(_today_dir(out_base) / "result.parquet")
        )
        return {
            (
                r["geographical_location_oid"],
                r["item_rank"],
                r["item_name"],
                r["geographical_location"],
            )
            for r in df.collect()
        }

    task1_set = _run(task1_etl_job, "t1")
    task2_set = _run(task2_etl_job, "t2")
    assert task1_set == task2_set, (
        f"task1 vs task2 diverged: "
        f"only_in_task1={task1_set - task2_set}, "
        f"only_in_task2={task2_set - task1_set}"
    )
