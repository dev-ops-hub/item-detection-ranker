import pytest

from item_ranker.main import load_job_module, parse_args


BASE_ARGS = [
    "--job", "task1_etl_job",
    "--dataset_a_path", "data/input/datasetA.parquet",
    "--dataset_b_path", "data/input/datasetB.parquet",
    "--output_path", "data/output/output.parquet",
    "--top-x", "10",
]


def test_parse_args_returns_expected_namespace():
    parsed = parse_args(BASE_ARGS)
    assert parsed.job == "task1_etl_job"
    assert parsed.dataset_a_path == "data/input/datasetA.parquet"
    assert parsed.dataset_b_path == "data/input/datasetB.parquet"
    assert parsed.output_path == "data/output/output.parquet"
    assert parsed.top_x == 10


def test_parse_args_missing_required_arg_exits():
    with pytest.raises(SystemExit):
        parse_args(["--job", "task1_etl_job"])


def test_parse_args_invalid_top_x_exits():
    bad = list(BASE_ARGS)
    bad[bad.index("10")] = "abc"
    with pytest.raises(SystemExit):
        parse_args(bad)


def test_load_job_module_resolves_known_job():
    module = load_job_module("task1_etl_job")
    assert hasattr(module, "run")


def test_load_job_module_strips_py_suffix():
    module = load_job_module("task1_etl_job.py")
    assert hasattr(module, "run")


def test_load_job_module_unknown_job_lists_available():
    with pytest.raises(ModuleNotFoundError) as excinfo:
        load_job_module("does_not_exist_job")
    assert "Available jobs" in str(excinfo.value)
    assert "task1_etl_job" in str(excinfo.value)
