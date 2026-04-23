"""Unit tests for `item_ranker.main`'s argparse layer and job loader."""
import os
import sys

import pytest

import item_ranker.main as main_module
from item_ranker.main import load_job_module, parse_args


# A complete, valid CLI invocation reused by several tests below.
BASE_ARGS = [
    "--job", "task1_etl_job",
    "--dataset_a_path", "data/input/datasetA.parquet",
    "--dataset_b_path", "data/input/datasetB.parquet",
    "--output_path", "data/output/output.parquet",
    "--top-x", "10",
]


def test_parse_args_returns_expected_namespace():
    """All five CLI flags map to attributes of the parsed namespace."""
    parsed = parse_args(BASE_ARGS)
    assert parsed.job == "task1_etl_job"
    assert parsed.dataset_a_path == "data/input/datasetA.parquet"
    assert parsed.dataset_b_path == "data/input/datasetB.parquet"
    assert parsed.output_path == "data/output/output.parquet"
    assert parsed.top_x == 10


def test_parse_args_missing_required_arg_exits():
    """argparse exits when any required argument is omitted."""
    with pytest.raises(SystemExit):
        parse_args(["--job", "task1_etl_job"])


def test_parse_args_invalid_top_x_exits():
    """`--top-x` is typed as int; non-numeric values cause SystemExit."""
    bad = list(BASE_ARGS)
    bad[bad.index("10")] = "abc"
    with pytest.raises(SystemExit):
        parse_args(bad)


def test_load_job_module_resolves_known_job():
    """A bare job name resolves to the module under item_ranker.jobs."""
    module = load_job_module("task1_etl_job")
    assert hasattr(module, "run")


def test_load_job_module_strips_py_suffix():
    """Trailing `.py` on the --job value is stripped before import."""
    module = load_job_module("task1_etl_job.py")
    assert hasattr(module, "run")


def test_load_job_module_unknown_job_lists_available():
    """Unknown job names raise with a helpful list of valid jobs."""
    with pytest.raises(ModuleNotFoundError) as excinfo:
        load_job_module("does_not_exist_job")
    assert "Available jobs" in str(excinfo.value)
    assert "task1_etl_job" in str(excinfo.value)
