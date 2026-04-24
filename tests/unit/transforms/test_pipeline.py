"""Unit tests for `TransformationPipeline`'s ordering and type checks."""
import pytest

from item_ranker.jobs.transforms.base import RDDTransformation
from item_ranker.jobs.transforms.pipeline import TransformationPipeline


class _FakeLogger:
    """Capture formatted log messages for assertions."""

    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(message % args)


class _AddOne(RDDTransformation):
    """Trivial transform that adds 1 to each element."""

    def execute(self, rdd):
        return rdd.map(lambda x: x + 1)


class _Double(RDDTransformation):
    """Trivial transform that doubles each element."""

    def execute(self, rdd):
        return rdd.map(lambda x: x * 2)


def test_pipeline_runs_transforms_in_order(spark):
    """Transforms are applied in list order: AddOne then Double."""
    rdd = spark.sparkContext.parallelize([1, 2, 3])
    out = sorted(
        TransformationPipeline([_AddOne(), _Double()]).run(rdd).collect()
    )
    assert out == [4, 6, 8]


def test_pipeline_rejects_non_transform():
    """Constructing with a non-RDDTransformation member raises TypeError."""
    with pytest.raises(TypeError):
        TransformationPipeline([_AddOne(), object()])


def test_pipeline_logs_stage_metrics_when_enabled(spark):
    """Metrics mode logs one message per transform with row counts."""
    logger = _FakeLogger()
    rdd = spark.sparkContext.parallelize([1, 2, 3])

    out = TransformationPipeline([_AddOne(), _Double()]).run(
        rdd,
        logger=logger,
        enable_metrics=True,
        initial_rows=3,
    )

    assert sorted(out.collect()) == [4, 6, 8]
    assert len(logger.messages) == 2
    assert "Pipeline stage 1/2 [_AddOne] input_rows=3 output_rows=3" in (
        logger.messages[0]
    )
    assert "Pipeline stage 2/2 [_Double] input_rows=3 output_rows=3" in (
        logger.messages[1]
    )

    out.unpersist()
