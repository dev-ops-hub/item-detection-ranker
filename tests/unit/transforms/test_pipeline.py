import pytest

from item_ranker.jobs.transforms.base import RDDTransformation
from item_ranker.jobs.transforms.pipeline import TransformationPipeline


class _AddOne(RDDTransformation):
    def execute(self, rdd):
        return rdd.map(lambda x: x + 1)


class _Double(RDDTransformation):
    def execute(self, rdd):
        return rdd.map(lambda x: x * 2)


def test_pipeline_runs_transforms_in_order(spark):
    rdd = spark.sparkContext.parallelize([1, 2, 3])
    out = sorted(
        TransformationPipeline([_AddOne(), _Double()]).run(rdd).collect()
    )
    assert out == [4, 6, 8]


def test_pipeline_rejects_non_transform():
    with pytest.raises(TypeError):
        TransformationPipeline([_AddOne(), object()])
