"""Unit tests for `AggregatorTransform`."""
from item_ranker.jobs.transforms.aggregator import AggregatorTransform


def test_aggregator_counts_by_composite_key(spark):
    """Counts are grouped by the composite key (geo_oid, item_name)."""
    rows = [
        (1, 10, 100, "apple", 1),
        (1, 10, 101, "apple", 1),
        (1, 10, 102, "banana", 1),
        (2, 20, 200, "apple", 1),
    ]
    rdd = spark.sparkContext.parallelize(rows)
    out = dict(
        AggregatorTransform(key_indices=(0, 3)).execute(rdd).collect()
    )
    assert out == {
        (1, "apple"): 2,
        (1, "banana"): 1,
        (2, "apple"): 1,
    }


def test_aggregator_single_row_returns_count_one(spark):
    """A single input row produces a single (key, 1) output entry."""
    rdd = spark.sparkContext.parallelize([(1, 0, 0, "x", 0)])
    out = AggregatorTransform(key_indices=(0, 3)).execute(rdd).collect()
    assert out == [((1, "x"), 1)]
