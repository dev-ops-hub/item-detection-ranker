"""Unit tests for `SaltedAggregatorTransform`.

Salting must be invisible to the caller: final counts must equal those
produced by the plain `AggregatorTransform`. Only the shuffle pattern
changes, never the result.
"""
from item_ranker.jobs.transforms.salted_aggregator import (
    SaltedAggregatorTransform,
)


def test_salted_aggregator_matches_plain_counts(spark):
    """Salting must not change final counts, only shuffle layout."""
    rows = [
        (1, 10, 100, "apple", 1),
        (1, 10, 101, "apple", 1),
        (1, 10, 102, "apple", 1),
        (1, 10, 103, "banana", 1),
        (2, 20, 200, "apple", 1),
    ]
    rdd = spark.sparkContext.parallelize(rows)
    out = dict(
        SaltedAggregatorTransform(
            key_indices=(0, 3), num_salts=4
        ).execute(rdd).collect()
    )
    assert out == {
        (1, "apple"): 3,
        (1, "banana"): 1,
        (2, "apple"): 1,
    }


def test_salted_aggregator_handles_hot_key(spark):
    """A single hot key (high cardinality) still aggregates correctly."""
    rows = [(1, 0, i, "hot", 1) for i in range(50)]
    rdd = spark.sparkContext.parallelize(rows, numSlices=4)
    out = dict(
        SaltedAggregatorTransform(
            key_indices=(0, 3), num_salts=8
        ).execute(rdd).collect()
    )
    assert out == {(1, "hot"): 50}


def test_salted_aggregator_single_row(spark):
    """A single input row produces a single (key, 1) output entry."""
    rdd = spark.sparkContext.parallelize([(1, 0, 0, "x", 0)])
    out = SaltedAggregatorTransform(
        key_indices=(0, 3), num_salts=2
    ).execute(rdd).collect()
    assert out == [((1, "x"), 1)]
