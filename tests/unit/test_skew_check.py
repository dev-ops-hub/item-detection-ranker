"""Unit tests for `task2_etl_job.check_data_skew`."""
from item_ranker.jobs.quality.skew_validator import DataSkewValidator


def test_check_data_skew_flags_skewed_rdd(spark):
    """An obviously unbalanced RDD must report skew (factor > 1.5)."""
    # Build an intentionally unbalanced RDD: one partition with 100 rows,
    # three partitions with 1 row each. parallelize() distributes
    # round-robin within a single call, so use union() to control layout.
    big = spark.sparkContext.parallelize([1] * 100, numSlices=1)
    small = spark.sparkContext.parallelize([2, 3, 4], numSlices=3)
    rdd = big.union(small)
    assert rdd.getNumPartitions() == 4

    is_skewed, factor = DataSkewValidator.check_data_skew(rdd)
    assert is_skewed is True
    assert factor > 1.5


def test_check_data_skew_returns_false_for_balanced_rdd(spark):
    """A uniformly distributed RDD must NOT be flagged as skewed."""
    # 20 rows evenly across 4 partitions
    rdd = spark.sparkContext.parallelize(list(range(20)), numSlices=4)
    is_skewed, factor = DataSkewValidator.check_data_skew(rdd)
    assert is_skewed is False
    assert factor <= 1.5
