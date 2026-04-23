"""Unit tests for `RankingTransform` ordering, ties, and truncation."""
from item_ranker.jobs.transforms.ranking import RankingTransform


def test_ranking_orders_descending_by_count(spark):
    """Higher counts get lower (better) ranks."""
    rdd = spark.sparkContext.parallelize([
        ((1, "apple"), 5),
        ((1, "banana"), 10),
        ((1, "cherry"), 1),
    ])
    out = sorted(RankingTransform(top_x=3).execute(rdd).collect(),
                 key=lambda r: r[1])
    assert out == [
        (1, 1, "banana"),
        (1, 2, "apple"),
        (1, 3, "cherry"),
    ]


def test_ranking_breaks_ties_alphabetically(spark):
    """Equal counts are ordered by item name ascending (deterministic)."""
    rdd = spark.sparkContext.parallelize([
        ((1, "banana"), 5),
        ((1, "apple"), 5),
        ((1, "cherry"), 5),
    ])
    out = sorted(RankingTransform(top_x=3).execute(rdd).collect(),
                 key=lambda r: r[1])
    assert out == [
        (1, 1, "apple"),
        (1, 2, "banana"),
        (1, 3, "cherry"),
    ]


def test_ranking_truncates_to_top_x(spark):
    """Only the top X items per group survive the truncation."""
    rdd = spark.sparkContext.parallelize([
        ((1, "a"), 4),
        ((1, "b"), 3),
        ((1, "c"), 2),
        ((1, "d"), 1),
    ])
    out = sorted(RankingTransform(top_x=2).execute(rdd).collect(),
                 key=lambda r: r[1])
    assert out == [(1, 1, "a"), (1, 2, "b")]


def test_ranking_handles_groups_independently(spark):
    """Each group_key (geo_oid) is ranked independently."""
    rdd = spark.sparkContext.parallelize([
        ((1, "a"), 2),
        ((1, "b"), 1),
        ((2, "x"), 10),
    ])
    out = sorted(RankingTransform(top_x=5).execute(rdd).collect())
    assert out == [
        (1, 1, "a"),
        (1, 2, "b"),
        (2, 1, "x"),
    ]
