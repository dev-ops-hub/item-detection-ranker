from item_ranker.jobs.transforms.deduplicator import DeduplicatorTransform


def test_deduplicator_keeps_one_row_per_unique_key(spark):
    rows = [
        (1, 10, 100, "a", 1),
        (1, 10, 100, "a", 1),
        (1, 11, 101, "b", 2),
        (2, 12, 102, "c", 3),
        (2, 12, 102, "c", 3),
    ]
    rdd = spark.sparkContext.parallelize(rows)
    out = DeduplicatorTransform(key_index=2).execute(rdd).collect()
    detection_oids = sorted(r[2] for r in out)
    assert detection_oids == [100, 101, 102]


def test_deduplicator_empty_rdd(spark):
    rdd = spark.sparkContext.parallelize([], numSlices=1)
    # Provide a dummy element then filter out so partitioner is happy
    rdd = rdd.filter(lambda _: False)
    out = DeduplicatorTransform(key_index=0).execute(rdd).collect()
    assert out == []


def test_deduplicator_all_duplicates_collapse_to_one(spark):
    rows = [(1, 1, 999, "x", 1)] * 5
    rdd = spark.sparkContext.parallelize(rows)
    out = DeduplicatorTransform(key_index=2).execute(rdd).collect()
    assert len(out) == 1
