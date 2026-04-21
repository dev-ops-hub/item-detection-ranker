from item_ranker.jobs.transforms.enricher import EnricherTransform


def test_enricher_inserts_lookup_value_at_position(spark):
    bcast = spark.sparkContext.broadcast({1: "Alpha", 2: "Beta"})
    rdd = spark.sparkContext.parallelize([
        (1, 1, "apple"),
        (2, 1, "banana"),
    ])
    out = sorted(
        EnricherTransform(
            enrich_data=bcast,
            pri_key_index=0,
            insert_pos=1,
        ).execute(rdd).collect()
    )
    assert out == [
        (1, "Alpha", 1, "apple"),
        (2, "Beta", 1, "banana"),
    ]


def test_enricher_inserts_none_when_key_missing(spark):
    bcast = spark.sparkContext.broadcast({1: "Alpha"})
    rdd = spark.sparkContext.parallelize([(99, 1, "x")])
    out = EnricherTransform(
        enrich_data=bcast, pri_key_index=0, insert_pos=1
    ).execute(rdd).collect()
    assert out == [(99, None, 1, "x")]
