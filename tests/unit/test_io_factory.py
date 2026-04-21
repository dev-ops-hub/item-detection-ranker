import pytest

from item_ranker.io.factory_rdd import RDDIOFactory


def test_read_rdd_unsupported_format_raises():
    with pytest.raises(ValueError, match="Unsupported format"):
        RDDIOFactory.read_rdd(None, "bogus", "x")


def test_write_rdd_unsupported_format_raises():
    with pytest.raises(ValueError, match="Unsupported format"):
        RDDIOFactory.write_rdd(None, None, "bogus", "x", None)


def test_read_rdd_format_lookup_is_case_insensitive(spark, tmp_path):
    df = spark.createDataFrame([(1, "a")], ["k", "v"])
    target = str(tmp_path / "x.parquet")
    df.write.parquet(target)

    rdd = RDDIOFactory.read_rdd(spark, "PARQUET", target)
    assert rdd.count() == 1
