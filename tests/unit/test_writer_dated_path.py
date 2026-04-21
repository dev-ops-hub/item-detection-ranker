from item_ranker.io.writer_rdd import ParquetRDDWriter


def _writer(date_str="2026-04-21"):
    return ParquetRDDWriter(is_overwrite=True, run_date=date_str)


def test_dated_path_appends_date_when_path_ends_with_slash():
    w = _writer()
    assert w._dated_output_path("data/output/") == "data/output/2026-04-21"


def test_dated_path_inserts_date_before_filename_when_extension_present():
    w = _writer()
    assert (
        w._dated_output_path("data/output/result.parquet")
        == "data/output/2026-04-21/result.parquet"
    )


def test_dated_path_appends_date_subdir_when_no_extension():
    w = _writer()
    assert (
        w._dated_output_path("data/output/result")
        == "data/output/result/2026-04-21"
    )


def test_dated_path_handles_bare_leaf_without_slash():
    w = _writer()
    assert w._dated_output_path("output") == "output/2026-04-21"


def test_dated_path_handles_backslash_separator():
    w = _writer()
    assert (
        w._dated_output_path("data\\output\\result.parquet")
        == "data\\output\\2026-04-21\\result.parquet"
    )
