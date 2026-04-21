import dataclasses

import pytest

from item_ranker.config import PipelineConfig


def test_pipeline_config_fields_round_trip():
    cfg = PipelineConfig(
        dataset_a_path="a.parquet",
        dataset_b_path="b.parquet",
        output_path="out.parquet",
        top_x=10,
    )
    assert cfg.dataset_a_path == "a.parquet"
    assert cfg.dataset_b_path == "b.parquet"
    assert cfg.output_path == "out.parquet"
    assert cfg.top_x == 10


def test_pipeline_config_is_frozen():
    cfg = PipelineConfig("a", "b", "o", 1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.top_x = 99
