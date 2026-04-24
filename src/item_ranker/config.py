"""Immutable runtime configuration for the item-detection-ranker pipeline.

`PipelineConfig` is populated from CLI arguments in `item_ranker.main` and
then handed to every job's `run(spark, config)` entry point. It isolates
user-facing CLI parsing from internal pipeline logic and guarantees the
configuration cannot be mutated once a job starts executing.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the top-X item detection pipeline.

    Attributes:
        dataset_a_path: Path to the detections parquet file (Dataset A).
        dataset_b_path: Path to the locations parquet file (Dataset B).
        output_path: Destination path for the ranked parquet output.
            A run-date subdirectory is inserted by the writer.
        top_x: Number of top-ranked items to keep per geographical
            location (rank 1 is the most frequently detected item).
    """

    dataset_a_path: str
    dataset_b_path: str
    output_path: str
    top_x: int
