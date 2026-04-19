from dataclasses import dataclass

@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the top-X item detection pipeline."""

    dataset_a_path: str
    dataset_b_path: str
    output_path: str
    top_x: int
