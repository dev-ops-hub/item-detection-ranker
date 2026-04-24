"""Abstract base class for composable RDD transformations.

All concrete transforms in `item_ranker.jobs.transforms` inherit from
`RDDTransformation` so they can be plugged into
`item_ranker.jobs.transforms.pipeline.TransformationPipeline` and
swapped freely.
"""
from abc import ABC, abstractmethod
from pyspark import RDD


class RDDTransformation(ABC):
    """Abstract base class for RDD transformations.

    Enables the Pipeline / Strategy design pattern: each transformation
    is a composable, reusable unit. Swap implementations to assemble
    entirely different pipelines for different specifications.
    """

    @abstractmethod
    def execute(self, rdd: RDD):
        """Apply the transformation to an RDD and return a new RDD."""
        raise NotImplementedError
