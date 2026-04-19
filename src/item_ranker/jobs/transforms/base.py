from abc import ABC, abstractmethod
from pyspark import RDD

class RDDTransformation(ABC):
    """Abstract base class for RDD transformations.

    Enables the Pipeline/Strategy design pattern: each transformation
    is a composable, reusable unit. Swap out transforms to create
    entirely different pipelines for different specifications.
    """

    @abstractmethod
    def execute(self, rdd:RDD):
        """Apply the transformation to an RDD and return a new RDD."""
        raise NotImplementedError
