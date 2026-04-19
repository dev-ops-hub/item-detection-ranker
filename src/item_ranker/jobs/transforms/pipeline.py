from item_ranker.jobs.transforms.base import RDDTransformation

class TransformationPipeline:
    """Orchestrates a sequence of RDD transformations.

    Chains multiple RDDTransformation objects, executing them
    sequentially. This enables reuse: swap transforms to build
    entirely different pipelines for different specifications.
    """

    def __init__(self, transforms: list):
        for t in transforms:
            if not isinstance(t, RDDTransformation):
                raise TypeError(
                    f"Expected RDDTransformation, got {type(t).__name__}"
                )
        self._transforms = list(transforms)

    def run(self, rdd):
        """Execute all transformations in sequence on the given RDD."""
        for transform in self._transforms:
            rdd = transform.execute(rdd)
        return rdd
