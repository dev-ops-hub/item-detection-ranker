from item_ranker.jobs.transforms.base import RDDTransformation


class TransformationPipeline:
    """Orchestrates a sequence of RDD transformations.

    Chains multiple `RDDTransformation` objects and executes them
    sequentially. The output of one transform becomes the input of the
    next, enabling reuse: swap transforms to build entirely different
    pipelines for different specifications.

    Type-checks each member at construction time so misconfigured
    pipelines fail fast rather than at the first Spark action.
    """

    def __init__(self, transforms: list):
        for t in transforms:
            if not isinstance(t, RDDTransformation):
                raise TypeError(
                    f"Expected RDDTransformation, got {type(t).__name__}"
                )
        # Defensive copy so caller mutations don't affect the pipeline.
        self._transforms = list(transforms)

    def run(self, rdd):
        """Execute all transformations in sequence on the given RDD."""
        for transform in self._transforms:
            rdd = transform.execute(rdd)
        return rdd
