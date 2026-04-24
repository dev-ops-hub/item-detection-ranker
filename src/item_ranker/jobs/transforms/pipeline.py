from time import perf_counter

from pyspark.storagelevel import StorageLevel

from item_ranker.jobs.transforms.base import RDDTransformation
from item_ranker.util import LogManager


class TransformationPipeline:
    """Orchestrates a sequence of RDD transformations.

    Chains multiple `RDDTransformation` objects and executes them
    sequentially. The output of one transform becomes the input of the
    next, enabling reuse: swap transforms to build entirely different
    pipelines for different specifications. When metrics are enabled,
    the pipeline logs per-stage input/output row counts, elapsed time,
    and partition counts while caching intermediate outputs to avoid
    immediate recomputation for the count action.

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

    def run(self, rdd, *, logger=None, enable_metrics=False,
            initial_rows=None):
        """Execute all transformations in sequence on the given RDD.

        Args:
            rdd: Input RDD for the first transformation stage.
            logger: Optional logger used for per-stage metrics output.
                When omitted, a pipeline-scoped logger is created.
            enable_metrics: When ``True``, log per-stage input/output
                row counts, elapsed time, and partition counts.
            initial_rows: Optional precomputed row count for ``rdd``.
                Supplying this avoids an extra ``count()`` before the
                first stage when metrics are enabled.

        Returns:
            The final transformed RDD. In metrics mode, intermediate
            stage outputs are persisted long enough to collect counts,
            and only the final RDD remains persisted on return.
        """
        if not enable_metrics:
            for transform in self._transforms:
                rdd = transform.execute(rdd)
            return rdd

        logger = logger or LogManager.get_logger("pipeline")
        current_rdd = rdd
        current_rows = initial_rows
        total_stages = len(self._transforms)

        for index, transform in enumerate(self._transforms, start=1):
            stage_name = type(transform).__name__
            if current_rows is None:
                current_rows = current_rdd.count()

            start = perf_counter()
            next_rdd = transform.execute(current_rdd).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            next_rows = next_rdd.count()
            elapsed = perf_counter() - start

            logger.info(
                "Pipeline stage %s/%s [%s] input_rows=%s output_rows=%s "
                "elapsed_s=%.3f partitions=%s",
                index,
                total_stages,
                stage_name,
                current_rows,
                next_rows,
                elapsed,
                next_rdd.getNumPartitions(),
            )

            if current_rdd is not rdd:
                current_rdd.unpersist()

            current_rdd = next_rdd
            current_rows = next_rows

        return current_rdd
