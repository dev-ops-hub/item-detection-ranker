from operator import add

from item_ranker.jobs.transforms.base import RDDTransformation


class AggregatorTransform(RDDTransformation):
    """Count occurrences per composite key.

    Given an RDD of tuples, extracts a composite key from the supplied
    positional indices and counts occurrences with ``reduceByKey``
    (which performs map-side combining for an efficient shuffle).

    Args:
        key_indices: Tuple of positional indices forming the grouping key.

    Output RDD format: ``((key_field_1, key_field_2, ...), count)``
    For this pipeline that is ``((geo_oid, item_name), count)``.
    """

    def __init__(self, key_indices: tuple):
        self._key_indices = key_indices

    def execute(self, rdd):
        """Aggregate counts by composite key."""
        # Bind to a local for the closure shipped to executors.
        indices = self._key_indices
        return (
            rdd
            .map(lambda row: (tuple(row[i] for i in indices), 1))
            .reduceByKey(add)
        )
