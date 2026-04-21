from operator import add

from item_ranker.jobs.transforms.base import RDDTransformation


class AggregatorTransform(RDDTransformation):
    """Count occurrences per composite key.

    Given an RDD of tuples, extracts a composite key from specified
    indices and counts occurrences using reduceByKey.

    Args:
        key_indices: Tuple of positional indices forming the grouping key.

    Output RDD format: ((geo_oid, item_name), no_of_occurence)
    """

    def __init__(self, key_indices: tuple):
        self._key_indices = key_indices

    def execute(self, rdd):
        """Aggregate counts by composite key."""
        indices = self._key_indices
        return (
            rdd
            .map(lambda row: (tuple(row[i] for i in indices), 1))
            .reduceByKey(add)
        )
