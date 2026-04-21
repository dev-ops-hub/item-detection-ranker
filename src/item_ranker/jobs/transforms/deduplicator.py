from item_ranker.jobs.transforms.base import RDDTransformation


class DeduplicatorTransform(RDDTransformation):
    """Deduplicate rows by a specified key index.

    Given an RDD of tuples, keys by the element at `key_index`,
    keeps only one row per unique key via reduceByKey, then
    strips the key wrapper to return the original tuple structure.

    Args:
        key_index: Positional index of the deduplication key in each tuple.
    """

    def __init__(self, key_index: int):
        self._key_index = key_index

    def execute(self, rdd):
        """Deduplicate RDD rows by the configured key index."""
        return (
            rdd
            .map(tuple)
            .map(lambda row: (row[self._key_index], row))
            .reduceByKey(lambda a, b: a)
            .map(lambda kv: kv[1])
        )
