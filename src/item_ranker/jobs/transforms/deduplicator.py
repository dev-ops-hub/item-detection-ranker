from item_ranker.jobs.transforms.base import RDDTransformation


class DeduplicatorTransform(RDDTransformation):
    """Deduplicate rows by a specified key index.

    Given an RDD of tuples, keys each row by the element at
    ``key_index``, keeps a single row per unique key via ``reduceByKey``,
    then strips the key wrapper to return the original tuple shape.

    Using ``reduceByKey`` (rather than ``distinct`` or ``groupByKey``)
    keeps the shuffle small because partial deduplication happens on the
    map side.

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
            # Re-key by the dedup key so reduceByKey collapses duplicates.
            .map(lambda row: (row[self._key_index], row))
            # Arbitrary winner: any duplicate row is acceptable because
            # the rest of the columns are identical for the same key in
            # this pipeline's contract.
            .reduceByKey(lambda a, b: a)
            .map(lambda kv: kv[1])
        )
