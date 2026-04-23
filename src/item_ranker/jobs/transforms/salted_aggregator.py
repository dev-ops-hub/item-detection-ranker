import random

from operator import add
from item_ranker.jobs.transforms.base import RDDTransformation


class SaltedAggregatorTransform(RDDTransformation):
    """Count occurrences per composite key using salted two-phase aggregation.

    Mitigates data skew by appending a random salt to the grouping key
    in phase 1, spreading the hot key across ``num_salts`` partitions.
    Phase 2 strips the salt and combines the partial counts.

    The output format is identical to `AggregatorTransform`:
    ``((key_field_1, key_field_2, ...), count)``

    Args:
        key_indices: Tuple of positional indices forming the grouping key.
        num_salts: Number of salt buckets across which to distribute
            the hot key. Larger values trade more phase-1 partial keys
            for better parallelism on extreme skew.
    """

    def __init__(self, key_indices: tuple, num_salts: int):
        self._key_indices = key_indices
        self._num_salts = num_salts

    def execute(self, rdd):
        """Aggregate counts by composite key with salted two-phase reduce."""
        indices = self._key_indices
        num_salts = self._num_salts

        # Phase 1: append a random salt to the composite key, then run a
        # local reduce. Hot keys are now split across ``num_salts``
        # buckets, so the shuffle load is spread across more partitions.
        salted = (
            rdd
            .map(lambda row: (
                tuple(row[i] for i in indices) + (
                    random.randint(0, num_salts - 1),
                ),
                1,
            ))
            .reduceByKey(add)
        )

        # Phase 2: strip the salt and reduce again. This pass is cheap
        # because phase 1 already collapsed each (key, salt) bucket into
        # a single partial count.
        return (
            salted
            .map(lambda kv: (kv[0][:-1], kv[1]))
            .reduceByKey(add)
        )
