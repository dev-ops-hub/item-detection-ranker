from item_ranker.jobs.transforms.base import RDDTransformation


class EnricherTransform(RDDTransformation):
    """Enrich RDD rows using a broadcast lookup dictionary.

    Performs a map-side join by looking up a value from a broadcast
    dict keyed on a specified element index. The looked-up value is
    inserted at ``insert_pos`` in the output tuple.

    This avoids using ``.join`` and eliminates a shuffle stage, which
    is optimal when the lookup dataset is small enough to fit in memory.
    Missing keys produce ``None`` in the inserted position (no default
    value is configurable).

    Args:
        enrich_data: A Spark broadcast variable wrapping a dict.
        pri_key_index: Index of the lookup key in each input tuple.
        insert_pos: Position at which the looked-up value is inserted
            in the output tuple.
    """

    def __init__(
        self,
        enrich_data,
        pri_key_index: int = 0,
        insert_pos: int = 1,
    ):
        self._enrich_data = enrich_data
        self._pri_key_index = pri_key_index
        self._insert_pos = insert_pos

    def execute(self, rdd):
        """Enrich each row with the broadcast lookup value."""
        # Capture instance state into local variables so the closure
        # serialised to executors does not need to pickle ``self``.
        bcast = self._enrich_data
        key_idx = self._pri_key_index
        insert_idx = self._insert_pos

        def enrich(row):
            key = row[key_idx]
            value = bcast.value.get(key)
            row_list = list(row)
            # Insert the looked-up value at the configured position.
            # ``dict.get`` returns None on miss, which is acceptable
            # because the output schema marks the column as nullable.
            row_list.insert(insert_idx, value)
            return tuple(row_list)

        return rdd.map(enrich)
