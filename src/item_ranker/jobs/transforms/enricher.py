from item_ranker.jobs.transforms.base import RDDTransformation


class EnricherTransform(RDDTransformation):
    """Enrich RDD rows using a broadcast lookup dictionary.

    Performs a map-side join by looking up a value from a broadcasted
    dict keyed on a specified element index. The looked-up value is
    inserted at the specified output position.

    This avoids using .join and eliminates a shuffle stage, which is
    optimal when the lookup dataset is small enough to fit in memory.

    Args:
        broadcast_var: A Spark broadcast variable wrapping a dict.
        key_index: Index of the lookup key in each input tuple.
        insert_index: Position to insert the looked-up value
            in the output tuple.
        default: Default value if key is not found in the broadcast dict.
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
        bcast = self._enrich_data
        key_idx = self._pri_key_index
        insert_idx = self._insert_pos

        def enrich(row):
            key = row[key_idx]
            value = bcast.value.get(key)
            row_list = list(row)
            # row_list[0] = value
            row_list.insert(insert_idx, value)
            return tuple(row_list)

        return rdd.map(enrich)
