from item_ranker.jobs.transforms.base import RDDTransformation


class RankingTransform(RDDTransformation):
    """Rank items within each group and keep the top X.

    Input RDD format:  ``((group_key, item_key), count)``
    Output RDD format: ``(group_key, rank, item_key)``

    Items are ranked descending by ``count``. Ties are broken
    alphabetically by ``item_key`` to keep the output deterministic
    (important for downstream tests and reproducibility).

    Args:
        top_x: Maximum number of ranked items to keep per group.

    For this pipeline the concrete output is
    ``(geo_oid, rank, item_name)``.
    """

    def __init__(self, top_x: int):
        self._top_x = top_x

    def execute(self, rdd):
        """Rank and filter top X items per group."""
        top_x = self._top_x
        # Re-key the upstream aggregation result so all items for a given
        # group_key land together. ``groupByKey`` is acceptable here
        # because each group is small (bounded by item cardinality).
        # kv => ((geo_oid, item_name), count) from prior aggregation.
        # groupByKey yields (geo_oid, [(item_name_1, count_1), ...]).
        return (
            rdd
            .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
            .groupByKey()
            .flatMap(lambda kv: self._rank_items(kv[0], kv[1], top_x))
        )

    @staticmethod
    def _rank_items(group_key, items, top_x):
        """Sort items by count desc, then name asc, assign ranks 1..top_x."""
        # Sort key: negative count for descending order, then item name
        # ascending for deterministic tie-breaking.
        sorted_items = sorted(items, key=lambda x: (-x[1], x[0]))
        return [
            (group_key, rank, item_name)
            for rank, (item_name, num_of_occurence) in enumerate(
                sorted_items[:top_x],
                start=1,
            )
        ]
