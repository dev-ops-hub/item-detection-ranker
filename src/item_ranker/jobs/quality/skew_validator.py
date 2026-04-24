from pyspark import RDD


# Above this max/avg partition-size ratio we consider the RDD skewed
# enough that the salted two-phase aggregator is worth its overhead.
SKEW_RATIO_THRESHOLD = 1.5


class DataSkewValidator:
    """Checks if data is unevenly distributed across partitions."""

    @staticmethod
    def check_data_skew(rdd: RDD):
        """Check whether an RDD exhibits data skew across its partitions.

        Computes the number of records in each partition and calculates a
        skew factor as ``max_partition_size / avg_partition_size``. A skew
        factor greater than ``SKEW_RATIO_THRESHOLD`` is treated as skewed.

        Args:
            rdd: The input RDD to evaluate for data skew.

        Returns:
            A tuple ``(is_skewed, skew_factor)`` where:
                * ``is_skewed`` (bool) - True if
                ``skew_factor > SKEW_RATIO_THRESHOLD``.
                * ``skew_factor`` (float) - Ratio of max partition size to
                average partition size, or ``0.0`` if the RDD has no
                partitions.
        """
        # Count rows locally inside each partition without shuffling.
        counts = rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        if not counts:
            return False, 0.0

        avg_val = sum(counts) / len(counts)
        skew_factor = max(counts) / avg_val if avg_val > 0 else 0.0
        return skew_factor > SKEW_RATIO_THRESHOLD, skew_factor
