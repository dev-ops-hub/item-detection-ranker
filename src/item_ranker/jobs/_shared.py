"""Helpers shared between job modules under `item_ranker.jobs`.

Kept private (leading underscore) because nothing outside the jobs
package should import from here.
"""
from pyspark import RDD
from pyspark.sql import SparkSession


def broadcast_dataset_b(spark: SparkSession, dataset_b_rdd: RDD):
    """Collect a small lookup RDD into a dict and broadcast it.

    Args:
        spark: SparkSession instance.
        dataset_b_rdd: RDD of
            ``(geographical_location_oid, geographical_location)``.

    Returns:
        A Spark broadcast variable wrapping a
        ``{geo_oid: geo_location}`` dict. Suitable for map-side joins
        because Dataset B is small (~10K rows).
    """
    locations_dict = dict(
        dataset_b_rdd.map(lambda row: (row[0], row[1])).collect()
    )
    return spark.sparkContext.broadcast(locations_dict)
