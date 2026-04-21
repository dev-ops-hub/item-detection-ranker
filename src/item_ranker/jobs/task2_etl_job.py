import math
from item_ranker.jobs.transforms import aggregator
from item_ranker.util import LogManager
from item_ranker.config import PipelineConfig
from item_ranker.io.factory_rdd import PARQUET_FORMAT, RDDIOFactory
from item_ranker.jobs.schema.mapping import DATASETA_SCHEMA, OUTPUT_SCHEMA
from pyspark import RDD
from pyspark.sql import SparkSession 

from item_ranker.jobs.transforms.pipeline import TransformationPipeline
from item_ranker.jobs.transforms.deduplicator import DeduplicatorTransform
from item_ranker.jobs.transforms.aggregator import AggregatorTransform
from item_ranker.jobs.transforms.ranking import RankingTransform
from item_ranker.jobs.transforms.enricher import EnricherTransform
from item_ranker.jobs.transforms.salted_aggregator import SaltedAggregatorTransform


def check_data_skew (rdd:RDD):
    """Check whether an RDD exhibits data skew across its partitions.

    Computes the number of records in each partition and calculates a skew
    factor as the ratio of the maximum partition size to the average partition
    size. A skew factor greater than 1.5 is considered skewed.

    Args:
        rdd: The input RDD to evaluate for data skew.

    Returns:
        A tuple (is_skewed, skew_factor) where:
            - is_skewed (bool): True if skew_factor > 1.5, False otherwise.
            - skew_factor (float): Ratio of max partition size to average
              partition size, or 0 if the RDD has no partitions.
    """
    counts = rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    if counts:
        max_val = max(counts)
        avg_val = sum(counts)/len(counts)
        skew_factor = max_val / avg_val if avg_val > 0 else 0
        if skew_factor > 1.5:
            return True, skew_factor
        else:
            return False, skew_factor
    return False, 0

def broadcast_dataset_b(spark: SparkSession, dataset_b_rdd:RDD):
    """Collect locations RDD into a dict and broadcast it.

    Args:
        spark: SparkSession instance.
        locations_rdd: RDD of
            (geographical_location_oid, geographical_location).

    Returns:
        A Spark broadcast variable wrapping {geo_oid: geo_location} dict.
    """
    locations_dict = dict(
        dataset_b_rdd.map(tuple).map(lambda row: (row[0], row[1])).collect()
    )
    return spark.sparkContext.broadcast(locations_dict)


def run(spark: SparkSession, config: PipelineConfig):
    logger = LogManager.get_logger("task2_etl")

    dataset_a_rdd = RDDIOFactory.read_rdd(
        spark, PARQUET_FORMAT, config.dataset_a_path)
    dataset_b_rdd = RDDIOFactory.read_rdd(
        spark, PARQUET_FORMAT, config.dataset_b_path)

    logger.info("Loaded dataset A with rows=%s", dataset_a_rdd.count())
    logger.info("Loaded dataset B with rows=%s", dataset_b_rdd.count())
    logger.info("Loaded dataset A with numPartitions=%s",
                dataset_a_rdd.getNumPartitions())
    logger.info("Loaded dataset B with numPartitions=%s",
                dataset_b_rdd.getNumPartitions())
    
    is_data_skew, skew_factor = check_data_skew(dataset_a_rdd)

    logger.info("Skew Factor %d, Skew Data [T/F] %s", skew_factor, is_data_skew )

     # ------ setting up and running the pipeline --------------

    # dict {geo_oid: geo_location}
    dataset_b_broadcast = broadcast_dataset_b(spark, dataset_b_rdd)

    # get the indices of each field for Dataset A
    geo_oid_idx = DATASETA_SCHEMA.fieldNames().index(
        "geographical_location_oid"
    )
    detection_idx = DATASETA_SCHEMA.fieldNames().index("detection_oid")
    item_name_idx = DATASETA_SCHEMA.fieldNames().index("item_name")

    # get the indices of each field for Output
    geo_loc_idx = OUTPUT_SCHEMA.fieldNames().index("geographical_location")

    if is_data_skew:
        aggregation = SaltedAggregatorTransform(
            key_indices=(geo_oid_idx, item_name_idx),
            # match the salt to the skew factor
            num_salts=max(2, math.ceil(skew_factor)),
        )
    else:
        aggregation = AggregatorTransform(
            key_indices=(geo_oid_idx, item_name_idx),
        )

    # configure the transformation pipeline
    pipeline = TransformationPipeline([
        DeduplicatorTransform(key_index=detection_idx),
        aggregation,
        RankingTransform(top_x=config.top_x),
        EnricherTransform(
            enrich_data=dataset_b_broadcast,
            pri_key_index=geo_oid_idx,
            insert_pos=geo_loc_idx
        ),
    ])
    # run the transformation pipeline
    result_rdd = pipeline.run(dataset_a_rdd)

    # write the results to the output path
    RDDIOFactory.write_rdd(
        spark,
        result_rdd,
        PARQUET_FORMAT,
        config.output_path,
        OUTPUT_SCHEMA,
    )

  