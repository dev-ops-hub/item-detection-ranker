"""Skew-aware ETL job for the item-detection-ranker pipeline (Task 2).

Functionally equivalent to ``task1_etl_job`` but inspects the input
partition distribution at runtime:

    * If ``max_partition_size / avg_partition_size > 1.5``, it swaps the
      plain `AggregatorTransform` for `SaltedAggregatorTransform`, a
      two-phase reduce that appends a random salt to hot keys to spread
      them across partitions.
    * Otherwise the job runs identically to Task 1.

Salting only changes the shuffle distribution; the final ranked output
is identical to Task 1's for any given input (verified by integration
tests in ``tests/integration/test_with_real_fixtures.py``).
"""
import math

from pyspark.sql import SparkSession

from item_ranker.util import LogManager
from item_ranker.config import PipelineConfig
from item_ranker.io.factory_rdd import PARQUET_FORMAT, RDDIOFactory
from item_ranker.util.broadcast_helper import broadcast_helper
from item_ranker.jobs.schema.mapping import DATASETA_SCHEMA, OUTPUT_SCHEMA
from item_ranker.jobs.transforms.pipeline import TransformationPipeline
from item_ranker.jobs.transforms.deduplicator import DeduplicatorTransform
from item_ranker.jobs.transforms.aggregator import AggregatorTransform
from item_ranker.jobs.transforms.ranking import RankingTransform
from item_ranker.jobs.transforms.enricher import EnricherTransform
from item_ranker.jobs.transforms.salted_aggregator import (
    SaltedAggregatorTransform,
)
from item_ranker.jobs.quality.skew_validator import DataSkewValidator


def run(spark: SparkSession, config: PipelineConfig):
    """Execute the Task 2 ETL pipeline (skew-aware variant).

    Selects `SaltedAggregatorTransform` when ``check_data_skew`` reports
    skew; otherwise behaves exactly like Task 1.

    Args:
        spark: Active SparkSession.
        config: Pipeline configuration with input/output paths and top_x.
    """
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

    # Inspect partition distribution to decide between plain and salted
    # aggregation. This runs one lightweight mapPartitions + collect.
    is_data_skew, skew_factor = DataSkewValidator.check_data_skew(
        dataset_a_rdd
    )

    logger.info("Skew Factor %.2f, Skew Data [T/F] %s",
                skew_factor, is_data_skew)

    # ------ setting up and running the pipeline --------------

    # Broadcast Dataset B (~10K rows) as a {geo_oid: geo_location} dict
    # so enrichment is a map-side join (no shuffle).
    dataset_b_broadcast = broadcast_helper(spark, dataset_b_rdd)

    # Resolve field indices by name for robustness against schema edits.
    geo_oid_idx = DATASETA_SCHEMA.fieldNames().index(
        "geographical_location_oid"
    )
    detection_idx = DATASETA_SCHEMA.fieldNames().index("detection_oid")
    item_name_idx = DATASETA_SCHEMA.fieldNames().index("item_name")

    geo_loc_idx = OUTPUT_SCHEMA.fieldNames().index("geographical_location")

    if is_data_skew:
        # Pick num_salts proportional to the observed skew_factor so we
        # only pay the two-phase cost when it meaningfully helps. Always
        # use at least 2 salts to actually split the hot key.
        aggregation = SaltedAggregatorTransform(
            key_indices=(geo_oid_idx, item_name_idx),
            num_salts=max(2, math.ceil(skew_factor)),
        )
    else:
        aggregation = AggregatorTransform(
            key_indices=(geo_oid_idx, item_name_idx),
        )

    # Same overall pipeline as Task 1; only the aggregation stage differs.
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
    # Run the transformation pipeline.
    result_rdd = pipeline.run(dataset_a_rdd)

    # Write the result to the output path (writer stamps the run date).
    RDDIOFactory.write_rdd(
        spark,
        result_rdd,
        PARQUET_FORMAT,
        config.output_path,
        OUTPUT_SCHEMA,
    )
