"""Baseline ETL job for the item-detection-ranker pipeline (Task 1).

Pipeline stages (all RDD-based):
    1. Load Dataset A (detections) and Dataset B (locations lookup) as RDDs.
    2. Broadcast Dataset B as a dict so enrichment is a map-side join
       (no shuffle).
    3. Run the transformation chain on Dataset A:
         Deduplicator -> Aggregator -> Ranking -> Enricher
    4. Write the ranked, enriched result as parquet to the dated output path.

This job assumes reasonably balanced partitions. For data that exhibits
heavy key skew, use ``task2_etl_job`` which swaps in a salted aggregator.
"""
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


def run(spark, config: PipelineConfig):
    """Execute the Task 1 ETL pipeline.

    Args:
        spark: Active SparkSession supplied by `item_ranker.main`.
        config: Frozen `PipelineConfig` carrying input/output paths
            and the ``top_x`` parameter.
    """
    logger = LogManager.get_logger("task1_etl")

    # Read both input parquet files via the RDD IO factory pattern.
    dataset_a_rdd = RDDIOFactory.read_rdd(
        spark, PARQUET_FORMAT, config.dataset_a_path)
    dataset_b_rdd = RDDIOFactory.read_rdd(
        spark, PARQUET_FORMAT, config.dataset_b_path)

    dataset_a_rows = dataset_a_rdd.count()
    dataset_b_rows = dataset_b_rdd.count()

    logger.info("Loaded dataset A with rows=%s", dataset_a_rows)
    logger.info("Loaded dataset B with rows=%s", dataset_b_rows)
    logger.info("Loaded dataset A with numPartitions=%s",
                dataset_a_rdd.getNumPartitions())
    logger.info("Loaded dataset B with numPartitions=%s",
                dataset_b_rdd.getNumPartitions())

    # ------ setting up and running the pipeline --------------

    # Broadcast Dataset B as a {geo_oid: geo_location} dict so the
    # enrichment stage is a map-side join (no shuffle).
    dataset_b_broadcast = broadcast_helper(spark, dataset_b_rdd)

    # Resolve field positions from the schemas rather than hard-coding
    # magic numbers. Keeps the pipeline robust to schema reordering.
    geo_oid_idx = DATASETA_SCHEMA.fieldNames().index(
        "geographical_location_oid"
    )
    detection_idx = DATASETA_SCHEMA.fieldNames().index("detection_oid")
    item_name_idx = DATASETA_SCHEMA.fieldNames().index("item_name")

    # Insert position for the enriched geographical_location column.
    geo_loc_idx = OUTPUT_SCHEMA.fieldNames().index("geographical_location")

    # Configure the transformation pipeline:
    #   dedup by detection_oid -> count per (geo, item)
    #   -> rank top-X per geo -> enrich with human-readable location name.
    pipeline = TransformationPipeline([
        DeduplicatorTransform(key_index=detection_idx),
        AggregatorTransform(key_indices=(geo_oid_idx, item_name_idx)),
        RankingTransform(top_x=config.top_x),
        EnricherTransform(
            enrich_data=dataset_b_broadcast,
            pri_key_index=geo_oid_idx,
            insert_pos=geo_loc_idx
        ),
    ])
    # Run the transformation pipeline.
    result_rdd = pipeline.run(
        dataset_a_rdd,
        logger=logger,
        enable_metrics=True,
        initial_rows=dataset_a_rows,
    )

    # Write the result to the output path (writer stamps the run date).
    RDDIOFactory.write_rdd(
        spark,
        result_rdd,
        PARQUET_FORMAT,
        config.output_path,
        OUTPUT_SCHEMA,
    )

    result_rdd.unpersist()
