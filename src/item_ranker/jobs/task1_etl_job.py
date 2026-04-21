from item_ranker.util import LogManager
from item_ranker.config import PipelineConfig
from item_ranker.io.factory_rdd import PARQUET_FORMAT, RDDIOFactory
from item_ranker.jobs.schema.mapping import DATASETA_SCHEMA, OUTPUT_SCHEMA

from item_ranker.jobs.transforms.pipeline import TransformationPipeline
from item_ranker.jobs.transforms.deduplicator import DeduplicatorTransform
from item_ranker.jobs.transforms.aggregator import AggregatorTransform
from item_ranker.jobs.transforms.ranking import RankingTransform
from item_ranker.jobs.transforms.enricher import EnricherTransform


def broadcast_dataset_b(spark, dataset_b_rdd):
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


def run(spark, config: PipelineConfig):
    logger = LogManager.get_logger("task1_etl")

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

    # configure the transformation pipeline
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

    # sample = result_rdd.take(5)
    # for row in sample:
    #     print(row)
