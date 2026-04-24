"""Canonical StructType schemas for the item-detection-ranker pipeline.

These schemas define the contract for:
    * Dataset A (detections parquet input)
    * Dataset B (locations parquet input)
    * The ranked parquet output

Job code resolves column positions by calling ``<SCHEMA>.fieldNames().index(
"<column_name>")`` rather than hard-coding integer indices, so schema
reordering stays safe.
"""
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Dataset A -- detections (~1M rows). ``detection_oid`` may repeat across
# rows and must be deduplicated before counting items.
# Fields: (geographical_location_oid, video_camera_oid, detection_oid,
#          item_name, timestamp_detected)
DATASETA_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("video_camera_oid", LongType(), False),
    StructField("detection_oid", LongType(), False),
    StructField("item_name", StringType(), False),
    StructField("timestamp_detected", LongType(), False),
])

# Dataset B -- location lookup (~10K rows). Small enough to broadcast for
# a map-side join in the enrichment stage.
DATASETB_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("geographical_location", StringType(), False),
])

# Output schema -- ranked items per geographical location.
# ``geographical_location`` is nullable because Dataset A may contain a
# geo_oid that does not exist in the Dataset B lookup.
OUTPUT_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("geographical_location", StringType(), True),
    StructField("item_rank", IntegerType(), False),
    StructField("item_name", StringType(), False),
])
