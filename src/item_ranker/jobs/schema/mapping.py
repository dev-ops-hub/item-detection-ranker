from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)


# # Column indices for Dataset A (detections parquet)
# # (geographical_location_oid, video_camera_oid, detection_oid, item_name, timestamp_detected)

# DATASETA_GEOLOC_INDEX = 0
# DATASETA_VIDEOCAM_INDEX = 1
# DATASETA_DETECTION_INDEX = 2
# DATASETA_ITEMNAME_INDEX = 3
# DATASETA_TIMESTAMP_INDEX = 4


# DATASETA_INDEX_MAP = {
#     "GEO_LOC_INDEX": 0,
#     "VIDEO_CAM_INDEX": 1,
#     "DETECTION_INDEX": 2,
#     "ITEM_NAME_INDEX": 3,
#     "TIMESTAMP_INDEX": 4
# }

# # (geographical_location_oid, geo_location)
# DATASETB_INDEX_MAP = {
#     "GEO_LOC_INDEX": 0,
#     "GEO_NAME_INDEX": 1,
# }


DATASETA_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("video_camera_oid", LongType(), False),
    StructField("detection_oid", LongType(), False),
    StructField("item_name", StringType(), False),
    StructField("timestamp_detected", LongType(), False),
])

DATASETB_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("geographical_location", StringType(), False),
])

OUTPUT_SCHEMA = StructType([
    StructField("geographical_location_oid", LongType(), False),
    StructField("geographical_location", StringType(), True),
    StructField("item_rank", IntegerType(), False),
    StructField("item_name", StringType(), False),
])
