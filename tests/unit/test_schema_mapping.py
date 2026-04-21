from pyspark.sql.types import IntegerType, LongType, StringType

from item_ranker.jobs.schema.mapping import (
    DATASETA_SCHEMA,
    DATASETB_SCHEMA,
    OUTPUT_SCHEMA,
)


def test_dataset_a_schema_field_names_and_types():
    names = DATASETA_SCHEMA.fieldNames()
    assert names == [
        "geographical_location_oid",
        "video_camera_oid",
        "detection_oid",
        "item_name",
        "timestamp_detected",
    ]
    types = {f.name: type(f.dataType) for f in DATASETA_SCHEMA.fields}
    assert types["geographical_location_oid"] is LongType
    assert types["video_camera_oid"] is LongType
    assert types["detection_oid"] is LongType
    assert types["item_name"] is StringType
    assert types["timestamp_detected"] is LongType


def test_dataset_b_schema_field_names_and_types():
    assert DATASETB_SCHEMA.fieldNames() == [
        "geographical_location_oid",
        "geographical_location",
    ]
    assert isinstance(
        DATASETB_SCHEMA["geographical_location_oid"].dataType, LongType
    )
    assert isinstance(
        DATASETB_SCHEMA["geographical_location"].dataType, StringType
    )


def test_output_schema_matches_contract():
    assert OUTPUT_SCHEMA.fieldNames() == [
        "geographical_location_oid",
        "geographical_location",
        "item_rank",
        "item_name",
    ]
    fields = {f.name: f for f in OUTPUT_SCHEMA.fields}
    assert isinstance(fields["geographical_location_oid"].dataType, LongType)
    assert isinstance(fields["geographical_location"].dataType, StringType)
    assert fields["geographical_location"].nullable is True
    assert isinstance(fields["item_rank"].dataType, IntegerType)
    assert isinstance(fields["item_name"].dataType, StringType)


def test_indexes_used_by_etl_are_stable():
    a_names = DATASETA_SCHEMA.fieldNames()
    o_names = OUTPUT_SCHEMA.fieldNames()
    assert a_names.index("geographical_location_oid") == 0
    assert a_names.index("detection_oid") == 2
    assert a_names.index("item_name") == 3
    assert o_names.index("geographical_location") == 1
