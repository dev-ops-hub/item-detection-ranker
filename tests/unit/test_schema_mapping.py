"""Unit tests pinning down the StructType schemas used by the pipeline.

These tests guard against accidental schema drift: column ordering,
types, and nullability all matter because the ETL jobs resolve column
positions by name from these schemas.
"""
from pyspark.sql.types import IntegerType, LongType, StringType

from item_ranker.jobs.schema.mapping import (
    DATASETA_SCHEMA,
    DATASETB_SCHEMA,
    OUTPUT_SCHEMA,
)


def test_dataset_a_schema_field_names_and_types():
    """Dataset A schema matches the documented contract field-by-field."""
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
    """Dataset B (location lookup) schema matches the documented contract."""
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
    """Output schema columns / types / nullability match the spec.

    ``geographical_location`` is nullable because Dataset A may contain a
    ``geographical_location_oid`` that has no entry in Dataset B.
    """
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
    """Field positions referenced by the ETL jobs must not drift."""
    a_names = DATASETA_SCHEMA.fieldNames()
    o_names = OUTPUT_SCHEMA.fieldNames()
    assert a_names.index("geographical_location_oid") == 0
    assert a_names.index("detection_oid") == 2
    assert a_names.index("item_name") == 3
    assert o_names.index("geographical_location") == 1
