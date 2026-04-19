from .reader_rdd import CSVRDDReader, JSONRDDReader, ParquetRDDReader, TextRDDReader
from .writer_rdd import CSVRDDWriter, JSONRDDWriter, ParquetRDDWriter, TextRDDWriter
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import RDD


PARQUET_FORMAT = "parquet"
CSV_FORMAT = "csv"
JSON_FORMAT = "json"
TEXT_FORMAT = "txt"


class RDDIOFactory:
    _readers = {
        "txt": TextRDDReader(),
        "csv": CSVRDDReader(),
        "json": JSONRDDReader(),
        "parquet": ParquetRDDReader()
    }

    _writers = {
        "txt": TextRDDWriter(is_overwrite=True),
        "csv": CSVRDDWriter(is_overwrite=True),
        "json": JSONRDDWriter(is_overwrite=True),
        "parquet": ParquetRDDWriter(is_overwrite=True),
    }

    @staticmethod
    def read_rdd(sc : SparkSession, file_type: str, path: str):
        reader = RDDIOFactory._readers.get(file_type.lower())
        if not reader:
            raise ValueError(f"Unsupported format: {file_type}")
        
        return reader.read(sc, path)
    

    @staticmethod
    def write_rdd(sc: SparkSession, rdd: RDD, file_type: str, path: str, schema: StructType):
        writer = RDDIOFactory._writers.get(file_type.lower())
        if not writer:
            raise ValueError(f"Unsupported format: {file_type}")

        return writer.write(sc, rdd, path, schema)