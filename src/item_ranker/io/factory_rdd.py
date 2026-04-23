"""Format-agnostic factory for reading and writing RDDs.

The factory follows a simple registry pattern: a ``file_type`` string
(``"parquet"``, ``"csv"``, ``"json"``, ``"txt"``) is looked up against
an internal map of readers and writers. New formats can be supported by
registering additional concrete classes without changing call sites.
"""
from .reader_rdd import (
    CSVRDDReader,
    JSONRDDReader,
    ParquetRDDReader,
    TextRDDReader,
)
from .writer_rdd import (
    CSVRDDWriter,
    JSONRDDWriter,
    ParquetRDDWriter,
    TextRDDWriter,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import RDD


# Supported format identifiers (lower-case, used as registry keys).
PARQUET_FORMAT = "parquet"
CSV_FORMAT = "csv"
JSON_FORMAT = "json"
TEXT_FORMAT = "txt"


class RDDIOFactory:
    """Registry-based factory that dispatches to the right reader/writer.

    All writers are constructed with ``is_overwrite=True`` so repeated
    local runs do not fail because of a pre-existing target directory.
    The factory methods used are staticmethods so callers do
    not need to instantiate the factory.
    """

    # Reader registry keyed by lower-case format name.
    _readers = {
        "txt": TextRDDReader(),
        "csv": CSVRDDReader(),
        "json": JSONRDDReader(),
        "parquet": ParquetRDDReader()
    }

    # Writer registry keyed by lower-case format name.
    _writers = {
        "txt": TextRDDWriter(is_overwrite=True),
        "csv": CSVRDDWriter(is_overwrite=True),
        "json": JSONRDDWriter(is_overwrite=True),
        "parquet": ParquetRDDWriter(is_overwrite=True),
    }

    @staticmethod
    def read_rdd(sc: SparkSession, file_type: str, path: str):
        """Read ``path`` using the reader registered for ``file_type``.

        Args:
            sc: Active SparkSession.
            file_type: Format identifier (case-insensitive), e.g. ``"parquet"``.
            path: Input path to read from.

        Returns:
            An RDD of rows produced by the matching reader.

        Raises:
            ValueError: If ``file_type`` is not a supported format.
        """
        reader = RDDIOFactory._readers.get(file_type.lower())
        if not reader:
            raise ValueError(f"Unsupported format: {file_type}")

        return reader.read(sc, path)

    @staticmethod
    def write_rdd(
        sc: SparkSession,
        rdd: RDD,
        file_type: str,
        path: str,
        schema: StructType,
    ):
        """Write ``rdd`` to ``path`` using the registered writer for ``file_type``.

        Args:
            sc: Active SparkSession (used to build a DataFrame with ``schema``).
            rdd: RDD of tuples matching ``schema``.
            file_type: Format identifier (case-insensitive).
            path: Base output path; writers insert a run-date subdirectory.
            schema: StructType describing the columns of ``rdd``.

        Raises:
            ValueError: If ``file_type`` is not a supported format.
        """
        writer = RDDIOFactory._writers.get(file_type.lower())
        if not writer:
            raise ValueError(f"Unsupported format: {file_type}")

        return writer.write(sc, rdd, path, schema)
