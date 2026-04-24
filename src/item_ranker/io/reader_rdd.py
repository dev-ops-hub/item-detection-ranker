"""Concrete RDD readers used by `RDDIOFactory`.

Each reader wraps one of Spark's DataFrameReader APIs and exposes the
resulting rows as an RDD (since the project must perform transformations
in RDD form per the design constraints).
"""
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession
from pyspark import RDD


class RDDReader(ABC):
    """Abstract base for all concrete RDD readers.

    Implementations must return an `RDD` loaded from the given path.
    """

    @abstractmethod
    def read(self, sc: SparkSession, path: str) -> RDD:
        """Read from ``path`` and return the contents as an RDD."""
        raise NotImplementedError

# --- Concrete Implementations ---


class TextRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a text file and return its contents as an RDD of Row(value)."""
        return sc.read.text(path).rdd


class CSVRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a CSV file (with header) and return it as an RDD of Row."""
        return sc.read.csv(path, header=True).rdd


class JSONRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a JSON file and return its contents as an RDD of Row."""
        return sc.read.json(path).rdd


class ParquetRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a parquet file and return its contents as an RDD of Row."""
        return sc.read.parquet(path).rdd
