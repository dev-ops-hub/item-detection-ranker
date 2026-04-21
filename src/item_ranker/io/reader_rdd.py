from abc import ABC, abstractmethod

from pyspark.sql import SparkSession
from pyspark import RDD


class RDDReader(ABC):
    @abstractmethod
    def read(self, sc: SparkSession, path: str) -> RDD:
        """Must return an RDD"""
        raise NotImplementedError

# --- Concrete Implementations ---


class TextRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a parquet file and return its contents as an RDD of text."""
        return sc.read.text(path).rdd


class CSVRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a csv file and return its contents as an RDD of tuples."""
        return sc.read.csv(path, header=True).rdd


class JSONRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a json file and return its contents as an RDD of tuples."""
        return sc.read.json(path).rdd


class ParquetRDDReader(RDDReader):
    def read(self, sc, path):
        """Read a parquet file and return its contents as an RDD of tuples."""
        return sc.read.parquet(path).rdd
