from abc import ABC, abstractmethod
from datetime import date
from typing import Optional
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from item_ranker.util.log_manager import LogManager


class RDDWriter(ABC):
    def __init__(self, is_overwrite: bool, run_date: Optional[str] = None):
        """Initialize writer mode and run date.

        Defaults run_date to today when it is not provided.
        """

        self._logger = LogManager.get_logger("RDDWriter")
        self._run_date = run_date or date.today().isoformat()
        if not is_overwrite:
            self._mode = "errorifexists"
        else:
            self._mode = "overwrite"

    def _dated_output_path(self, base_path: str) -> str:
        """Return output path with run_date.

        Insert date before filename when a file leaf is present.
        """
        if base_path.endswith(("/", "\\")):
            return f"{base_path}{self._run_date}"

        last_slash = max(base_path.rfind("/"), base_path.rfind("\\"))
        if last_slash >= 0:
            parent = base_path[: last_slash + 1]
            leaf = base_path[last_slash + 1:]
            sep = base_path[last_slash]
        else:
            parent = ""
            leaf = base_path
            sep = "/"

        has_file_extension = "." in leaf and not leaf.startswith(
            ".") and not leaf.endswith(".")
        if has_file_extension:
            return f"{parent}{self._run_date}{sep}{leaf}"

        return f"{base_path}{sep}{self._run_date}"

    @abstractmethod
    def write(self, sc: SparkSession, rdd: RDD, path: str, schema: StructType):
        pass

# --- Concrete Implementations ---


class TextRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema=None):
        """Write as txt file organised by date folder."""

        rdd.saveAsTextFile(self._dated_output_path(path))


class CSVRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write csv under dated folder."""

        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).csv(self._dated_output_path(path))


class JSONRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write JSON under dated folder."""

        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).json(self._dated_output_path(path))


class ParquetRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write parquet under dated folder."""

        filepath = self._dated_output_path(path)
        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).parquet(filepath)
        self._logger.info("wrote parquet to %s", filepath)
