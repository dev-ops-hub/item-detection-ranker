"""Concrete RDD writers used by `RDDIOFactory`.

Every writer inserts a ``YYYY-MM-DD`` run-date into the output path so
successive runs land in separate directories (useful for partitioned
warehouses and easy local inspection). The run date can be injected via
the ``run_date`` constructor argument for deterministic testing.
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from item_ranker.util.log_manager import LogManager


class RDDWriter(ABC):
    """Abstract base for all concrete RDD writers.

    Provides shared behaviour: save-mode resolution, run-date stamping,
    and a dated-path helper used by every subclass.
    """

    def __init__(self, is_overwrite: bool, run_date: Optional[str] = None):
        """Initialize writer mode and run date.

        Args:
            is_overwrite: When True, writes use Spark's ``overwrite`` save
                mode; otherwise writes fail if the target path exists
                (``errorifexists``).
            run_date: ISO date string (``YYYY-MM-DD``) used as the dated
                output segment. Defaults to today's date when omitted.
        """

        self._logger = LogManager.get_logger("RDDWriter")
        self._run_date = run_date or date.today().isoformat()
        if not is_overwrite:
            self._mode = "errorifexists"
        else:
            self._mode = "overwrite"

    def _dated_output_path(self, base_path: str) -> str:
        """Return ``base_path`` with the run date injected.

        Behaviour by path shape:
            * ends with ``/`` or ``\\``  -> append ``run_date`` (e.g.
              ``data/out/``  ->  ``data/out/2026-04-21``).
            * contains a file-like leaf with an extension -> insert the
              run date as a subdirectory *before* the filename
              (e.g. ``data/out/result.parquet``  ->
              ``data/out/2026-04-21/result.parquet``).
            * otherwise  -> append ``run_date`` as a subdirectory
              (e.g. ``data/out/result``  ->  ``data/out/result/2026-04-21``).

        The helper preserves the separator (``/`` or ``\\``) that appears
        in ``base_path`` so it works on both POSIX and Windows-style paths.
        """
        if base_path.endswith(("/", "\\")):
            return f"{base_path}{self._run_date}"

        # Find the right-most path separator, whichever style is used.
        last_slash = max(base_path.rfind("/"), base_path.rfind("\\"))
        if last_slash >= 0:
            parent = base_path[: last_slash + 1]
            leaf = base_path[last_slash + 1:]
            sep = base_path[last_slash]
        else:
            # No separator: treat the whole value as a leaf under `./`.
            parent = ""
            leaf = base_path
            sep = "/"

        # A leaf that looks like ``name.ext`` (but not a dotfile such as
        # ``.env`` or a trailing-dot oddity) is treated as a filename.
        has_file_extension = "." in leaf and not leaf.startswith(
            ".") and not leaf.endswith(".")
        if has_file_extension:
            return f"{parent}{self._run_date}{sep}{leaf}"

        return f"{base_path}{sep}{self._run_date}"

    @abstractmethod
    def write(self, sc: SparkSession, rdd: RDD, path: str, schema: StructType):
        """Persist ``rdd`` under the dated form of ``path``."""
        pass

# --- Concrete Implementations ---


class TextRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema=None):
        """Write as a plain text file under a run-date subdirectory."""

        rdd.saveAsTextFile(self._dated_output_path(path))


class CSVRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write CSV under a run-date folder."""

        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).csv(self._dated_output_path(path))


class JSONRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write JSON under a run-date folder."""

        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).json(self._dated_output_path(path))


class ParquetRDDWriter(RDDWriter):
    def write(self, sc, rdd, path, schema):
        """Convert RDD to DataFrame and write parquet under a dated path."""

        filepath = self._dated_output_path(path)
        df = sc.createDataFrame(rdd, schema=schema)
        df.write.mode(self._mode).parquet(filepath)
        self._logger.info("wrote parquet to %s", filepath)
