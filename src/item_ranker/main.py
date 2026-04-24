"""CLI entry point for the item-detection-ranker pipeline.

Responsibilities:
    * Parse command-line arguments into a `PipelineConfig`.
    * Dynamically resolve and import the requested job module from
      `item_ranker.jobs` (e.g. ``task1_etl_job``, ``task2_etl_job``).
    * Create a single `SparkSession` for the whole run.
    * Delegate execution to the job module's ``run(spark, config)`` function.
    * Ensure the `SparkSession` is always stopped, even on failure.

The module is designed so that individual pieces (argument parsing,
job loading, main) can be unit-tested independently.
"""
import argparse
import importlib
import pkgutil
import time
import traceback
from pyspark.sql import SparkSession

from item_ranker.util import LogManager, get_log_level, load_project_env
from item_ranker.config import PipelineConfig


def parse_args(args=None):
    """Parse command-line arguments for pipeline configuration."""
    parser = argparse.ArgumentParser(
        description="Compute top X detected items per geographical location."
    )
    parser.add_argument(
        "--job", required=True,
        help="Name of the job file in jobs to run the pipeline."
    )
    # Note: hyphenated long-options are the argparse convention. The
    # underscore aliases are kept for backwards compatibility with
    # existing scripts/CI invocations.
    parser.add_argument(
        "--dataset-a-path", "--dataset_a_path",
        dest="dataset_a_path", required=True,
        help="Input path for detections parquet file (Dataset A)."
    )
    parser.add_argument(
        "--dataset-b-path", "--dataset_b_path",
        dest="dataset_b_path", required=True,
        help="Input path for locations parquet file (Dataset B)."
    )
    parser.add_argument(
        "--output-path", "--output_path",
        dest="output_path", required=True,
        help="Output path for the result parquet file."
    )
    parser.add_argument(
        "--top-x", type=int, required=True,
        help="Number of top items to return per location."
    )

    return parser.parse_args(args)


def load_job_module(job_arg: str):
    """Resolve and import a job module from a CLI job argument.

    Accepts any of the following forms:
        * bare module name, e.g. ``task1_etl_job``
        * filename with ``.py`` suffix, e.g. ``task1_etl_job.py``
        * fully-qualified dotted path,
          e.g. ``item_ranker.jobs.task1_etl_job``

    If the module cannot be found, the error message lists every
    discoverable job under ``item_ranker.jobs`` so the user can correct
    the ``--job`` argument without reading source.

    Args:
        job_arg: The raw value supplied to the ``--job`` CLI flag.

    Returns:
        The imported job module object. Callers must verify it exposes
        a ``run(spark, config)`` function.

    Raises:
        ModuleNotFoundError: If the requested job module does not exist.
    """
    # Strip an optional .py suffix so users can pass either form.
    raw_job = job_arg[:-3] if job_arg.endswith(".py") else job_arg
    # Allow fully-qualified dotted paths to pass through untouched;
    # otherwise prepend the `item_ranker.jobs` package namespace.
    if raw_job.startswith("item_ranker.jobs."):
        module_name = raw_job
    else:
        module_name = f"item_ranker.jobs.{raw_job}"

    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        # Only translate the error when the missing module is the job
        # itself. A failing transitive import (``exc.name != module_name``)
        # must propagate unchanged so the real root cause is not hidden.
        if exc.name == module_name:
            jobs_pkg = importlib.import_module("item_ranker.jobs")
            # Discover all non-private, non-subpackage modules in jobs/.
            available = sorted(
                name
                for _, name, is_pkg in pkgutil.iter_modules(jobs_pkg.__path__)
                if not is_pkg and not name.startswith("_")
            )
            available_jobs = ", ".join(available) if available else "<none>"
            raise ModuleNotFoundError(
                f"Unknown job '{job_arg}'. Available jobs: {available_jobs}"
            ) from exc
        raise


def main(args=None):
    """Program entry point.

    Loads environment variables, parses CLI arguments, creates a single
    SparkSession, loads the requested job module, and invokes its
    ``run(spark, config)`` function. Any exception is caught and logged
    as FATAL (rather than re-raised) so the ``finally`` block can always
    stop Spark cleanly.

    Args:
        args: Optional iterable of CLI arguments. Defaults to
            ``sys.argv[1:]`` when ``None`` (standard argparse behaviour).
    """
    is_env_loaded = load_project_env()
    parsed = parse_args(args)
    LogManager.configure(get_log_level())
    logger = LogManager.get_logger("item-detection-ranker")
    logger.info("Environment variables .env file loaded : %s", is_env_loaded)
    logger.info("Starting item detection ranker job=%s", parsed.job)

    start_time = time.time()
    spark = None

    config = PipelineConfig(
        dataset_a_path=parsed.dataset_a_path,
        dataset_b_path=parsed.dataset_b_path,
        output_path=parsed.output_path,
        top_x=parsed.top_x,
    )

    try:
        # KryoSerializer is noticeably faster than the Java default for
        # the RDD shuffles this pipeline performs (reduceByKey etc.).
        spark = (
            SparkSession.builder
            .appName("item-detection-ranker")
            .config(
                "spark.serializer",
                "org.apache.spark.serializer.KryoSerializer",
            )
            .getOrCreate()
        )
        logger.info("Spark session created: %s", spark.sparkContext.appName)

        job_module = load_job_module(parsed.job)

        if not hasattr(job_module, "run"):
            raise AttributeError(
                (
                    f"Job '{parsed.job}' must expose a run(spark, config) "
                    "function"
                )
            )
        job_module.run(spark, config)

        elapsed_time = time.time() - start_time
        logger.info(
            "Pipeline completed in %.2f seconds", elapsed_time)

    except Exception:
        error_stack = traceback.format_exc()
        logger.critical("FATAL: Job '%s' failed execution.\n%s",
                        parsed.job, error_stack)
        # Re-raise so the process exits non-zero. The ``finally`` block
        # below still runs first, guaranteeing Spark is stopped cleanly.
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
