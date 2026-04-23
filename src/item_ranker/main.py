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
    LogManager.configure(get_log_level())

    parser = argparse.ArgumentParser(
        description="Compute top X detected items per geographical location."
    )
    parser.add_argument(
        "--job", required=True,
        help="Name of the job file in jobs to run the pipeline."
    )
    parser.add_argument(
        "--dataset_a_path", required=True,
        help="Input path for detections parquet file (Dataset A)."
    )
    parser.add_argument(
        "--dataset_b_path", required=True,
        help="Input path for locations parquet file (Dataset B)."
    )
    parser.add_argument(
        "--output_path", required=True,
        help="Output path for the result parquet file."
    )
    parser.add_argument(
        "--top-x", type=int, required=True,
        help="Number of top items to return per location."
    )

    return parser.parse_args(args)


def load_job_module(job_arg: str):
    """Resolve and import a job module from a CLI job argument."""
    raw_job = job_arg[:-3] if job_arg.endswith(".py") else job_arg
    if raw_job.startswith("item_ranker.jobs."):
        module_name = raw_job
    else:
        module_name = f"item_ranker.jobs.{raw_job}"

    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            jobs_pkg = importlib.import_module("item_ranker.jobs")
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
    """Entry point: parse args, create Spark session, run pipeline."""
    is_env_loaded = load_project_env()
    parsed = parse_args(args)
    logger = LogManager.get_logger("item-detection-ranker")
    logger.info(f"Environment variables .env file loaded : {is_env_loaded}")
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
        spark = (
            SparkSession.builder
            .appName("item-detection-ranker")
            .config(
                "spark.serializer",
                "org.apache.spark.serializer.KryoSerializer",
            )
            # .config("spark.pyspark.python", sys.executable)
            # .config("spark.pyspark.driver.python", sys.executable)
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
        logger.fatal("FATAL: Job '%s' failed execution.\n%s",
                     parsed.job, error_stack)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
