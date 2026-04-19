================================================================================
  PySpark RDD Pipeline — Design Considerations & Proposed Spark Configurations
================================================================================
Date:    2026-04-19
Author:  Data Engineering Team
Project: Top-X Item Detection per Geographical Location
================================================================================


1. PROBLEM STATEMENT
====================

Compute the top X most frequently detected items per geographical location
from video camera detection data in the Town of Utopia.

Inputs:
  - Dataset A (~1M rows): detection events with geo location, camera, item,
    timestamp, and a detection_oid that may contain upstream duplicates.
    Schema: (geographical_location_oid, video_camera_oid, detection_oid,
             item_name, timestamp_detected)
  - Dataset B (~10K rows): lookup table mapping geographical_location_oid to
    human-readable location names.
    Schema: (geographical_location_oid, geographical_location)

Output:
  - Per-location ranked list of the X most popular items, with rank 1 being
    the most frequently detected.
    Schema: (geographical_location_oid, geographical_location, item_rank,
             item_name)


2. ARCHITECTURE & PROJECT LAYOUT
=================================

The project follows a src-layout architecture with clear separation of
concerns:

  src/item_ranker/
  ├─ main.py                      Entry point: CLI parsing, .env loading,
  │                                SparkSession, dynamic job module loading
  ├─ config.py                    Immutable PipelineConfig dataclass
  ├─ io/
  │   ├─ factory_rdd.py           RDDIOFactory: format-agnostic read/write
  │   ├─ reader_rdd.py            Concrete readers (Parquet, CSV, JSON, Text)
  │   └─ writer_rdd.py            Concrete writers with dated output paths
  ├─ jobs/
  │   ├─ task1_etl_job.py         Main ETL job: orchestrates full pipeline
  │   ├─ schema/
  │   │   └─ mapping.py           StructType schemas for input/output datasets
  │   └─ transforms/
  │       ├─ base.py              Abstract RDDTransformation base class
  │       ├─ deduplicator.py      Deduplicate by detection_oid
  │       ├─ aggregator.py        Count items per (location, item) pair
  │       ├─ ranking.py           Rank + top-X filter per location
  │       ├─ enricher.py          Broadcast map-side join with location names
  │       └─ pipeline.py          TransformationPipeline orchestrator
  └─ util/
      ├─ environment.py           .env loading via python-dotenv
      └─ log_manager.py           Centralized logging configuration

Key architectural decisions:
  a) DataFrame API is used ONLY in io/reader_rdd.py and io/writer_rdd.py
     for reading and writing parquet files. All transformation logic
     operates exclusively on RDDs, as required by the specification.
  b) Each transformation is a self-contained class inheriting from
     RDDTransformation, enabling independent testing and reuse.
  c) The pipeline orchestrator (TransformationPipeline) chains transforms
     sequentially, decoupling the composition from implementation.
  d) Column indices are derived at runtime from StructType field names
     via DATASETA_SCHEMA.fieldNames().index(...), keeping schema
     definitions as the single source of truth.
  e) Job modules are loaded dynamically via --job argument, allowing
     multiple pipeline variants without modifying main.py.


3. DESIGN PATTERNS
==================

3.1 Strategy Pattern (via RDDTransformation base class)
-------------------------------------------------------
Each transformation (deduplication, aggregation, ranking, enrichment) is an
independent strategy implementing the execute(rdd) -> rdd interface. This
allows:
  - Swapping any transform without modifying the rest of the pipeline.
  - Building entirely different pipelines for different specifications by
    composing different transform objects.
  - Example: to produce a "top X items globally" report, replace
    AggregatorTransform with a variant that groups by item_name only.

3.2 Pipeline/Chain Pattern (TransformationPipeline)
---------------------------------------------------
The orchestrator accepts a list of RDDTransformation objects and applies
them sequentially. Benefits:
  - Adding/removing/reordering stages requires only changing the list
    passed to TransformationPipeline, not modifying any transform class.
  - Each stage's input/output contract is explicit (RDD of tuples).
  - The pipeline is testable at both unit (individual transform) and
    integration (full chain) levels.
  - Type-checked at construction: non-RDDTransformation objects raise
    TypeError immediately.

3.3 Immutable Configuration (PipelineConfig dataclass)
------------------------------------------------------
A frozen dataclass encapsulates all runtime parameters (dataset_a_path,
dataset_b_path, output_path, top_x). This ensures configuration is not
accidentally mutated during pipeline execution and provides a clean data
contract between the CLI layer and the pipeline logic.

3.4 Broadcast Variable Pattern (EnricherTransform)
---------------------------------------------------
Dataset B is collected into a Python dict via broadcast_dataset_b(),
broadcast to all executors, and used for map-side lookups. This is a
classic Spark optimization pattern for small-large table joins. The
transform is generic: it accepts any broadcast dict, a key index, and
an insert position, making it reusable for different enrichment tasks.

3.5 Factory Pattern (RDDIOFactory)
----------------------------------
A single factory class dispatches read and write operations to the
correct format-specific reader/writer (Parquet, CSV, JSON, Text). This
decouples the ETL logic from file format concerns and makes adding new
formats a one-line registration.

3.6 Dated Output Paths (RDDWriter)
----------------------------------
All writers automatically insert the current run date into the output
path, organizing output by execution date without manual intervention.


4. PIPELINE STAGES — ALGORITHMS & COMPLEXITY
=============================================

Stage 1: Deduplication (DeduplicatorTransform)
----------------------------------------------
  Algorithm:
    rdd.map(tuple)
       .map(row -> (row[detection_oid_index], row))
       .reduceByKey(lambda a, b: a)
       .map(kv -> kv[1])

  Why reduceByKey over distinct() or groupByKey:
    - reduceByKey performs map-side combine before shuffle, minimizing
      data transferred across the network.
    - distinct() would hash the entire row (5 fields); reduceByKey on
      detection_oid is more targeted and handles cases where duplicate
      detection_oids have slightly different metadata fields.
    - groupByKey would materialize all duplicates in memory before
      discarding them — wasteful for this use case.

  Complexity: O(n) time, 1 shuffle stage.

Stage 2: Aggregation (AggregatorTransform)
------------------------------------------
  Algorithm:
    rdd.map(row -> ((row[geo_oid_index], row[item_name_index]), 1))
       .reduceByKey(add)

  Output: ((geo_oid, item_name), count)

  Why reduceByKey(add):
    - Combinable: partial sums are computed per partition before shuffle,
      reducing network I/O by orders of magnitude compared to groupByKey
      followed by len().
    - The operator (add) is associative and commutative, making it ideal
      for distributed reduction.

  Complexity: O(n) time, 1 shuffle stage.

Stage 3: Ranking (RankingTransform)
-----------------------------------
  Algorithm:
    rdd.map(kv -> (geo_oid, (item_name, count)))
       .groupByKey()
       .flatMap(_rank_items: sort desc by count, break ties alphabetically,
               take top_x, assign rank 1..top_x via enumerate)

  Output: (geo_oid, rank, item_name)

  Why groupByKey here:
    - After aggregation, the data is already reduced to (unique location-
      item pairs, count). For ~1M initial rows across ~10K locations,
      this intermediate RDD is at most ~10K * num_unique_items entries,
      which is far smaller than the original dataset.
    - Sorting must occur over the complete set of items within each group
      to assign correct ranks, making groupByKey appropriate here.
    - Per-group sorting is O(k log k) where k is the number of unique
      items per location — typically small.

  Tie-breaking: items with identical counts are sorted alphabetically by
  item_name, ensuring deterministic, reproducible output across runs.

  Complexity: O(m) time where m is the aggregated item count, 1 shuffle
  stage. Per-group sorting is O(k log k) per location.

Stage 4: Enrichment (EnricherTransform)
----------------------------------------
  Algorithm:
    broadcast_dict = sc.broadcast(locations_dict)
    rdd.map(row -> insert location_name at insert_pos from broadcast lookup)

  Output: (geo_oid, geo_location, rank, item_name)

  Why broadcast join instead of RDD.join():
    - Dataset B (~10K rows) easily fits in executor memory.
    - Map-side lookup adds ZERO shuffle stages.
    - Avoids the overhead of partitioning and shuffling both RDDs for a
      standard hash join.
    - This also satisfies the bonus criterion of joining without using
      .join() explicitly.

  Complexity: O(n) time, 0 shuffle stages. Broadcast cost: one-time
  O(10K) network transfer to each executor.


5. SHUFFLE ANALYSIS
===================

Total shuffle stages: 3

  Stage  | Operator                    | Data Volume Moving
  -------|-----------------------------|------------------------------------
  1      | reduceByKey (dedup)         | Reduced: map-side combine on oid
  2      | reduceByKey (aggregation)   | Reduced: map-side combine on counts
  3      | groupByKey  (ranking)       | Small: post-aggregation entries only

The broadcast join adds 0 additional shuffles. By contrast, using RDD.join()
for enrichment would add a 4th shuffle on potentially large data.

Overall: 3 shuffles is near-optimal for this pipeline. The first two benefit
from map-side combiners (reduceByKey), and the third operates on a much
smaller dataset after aggregation.


6. DATA CORRECTNESS CONSIDERATIONS
===================================

  a) Duplicate detection_oid handling:
     The spec states duplicate detection_oids exist due to upstream ingestion
     errors. Each detection_oid is counted at most once via reduceByKey.
     The first row encountered per detection_oid is kept (arbitrary but
     deterministic within a single Spark execution). Since only
     (geo_oid, item_name) are used after dedup, and duplicates share
     the same detection event, this is correct.

  b) Deterministic ranking:
     Ties in item counts are broken alphabetically by item_name. Without
     this, rank assignment would be non-deterministic across runs, making
     testing unreliable.

  c) Missing location lookups:
     If a geo_oid in Dataset A has no corresponding entry in Dataset B,
     the enrichment stage inserts None as the location name (the
     OUTPUT_SCHEMA marks geographical_location as nullable). This
     preserves all rows rather than silently dropping data.

  d) Edge case — fewer items than top_x:
     If a location has fewer than X unique items, all items are returned
     (ranks 1..k where k < X). The pipeline does not pad or error.


7. RUNTIME CONFIGURABILITY
===========================

Five parameters are configurable at runtime via CLI arguments:

  --job               Name of the job module in jobs/ to execute
  --dataset_a_path    Input path for Dataset A (detections parquet)
  --dataset_b_path    Input path for Dataset B (locations parquet)
  --output_path       Output path for the result parquet
  --top-x             Number of top items to return per location

These are parsed by argparse in main.py and wrapped in a PipelineConfig
frozen dataclass, cleanly separating CLI concerns from pipeline logic.

Environment configuration:
  - Place a .env file at repo root with LOG_LEVEL=INFO (or DEBUG, etc.)
  - load_project_env() reads this at startup via python-dotenv.
  - PYTHONPATH must be set in the shell before launch (not via .env)
    because Python reads PYTHONPATH only at interpreter startup.


8. HOW TO RUN
=============

8.1 Prerequisites
-----------------
  - Python = 3.11
  - PySpark = 4.0.2
  - Virtual environment: .venv at repo root
  - Install: uv sync (or uv pip install -e .)

8.2 Python Module Run (recommended for local dev)
--------------------------------------------------
  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    .\.venv\Scripts\python.exe -m item_ranker.main `
      --job task1_etl_job `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output/output.parquet `
      --top-x 10

8.3 spark-submit Run
--------------------
  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    $env:PYSPARK_PYTHON=(Resolve-Path .\.venv\Scripts\python.exe).Path
    $env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON

    spark-submit --master local[*] src/item_ranker/main.py `
      --job task1_etl `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output/output.parquet `
      --top-x 10

  Why set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON:
    - Ensures Spark driver and workers use the same venv interpreter.
    - Prevents serialization mismatch errors (e.g. TimeType not found)
      that occur when the system Python differs from the venv Python.


9. TESTING STRATEGY
===================

9.1 Existing Tests (3 modules)
------------------------------
  test_environment.py:
    - .env file loads LOG_LEVEL correctly
    - Existing env vars are not overridden (override=False behavior)

  test_log_manager.py:
    - get_logger returns namespaced logger
    - configure sets requested level without duplicate handlers
    - Invalid log levels raise ValueError

  test_main.py:
    - CLI args parsed correctly
    - Environment LOG_LEVEL used as default
    - CLI --log-level overrides environment

9.2 Test Infrastructure
-----------------------
  - conftest.py injects src/ into sys.path for import resolution.
  - tmp_path fixture for isolated file I/O per test.
  - monkeypatch for environment variable manipulation.


10. PROPOSED SPARK CONFIGURATIONS
==================================

10.1 Local Development / Testing
---------------------------------
  spark.master                       = local[*]
  spark.ui.enabled                   = false
  spark.sql.shuffle.partitions       = 2
  spark.serializer                   = org.apache.spark.serializer.KryoSerializer
  spark.driver.memory                = 1g

  Rationale: Minimal overhead for rapid iteration. Shuffle partitions set
  to 2 since test data is tiny. UI disabled to avoid port conflicts.

10.2 Single-Node / Small Cluster (~1M rows)
--------------------------------------------
  spark.master                       = local[*] or yarn
  spark.serializer                   = org.apache.spark.serializer.KryoSerializer
  spark.sql.shuffle.partitions       = 64
  spark.default.parallelism          = 64
  spark.driver.memory                = 2g
  spark.executor.memory              = 4g
  spark.executor.cores               = 2
  spark.rdd.compress                 = true
  spark.shuffle.compress             = true
  spark.shuffle.spill.compress       = true
  spark.sql.adaptive.enabled         = true

  Rationale:
  - KryoSerializer: 2-10x faster than default Java serialization; critical
    for RDD-heavy workloads with many tuple shuffles.
  - 64 shuffle partitions: good starting point for ~1M rows across a small
    cluster. Prevents too-small partitions (wasted overhead) or too-large
    partitions (memory pressure, stragglers).
  - RDD/shuffle compression: reduces network I/O at the cost of minor CPU
    usage — a favorable trade-off for this pipeline where shuffles dominate.
  - Adaptive query execution: helps rebalance partitions dynamically in
    mixed DataFrame/RDD stages (parquet read/write).
  - 4g executor memory accommodates the groupByKey in ranking and the
    broadcast variable (~10K entries, negligible memory).

10.3 Production Cluster (YARN/Kubernetes, larger data)
------------------------------------------------------
  spark.master                       = yarn / k8s://...
  spark.deploy.mode                  = cluster
  spark.num.executors                = 4-8
  spark.executor.cores               = 4
  spark.executor.memory              = 8g
  spark.driver.memory                = 4g
  spark.serializer                   = org.apache.spark.serializer.KryoSerializer
  spark.sql.shuffle.partitions       = 200
  spark.default.parallelism          = 200
  spark.rdd.compress                 = true
  spark.shuffle.compress             = true
  spark.shuffle.spill.compress       = true
  spark.sql.adaptive.enabled         = true
  spark.sql.adaptive.coalescePartitions.enabled = true
  spark.speculation                  = true
  spark.speculation.quantile         = 0.9
  spark.dynamicAllocation.enabled    = true
  spark.dynamicAllocation.minExecutors = 2
  spark.dynamicAllocation.maxExecutors = 16

  Rationale:
  - Dynamic allocation: scales executor count to actual workload, reducing
    resource waste during lighter stages (e.g., the small ranking stage).
  - Speculation: re-launches straggler tasks to meet SLA; quantile=0.9
    triggers speculation only when a task is slower than 90% of peers.
  - 200 shuffle partitions: standard recommendation for cluster-scale Spark
    jobs; adaptive coalescing corrects if partitions are too small.
  - Cluster deploy mode: driver runs on the cluster, not the submitting
    machine, for fault tolerance and resource isolation.

10.4 Configuration Tuning Guidance
-----------------------------------
  - If many tasks finish near-instantly: reduce shuffle partitions.
  - If stages show long-tail stragglers: inspect key skew by geo_oid or
    item_name; consider salting or repartitioning.
  - If executors OOM at ranking stage: increase executor memory or
    repartition before groupByKey.
  - If broadcast join becomes slow: Dataset B is likely much larger than
    expected; switch to a standard shuffle join.


11. COMMON ERRORS AND FIXES
============================

  a) ModuleNotFoundError: No module named 'item_ranker'
     Cause: src/ is not on Python import path for the driver/executor.
     Fix:   $env:PYTHONPATH=(Resolve-Path .\src).Path

  b) [PATH_NOT_FOUND] Path does not exist: file:/.../src/data/input/...
     Cause: Running from wrong working directory with relative paths.
     Fix:   Run from repo root, or use absolute paths.

  c) Can't get attribute 'TimeType' on pyspark.sql.types
     Cause: Spark driver and workers use different Python interpreters.
     Fix:   Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the same
            venv executable before launching spark-submit.

  d) [FIELD_STRUCT_LENGTH_MISMATCH] Length of object (N) does not match
     with length of fields (M)
     Cause: RDD tuple size does not match OUTPUT_SCHEMA field count.
     Fix:   Ensure each pipeline stage output contract is preserved.
            Final tuple must have exactly 4 fields matching OUTPUT_SCHEMA.


12. POTENTIAL FUTURE IMPROVEMENTS
==================================

If data volumes grow significantly beyond 1M rows:

  a) Replace groupByKey in ranking with a combineByKey / aggregateByKey
     approach that maintains a bounded top-K heap per partition, avoiding
     materializing all items per group in memory.

  b) Add data quality checks (null detection_oid, null item_name) with
     configurable handling policies (drop, default, fail).

  c) Add pipeline metrics/logging: row counts before and after each
     transform stage, execution time per stage.

  d) Partition output by geographical_location_oid for efficient
     downstream queries.

  e) Consider pre-partitioning the detections RDD by geo_oid to co-locate
     data for aggregation and ranking, potentially reducing shuffle volume
     if key distribution allows.

  f) Add unit tests for each transform class (DeduplicatorTransform,
     AggregatorTransform, RankingTransform, EnricherTransform) using a
     shared SparkSession fixture with local[2] mode.

  g) Add integration tests for the full task1_etl pipeline with parquet
     I/O and output schema validation.


13. SUMMARY
============

The implemented pipeline achieves:
  - Full RDD-based transformation logic (DataFrame only for parquet I/O)
  - 3 total shuffle stages (near-optimal for dedup + aggregate + rank)
  - 0 additional shuffles from enrichment (broadcast map-side join)
  - Deterministic output via alphabetical tie-breaking
  - Clean separation via Strategy + Pipeline + Factory design patterns
  - Immutable configuration via frozen PipelineConfig dataclass
  - Dynamic job loading for multiple pipeline variants
  - Dated output paths for run-level organization
  - Runtime-configurable paths and top-X parameter via CLI
  - Environment configuration via .env with python-dotenv
  - Practical Spark configurations for dev, staging, and production