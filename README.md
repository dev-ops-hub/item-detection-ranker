
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


2. PROJECT LAYOUT
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

Design Considerations :
  a)	Implementation must use Spark RDD for transformation logic. DataFrame API is allowed only for reading and writing Parquet.
  b)	Use reusable design patterns so logic can adapt to other table specs.
  c)	Code must pass flake8 style checks and follow clean code practices.
  d)	Consider time and space complexity, minimize shuffle stages and avoid explicit .join if possible.
  e)	Dataset A contains duplicate detection_oid values. Each detection_oid must count once only.
  f)	Job’s input path for Dataset A & B, and Top X value are configurable and able to change
  g)	Job’s output path can be changed and the output shall save in parquet.
  h) 	Unit test and integration tests shall be included and can be run in local development environment.

3. RUNTIME CONFIGURABILITY
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


4. HOW TO RUN
=============

4.1 Prerequisites
-----------------
  - Python = 3.11
  - Java 21 (required by PySpark)
  - PySpark = 4.0.2
  - Virtual environment: .venv at repo root
  - Install: uv sync (or uv pip install -e .)

4.2 Python Module Run (recommended for local dev)
--------------------------------------------------
  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    .\.venv\Scripts\python.exe -m item_ranker.main `
      --job task1_etl_job `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output/output.parquet `
      --top-x 10

4.3 spark-submit Run
--------------------
  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    $env:PYSPARK_PYTHON=(Resolve-Path .\.venv\Scripts\python.exe).Path
    $env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON

    spark-submit --master local[*] src/item_ranker/main.py `
      --job task1_etl_job `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output/output.parquet `
      --top-x 10

  Rationale to set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON:
    - Ensures Spark driver and workers use the same venv interpreter.
    - Prevents serialization mismatch errors (e.g. TimeType not found)
      that occur when the system Python differs from the venv Python.

5. Program Flow
=================

```
CLI (main.py)
 │
 ├─ parse args & load .env
 ├─ create SparkSession (KryoSerializer)
 ├─ dynamically load job module
 │
 └─ task1_etl_job.run(spark, config)
     │
     ├─ Read Dataset A (detections)   ── RDDIOFactory ── ParquetRDDReader
     ├─ Read Dataset B (locations)    ── RDDIOFactory ── ParquetRDDReader
     ├─ Broadcast Dataset B as dict
     │
     ├─ TransformationPipeline
     │   ├─ Stage 1: DeduplicatorTransform   (reduceByKey)
     │   ├─ Stage 2: AggregatorTransform     (reduceByKey + add)
     │   ├─ Stage 3: RankingTransform        (groupByKey + sort + top-X)
     │   └─ Stage 4: EnricherTransform       (broadcast map-side join)
     │
     └─ Write results                 ── RDDIOFactory ── ParquetRDDWriter
```

**Key patterns:** 
a) Strategy Pattern (swappable transforms), 
b) Chain of Responsibilities Pattern (sequential stages), 
c) Factory Pattern, 
d) Broadcast Join Pattern (shuffle-free enrichment)

6. Pipeline Stages
=================

| Stage | Transform | Operation | Shuffles | Purpose |
|-------|-----------|-----------|----------|---------|
| 1 | `DeduplicatorTransform` | `reduceByKey` on `detection_oid` | 1 | Remove duplicate detection events |
| 2 | `AggregatorTransform` | `reduceByKey(add)` on `(geo_oid, item_name)` | 1 | Count items per location |
| 3 | `RankingTransform` | `groupByKey` + sort + slice | 1 | Rank items and keep top-X per location |
| 4 | `EnricherTransform` | Broadcast dict lookup | 0 | Add human-readable location names |

**Total: 3 shuffle stages** (near-optimal). Tie-breaking in ranking uses alphabetical order for deterministic results.

7. Data Schemas
=================
### Input: Dataset A (Detections) — Parquet, ~1M rows

| Field | Type | Description |
|-------|------|-------------|
| `geographical_location_oid` | Long | Location identifier |
| `video_camera_oid` | Long | Camera identifier |
| `detection_oid` | Long | Unique detection ID |
| `item_name` | String | Detected item name |
| `timestamp_detected` | Long | Detection timestamp |

### Input: Dataset B (Locations) — Parquet, ~10K rows

| Field | Type | Description |
|-------|------|-------------|
| `geographical_location_oid` | Long | Location identifier (FK) |
| `geographical_location` | String | Human-readable location name |

### Output — Parquet, dated folder

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `geographical_location_oid` | Long | No | Location identifier |
| `geographical_location` | String | Yes | Location name |
| `item_rank` | Integer | No | Rank (1 = most frequent) |
| `item_name` | String | No | Detected item name |

Output is written to a date-stamped subfolder, e.g. `data/output/{run_time_date}}/`.

8. SHUFFLE ANALYSIS
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

9. POTENTIAL FUTURE IMPROVEMENTS
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
  - Clean separation via Strategy + Pipeline + Factory design patterns
  - Immutable configuration via frozen PipelineConfig dataclass
  - Dynamic job loading for multiple pipeline variants
  - Dated output paths for run-level organization
  - Runtime-configurable paths and top-X parameter via CLI
  - Environment configuration via .env with python-dotenv
 