
# 1. PROBLEM STATEMENT

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


# 2. PROJECT LAYOUT

The project follows a src-layout architecture with clear separation of
concerns:
```
  src/item_ranker/
  ├─ main.py                      Entry point: CLI parsing,
  │                                SparkSession, dynamic job module loading
  ├─ config.py                    Immutable PipelineConfig dataclass
  ├─ io/
  │   ├─ factory_rdd.py           RDDIOFactory: format-agnostic read/write
  │   ├─ reader_rdd.py            Concrete readers (Parquet, CSV, JSON, Text)
  │   └─ writer_rdd.py            Concrete writers with dated output paths
  ├─ jobs/
  │   ├─ task1_etl_job.py         ETL job (baseline pipeline)
  │   ├─ task2_etl_job.py         ETL job with data-skew detection +
  │   │                            salted aggregation when skew detected
  │   ├─ quality/
  │   │   └─ skew_validator.py    Partition-skew detection helper for task2
  │   ├─ schema/
  │   │   └─ mapping.py           StructType schemas for input/output datasets
  │   └─ transforms/
  │       ├─ base.py              Abstract RDDTransformation base class
  │       ├─ deduplicator.py      Deduplicate by detection_oid
  │       ├─ aggregator.py        Count items per (location, item) pair
  │       ├─ salted_aggregator.py Two-phase salted aggregation for skew
  │       ├─ ranking.py           Rank + top-X filter per location
  │       ├─ enricher.py          Broadcast map-side join with location names
  │       └─ pipeline.py          TransformationPipeline orchestrator
  └─ util/
      ├─ environment.py           .env loading via python-dotenv
      └─ log_manager.py           Centralized logging configuration

  tests/
  ├─ conftest.py                  Session-scoped local SparkSession fixture
  ├─ test_environment.py          .env loader unit tests
  ├─ test_log_manager.py          LogManager unit tests
  ├─ unit/
  │   ├─ test_config.py           PipelineConfig unit tests
  │   ├─ test_io_factory.py       RDDIOFactory unit tests
  │   ├─ test_main_cli.py         CLI parsing + job-loader unit tests
  │   ├─ test_schema_mapping.py   Schema contract unit tests
  │   ├─ test_skew_check.py       Skew-detection unit tests
  │   ├─ test_writer_dated_path.py Dated-output-path writer tests
  │   └─ transforms/              One file per transform class
  └─ integration/                 End-to-end pipeline tests on local Spark
      ├─ test_task1_etl_job.py    Synthetic-data full pipeline (task1)
      ├─ test_task2_etl_job.py    Balanced + skewed-data paths (task2)
      ├─ test_main_end_to_end.py  CLI subprocess driving main()
      └─ test_with_real_fixtures.py  Smoke test on data/input/*.parquet
                                      (parametrized for task1 + task2)

```
Design Considerations :  
    a)	Implementation must use Spark RDD for transformation logic. DataFrame API is allowed only for reading and writing Parquet.  
    b)	Use reusable design patterns so logic can adapt to other table specs.  
    c)	Code must pass flake8 style checks and follow clean code practices.  
    d)	Consider time and space complexity, minimize shuffle stages and avoid explicit .join if possible.  
    e)	Dataset A contains duplicate detection_oid values. Each detection_oid must count once only.  
    f)	Job’s input path for Dataset A & B, and Top X value are configurable and able to change.  
    g)	Job’s output path can be changed and the output shall save in parquet.  
    h) 	Unit test and integration tests shall be included and can be run in local development environment.

# 3. RUNTIME CONFIGURABILITY

Five parameters are configurable at runtime via CLI arguments:

  --job               Name of the job module in jobs/ to execute
  --dataset_a_path    Input path for Dataset A (detections parquet)
  --dataset_b_path    Input path for Dataset B (locations parquet)
  --output_path       Output path for the result parquet
  --top-x             Number of top items to return per location

These are parsed by argparse in main.py and wrapped in a PipelineConfig
frozen dataclass, cleanly separating CLI concerns from pipeline logic.

Available jobs (passed via `--job`):
  - `task1_etl_job` — baseline pipeline (dedup → aggregate → rank → enrich)
  - `task2_etl_job` — same pipeline, but inspects partition counts at
    runtime; if `max_partition_size / avg_partition_size > 1.5`, switches
    to `SaltedAggregatorTransform` (two-phase reduce with random salt) to
    mitigate hot-key skew. Final output is identical to task1.

Environment configuration:
  - Place a .env file at repo root with LOG_LEVEL=INFO (or DEBUG, etc.)
  - load_project_env() reads this at startup via python-dotenv.



# 4. HOW TO RUN

4.0 Environment Setup
---------------------
Follow these steps once before running the project for the first time.

  ### Step 1 — Install Java 21

  PySpark requires Java 21 on the PATH.

  **macOS (Homebrew):**

    brew install openjdk@21
    # Add to shell profile (~/.zshrc or ~/.bash_profile):
    export JAVA_HOME=$(/usr/libexec/java_home -v 21)
    export PATH=$JAVA_HOME/bin:$PATH

  **Windows:**
    1. Download the OpenJDK 21 MSI from https://adoptium.net
    2. Run the installer (tick "Set JAVA_HOME variable" and "Add to PATH").
    3. Verify in a new terminal:
         java -version

  ---

  ### Step 2 — Install Python 3.11

  The project pins Python = 3.11 (see `.python-version`).

  **macOS (pyenv recommended):**

    brew install pyenv
    pyenv install 3.11
    pyenv local 3.11          # writes .python-version at repo root

  **Windows:**
    Download the Python 3.11 installer from https://python.org/downloads
    and tick "Add Python to PATH" during installation.

  ---

  ### Step 3 — Install uv (package / venv manager)

  **macOS/Linux:**

    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Restart your terminal or run:
    source $HOME/.cargo/env

  **Windows (PowerShell):**

    powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"

  ---

  ### Step 4 — Create the virtual environment and install dependencies

  `uv sync` will automatically create `.venv/` pinned to Python 3.11 (from
  `.python-version`). If `uv` cannot find Python 3.11 on your machine it
  will download and install it automatically.

  If you prefer to create the venv manually first, run:

  **macOS/Linux:**

    # Create venv explicitly with Python 3.11 (if uv does not auto-resolve)
    uv venv --python 3.11 .venv

    # Then install all dependencies
    uv sync

  **Windows (PowerShell):**

    # Create venv explicitly with Python 3.11 (if uv does not auto-resolve)
    uv venv --python 3.11 .venv

    # Then install all dependencies
    uv sync

  If you do not have `uv` and prefer plain Python tooling:

  **macOS/Linux:**

    python3.11 -m venv .venv
    .venv/bin/pip install -r requirements.txt

  **Windows (PowerShell):**

    py -3.11 -m venv .venv
    .\.venv\Scripts\pip.exe install -r requirements.txt

  All approaches create `.venv/` at the repo root and install all packages
  listed in `requirements.txt` (pyspark, pytest, flake8, python-dotenv, …).

  ---

  ### Step 5 — Create the .env file

  Copy the example below and save it as `.env` at the repository root:

    LOG_LEVEL=INFO

  Available log levels: DEBUG, INFO, WARN, ERROR, CRITICAL.

  ---

  ### Step 6 — Verify the setup

  **macOS/Linux:**

    export PYTHONPATH=$(pwd)/src
    .venv/bin/python -c "import pyspark; print(pyspark.__version__)"

  **Windows (PowerShell):**

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    .\.venv\Scripts\python.exe -c "import pyspark; print(pyspark.__version__)"

  **Windows (Command Prompt):**

    set PYTHONPATH=%CD%\src
    .venv\Scripts\python.exe -c "import pyspark; print(pyspark.__version__)"

  Expected output: `4.0.2`

4.1 Prerequisites
-----------------
  - Python = 3.11
  - Java 21 (required by PySpark)
  - PySpark = 4.0.2
  - Virtual environment: .venv at repo root
  - Install: uv sync (or uv pip install -e .)

4.2 Python Module Run (recommended for local dev)
--------------------------------------------------
Note: Replace "output" with the {jobname} if you want to partition the output
      based on the job run   

  From repository root (Command Prompt):

    set PYTHONPATH=%CD%\src
    set PYSPARK_PYTHON=%CD%\.venv\Scripts\python.exe
    set PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%

    .venv\Scripts\python.exe -m item_ranker.main ^
      --job task1_etl_job ^
      --dataset_a_path data/input/datasetA.parquet ^
      --dataset_b_path data/input/datasetB.parquet ^
      --output_path data/output ^
      --top-x 10

  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    $env:PYSPARK_PYTHON=(Resolve-Path .\.venv\Scripts\python.exe).Path
    $env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
    .\.venv\Scripts\python.exe -m item_ranker.main `
      --job task1_etl_job `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output `
      --top-x 10

  From repository root (macOS/Linux — bash/zsh):

    export PYTHONPATH=$(pwd)/src
    export PYSPARK_PYTHON=$(pwd)/.venv/bin/python
    export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    .venv/bin/python -m item_ranker.main \
      --job task1_etl_job \
      --dataset_a_path data/input/datasetA.parquet \
      --dataset_b_path data/input/datasetB.parquet \
      --output_path data/output \
      --top-x 10

4.3 spark-submit Run
--------------------
  Note: `spark-submit` is installed inside the venv by PySpark and is NOT
  a system command. Use the full path to the venv's spark-submit script.

  From repository root (Command Prompt):

    set PYTHONPATH=%CD%\src
    set PYSPARK_PYTHON=%CD%\.venv\Scripts\python.exe
    set PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%

    .venv\Scripts\spark-submit --master local[*] src/item_ranker/main.py ^
      --job task1_etl_job ^
      --dataset_a_path data/input/datasetA.parquet ^
      --dataset_b_path data/input/datasetB.parquet ^
      --output_path data/output ^
      --top-x 10
  
  From repository root (PowerShell):

    $env:PYTHONPATH=(Resolve-Path .\src).Path
    $env:PYSPARK_PYTHON=(Resolve-Path .\.venv\Scripts\python.exe).Path
    $env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON

    .\.venv\Scripts\spark-submit --master local[*] src/item_ranker/main.py `
      --job task1_etl_job `
      --dataset_a_path data/input/datasetA.parquet `
      --dataset_b_path data/input/datasetB.parquet `
      --output_path data/output `
      --top-x 10

  From repository root (macOS/Linux — bash/zsh):

    export PYTHONPATH=$(pwd)/src
    export PYSPARK_PYTHON=$(pwd)/.venv/bin/python
    export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON

    .venv/bin/spark-submit --master 'local[*]' src/item_ranker/main.py \
      --job task1_etl_job \
      --dataset_a_path data/input/datasetA.parquet \
      --dataset_b_path data/input/datasetB.parquet \
      --output_path data/output \
      --top-x 10

  Rationale to set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON:
    - Ensures Spark driver and workers use the same venv interpreter.
    - Prevents serialization mismatch errors (e.g. TimeType not found)
      that occur when the system Python differs from the venv Python.

4.4 Run the Test Suite
----------------------
  All tests run on a local Spark dev environment (no cluster required).
  The session-scoped SparkSession fixture in `tests/conftest.py` creates
  `master("local[2]")` and is reused across all Spark tests.

  Prerequisites:
    - Same as §4.1 (venv with project deps installed).
    - `pytest` is included as a project dependency.

  IMPORTANT — Windows / Spark interpreter consistency:
    Set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the venv Python
    (same rationale as §4.3). The conftest also unsets `SPARK_HOME` so
    that Spark workers use the venv-installed pyspark instead of any
    older bundled distribution (which can otherwise cause
    `AttributeError: Can't get attribute 'TimeType'`).

  From repository root (macOS/Linux — bash/zsh):
```
    export PYTHONPATH=$(pwd)/src
    export PYSPARK_PYTHON=$(pwd)/.venv/bin/python
    export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
    unset SPARK_HOME

    # Fast suite (unit + synthetic-data integration). Excludes the
    # real-fixtures smoke test marked @pytest.mark.slow.
    .venv/bin/python -m pytest -m "not slow"

    # Full suite — also runs integration tests against the real
    # data/input/dataset{A,B}.parquet fixtures for task1 AND task2,
    # plus a cross-job equivalence check (task1 ≡ task2 output).
    .venv/bin/python -m pytest

    # Run a single test or directory:
    .venv/bin/python -m pytest tests/unit -v
    .venv/bin/python -m pytest tests/integration/test_task2_etl_job.py -v
```

  From repository root (PowerShell):
```
    $env:PYTHONPATH=(Resolve-Path .\src).Path
    $env:PYSPARK_PYTHON=(Resolve-Path .\.venv\Scripts\python.exe).Path
    $env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
    Remove-Item env:SPARK_HOME -ErrorAction SilentlyContinue

    # Fast suite (unit + synthetic-data integration). Excludes the
    # real-fixtures smoke test marked @pytest.mark.slow.
    .\.venv\Scripts\python.exe -m pytest -m "not slow"

    # Full suite — also runs integration tests against the real
    # data/input/dataset{A,B}.parquet fixtures for task1 AND task2,
    # plus a cross-job equivalence check (task1 ≡ task2 output).
    .\.venv\Scripts\python.exe -m pytest

    # Run a single test or directory:
    .\.venv\Scripts\python.exe -m pytest tests/unit -v
    .\.venv\Scripts\python.exe -m pytest tests/integration/test_task2_etl_job.py -v

```

  From repository root (Command Prompt):
```
    set PYTHONPATH=%CD%\src
    set PYSPARK_PYTHON=%CD%\.venv\Scripts\python.exe
    set PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%
    set SPARK_HOME=

    REM Fast suite (unit + synthetic-data integration). Excludes the
    REM real-fixtures smoke test marked @pytest.mark.slow.
    .venv\Scripts\python.exe -m pytest -m "not slow"

    REM Full suite — also runs integration tests against the real
    REM data/input/dataset{A,B}.parquet fixtures for task1 AND task2,
    REM plus a cross-job equivalence check (task1 ≡ task2 output).
    .venv\Scripts\python.exe -m pytest

    REM Run a single test or directory:
    .venv\Scripts\python.exe -m pytest tests/unit -v
    .venv\Scripts\python.exe -m pytest tests/integration/test_task2_etl_job.py -v
```

  Test layout (see `tests/` tree in §2):
    - Unit tests use the shared Spark fixture for transforms; pure-Python
      tests (config, CLI, dated-path writer, schema mapping) need no Spark.
    - Integration tests:
        * `test_task1_etl_job.py` — in-process pipeline with synthetic
          parquet (covers dedup, ranking, enrichment with missing key,
          top-X truncation, output schema).
        * `test_task2_etl_job.py` — covers BOTH the balanced path and
          the skewed path that triggers `SaltedAggregatorTransform`.
        * `test_main_end_to_end.py` — invokes `main()` via subprocess so
          its `spark.stop()` cannot tear down the shared session.
        * `test_with_real_fixtures.py` — `@pytest.mark.slow`, parametrized
          over `task1_etl_job` and `task2_etl_job`, plus an equivalence
          test asserting task1 and task2 produce identical output sets
          (since salting must not change final results).
# 5. Program Flow

```
CLI (main.py)
 │
 ├─ parse args
 ├─ create SparkSession (KryoSerializer)
 ├─ dynamically load job module
 │
 └─ Dispatch by --job
   │
   ├─ task1_etl_job.run(spark, config)
   │   │
   │   ├─ Read Dataset A (detections)   ── RDDIOFactory ── ParquetRDDReader
   │   ├─ Read Dataset B (locations)    ── RDDIOFactory ── ParquetRDDReader
   │   ├─ Broadcast Dataset B as dict
   │   ├─ Stage 1: DeduplicatorTransform   (reduceByKey)
   │   ├─ Stage 2: AggregatorTransform     (reduceByKey + add)
   │   ├─ Stage 3: RankingTransform        (groupByKey + sort + top-X)
   │   ├─ Stage 4: EnricherTransform       (broadcast map-side join)
   │   └─ Write results                 ── RDDIOFactory ── ParquetRDDWriter
   │
   └─ task2_etl_job.run(spark, config)
     │
     ├─ Read Dataset A (detections)   ── RDDIOFactory ── ParquetRDDReader
     ├─ Read Dataset B (locations)    ── RDDIOFactory ── ParquetRDDReader
     ├─ Broadcast Dataset B as dict
     ├─ Stage 1: DeduplicatorTransform   (reduceByKey)
     ├─ SkewValidator: check partition-size ratio
     ├─ if skew ratio > skew_threshold
     │   ├─ Stage 2a: SaltedAggregatorTransform (phase 1 reduceByKey)
     │   └─ Stage 2b: SaltedAggregatorTransform (phase 2 reduceByKey)
     ├─ else
     │   └─ Stage 2: AggregatorTransform  (reduceByKey + add)
     ├─ Stage 3: RankingTransform         (groupByKey + sort + top-X)
     ├─ Stage 4: EnricherTransform        (broadcast map-side join)
     └─ Write results                 ── RDDIOFactory ── ParquetRDDWriter
```

**Key patterns:**  
  a) Strategy Pattern (swappable transforms),  
  b) Chain of Responsibilities Pattern (sequential stages),   
  c) Factory Pattern,   
  d) Broadcast Join Pattern (shuffle-free enrichment)  

# 6. Pipeline Stages

### `task1_etl_job` (baseline) and `task2_etl_job` (non-skew path)

| Stage | Transform | Operation | Shuffles | Purpose |
|-------|-----------|-----------|----------|---------|
| 1 | `DeduplicatorTransform` | `reduceByKey` on `detection_oid` | 1 | Remove duplicate detection events |
| 2 | `AggregatorTransform` | `reduceByKey(add)` on `(geo_oid, item_name)` | 1 | Count items per location |
| 3 | `RankingTransform` | `groupByKey` + sort + slice | 1 | Rank items and keep top-X per location |
| 4 | `EnricherTransform` | Broadcast dict lookup | 0 | Add human-readable location names |

**Total: 3 shuffle stages** (near-optimal).

### `task2_etl_job` (skew-detected path)

Before Stage 2, task2 checks partition-size skew. If
`max_partition_size / avg_partition_size > 1.5`, task2 swaps in salted
aggregation:

| Stage | Transform | Operation | Shuffles | Purpose |
|-------|-----------|-----------|----------|---------|
| 1 | `DeduplicatorTransform` | `reduceByKey` on `detection_oid` | 1 | Remove duplicate detection events |
| 2a | `SaltedAggregatorTransform` (phase 1) | `reduceByKey(add)` on salted `(geo_oid, item_name, salt)` | 1 | Split hot keys across buckets |
| 2b | `SaltedAggregatorTransform` (phase 2) | `reduceByKey(add)` after unsalt to `(geo_oid, item_name)` | 1 | Merge partial counts |
| 3 | `RankingTransform` | `groupByKey` + sort + slice | 1 | Rank items and keep top-X per location |
| 4 | `EnricherTransform` | Broadcast dict lookup | 0 | Add human-readable location names |

**Total: 4 shuffle stages** in the skew-aware path. Tie-breaking in ranking uses alphabetical order for deterministic results.

# 7. Data Schemas
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

# 8. SHUFFLE ANALYSIS

Baseline pipeline (`task1_etl_job`) and the non-skew path in
`task2_etl_job`: 3 shuffle stages

  Stage  | Operator                    | Data Volume Moving
  -------|-----------------------------|------------------------------------
  1      | reduceByKey (dedup)         | Reduced: map-side combine on oid
  2      | reduceByKey (aggregation)   | Reduced: map-side combine on counts
  3      | groupByKey (ranking)        | Small: post-aggregation entries only

Skew-aware pipeline (`task2_etl_job`) when skew is detected: 4 shuffle stages

  Stage  | Operator                            | Data Volume Moving
  -------|-------------------------------------|------------------------------------
  1      | reduceByKey (dedup)                 | Reduced: map-side combine on oid
  2      | reduceByKey (salted aggregation 1)  | Hot keys split across salt buckets
  3      | reduceByKey (salted aggregation 2)  | Partial counts merged after unsalt
  4      | groupByKey (ranking)                | Small: post-aggregation entries only

The broadcast join adds 0 additional shuffles in all paths. By contrast,
using `RDD.join()` for enrichment would add one more shuffle on potentially
large data.

Overall: the baseline pipeline is near-optimal at 3 shuffles for dedup,
aggregate, and rank. The salted path intentionally pays one extra shuffle to
spread hot keys more evenly and reduce skew-related stragglers.

# 9. POTENTIAL FUTURE IMPROVEMENTS

If data volumes grow significantly beyond 1M rows:  
  a) Replace groupByKey in ranking with a combineByKey / aggregateByKey
     approach that maintains a bounded top-K heap per partition, avoiding
     materializing all items per group in memory.  
  b) Add data quality checks (null detection_oid, null item_name) with
     configurable handling policies (drop, default, fail).  
  c) Partition output by geographical_location_oid for efficient
     downstream queries.  
  d) Consider pre-partitioning the dataSet_a RDD by geo_oid to co-locate
     data for aggregation and ranking, potentially reducing shuffle volume
     if key distribution allows.  

# 10. SUMMARY

The implemented pipeline achieves:
  - Full RDD-based transformation logic (DataFrame only for parquet I/O)
  - 3 total shuffle stages on the baseline path (near-optimal for
    dedup + aggregate + rank); +1 extra shuffle on the salted path,
    accepted only when runtime skew is detected
  - 0 additional shuffles from enrichment (broadcast map-side join)
  - Adaptive skew handling via `task2_etl_job`: inspects partition sizes
    at runtime and swaps in `SaltedAggregatorTransform` (two-phase
    salted reduce) 
  - Clean separation via Strategy + Pipeline + Factory design patterns
  - Immutable configuration via frozen PipelineConfig dataclass
  - Dynamic job loading for multiple pipeline variants
    (`task1_etl_job`, `task2_etl_job`)
  - Dated output paths for run-level organization
  - Runtime-configurable paths and top-X parameter via CLI
  - Environment configuration via .env with python-dotenv
 