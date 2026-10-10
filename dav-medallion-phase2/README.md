# S&P 500 Medallion Lakehouse Pipeline

Daily S&P 500 stock prices (OHLCV) from Yahoo Finance (`yfinance`), landed in S3 (Phase 1) and
processed into **Bronze** and **Silver** Delta tables with PySpark (Phase 2).

```text
Yahoo Finance --(Phase 1)--> raw Parquet in S3 --(Phase 2)--> BRONZE --> SILVER
                                                                  \          \
                                                                   \--> quarantine (rejected rows)
                       every step writes an audit row to  ops.pipeline_execution_logs
                       every schema drift is recorded in   ops.schema_drift_log
```

## Repository structure

```text
dav-medallion-pipeline/
├── ingestion/                      # Phase 1: Yahoo Finance -> raw Parquet (local + S3)
│   ├── full_load.py
│   └── incremental_load.py
├── pipeline/                       # Phase 2: PySpark pipeline
│   ├── schemas.py                  #   explicit StructType contracts for every table
│   ├── config.py                   #   paths, batch ids, date helpers (no hardcoded dates)
│   ├── transforms.py               #   casting, validation, drift detection (pure DataFrame code)
│   ├── bronze.py                   #   Raw -> Bronze
│   ├── silver.py                   #   Bronze -> Silver (+ quarantine)
│   ├── delta_utils.py              #   Spark session, table creation, MERGE INTO helper
│   ├── audit.py                    #   pipeline_execution_logs writer
│   └── run_pipeline.py             #   orchestration + CLI
├── notebooks/                      # Databricks notebooks (widgets = parameters)
│   ├── 01_raw_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   └── 03_demo_idempotency_and_audit.py
├── tests/                          # pytest: unit tests + Delta integration tests
├── sample_data/                    # 10-ticker Parquet samples from Phase 1
├── data/sp500_tickers.csv
├── docs/project-proposal.md
├── requirements.txt
└── README.md
```

---

# Phase 2 - Bronze & Silver with Apache Spark

## Design in one minute

| Requirement | How it is met |
| --- | --- |
| No schema inference | Every read uses an explicit `StructType` (`pipeline/schemas.py`); there is no `inferSchema` anywhere. |
| Strict types & casting | Bronze lands values as `STRING`; Silver casts with `try_cast` to `DATE` / `DOUBLE` / `BIGINT`. |
| `load_timestamp` everywhere | Present in Bronze, Silver, quarantine, execution logs and drift log. |
| Idempotency | All writes are `MERGE INTO` upserts. Rows are only updated when their `row_hash` changed, so re-running identical data inserts 0 rows, updates 0 rows and leaves `load_timestamp` untouched. |
| Parameterised backfills | `load_type`, `run_date`, `start_date`/`end_date`, `batch_id`, `source_file`, `raw_path`, `lakehouse_root` - no path or date is hardcoded. |
| Schema drift | New column -> kept via Delta schema evolution. Missing column -> NULL. Type change -> lands as string; values that cannot be cast are quarantined. The batch never crashes. |
| Fault tolerance | Each file/batch runs in its own audited step: a failure is logged as `FAILURE` and the rest of the backfill continues. The CLI exits non-zero if anything failed. |
| Audit logging | One row per file/batch in `pipeline_execution_logs` (layer, parameter, start/end, status, rows inserted/updated). |

### Tables

All tables are path-based Delta tables under `<lakehouse_root>`:

| Table | Path | Purpose |
| --- | --- | --- |
| Bronze | `bronze/sp500_prices` | Raw values as strings + lineage |
| Silver | `silver/sp500_prices` | Typed, validated, one row per ticker/day |
| Quarantine | `quarantine/sp500_prices_rejected` | Bronze rows that failed validation |
| Execution log | `ops/pipeline_execution_logs` | Audit trail |
| Drift log | `ops/schema_drift_log` | Detected schema drift |

---

## Data dictionary

### Bronze - `bronze/sp500_prices`

Grain: one row per ticker per trading day **per batch**. Primary key: `record_key`.
(Business key: `batch_id` + `ticker` + `date`.) Keys are enforced by the pipeline's MERGE, which
is why it is safe to re-run a batch.

| Column | Type | Description |
| --- | --- | --- |
| `record_key` | STRING | **PK.** `sha2(batch_id \| ticker \| date)`; deterministic, so a re-run hits the same row |
| `ticker` | STRING | Yahoo Finance symbol, as received |
| `date` | STRING | Trading date, as received (not yet cast) |
| `open` | STRING | Opening price, as received |
| `high` | STRING | Daily high, as received |
| `low` | STRING | Daily low, as received |
| `close` | STRING | Closing price, as received |
| `volume` | STRING | Shares traded, as received |
| `source_file` | STRING | Full path of the raw file the row came from |
| `batch_id` | STRING | `<load_type>_<yyyymmdd>`, e.g. `incremental_20240105` |
| `load_type` | STRING | `full` or `incremental` |
| `batch_date` | DATE | Logical date of the batch (`--run-date`, or the partition date for incrementals) |
| `row_hash` | STRING | `sha2` of the seven source columns; used to detect changed rows |
| `load_timestamp` | TIMESTAMP | When the row was ingested into Bronze |
| *(extra columns)* | STRING | Any new column that appears in the source is added automatically (schema evolution) |

Why strings? A raw layer should never reject data. Keeping Bronze faithful means a type change in
the source (e.g. `volume` becoming text) cannot break ingestion; the problem is dealt with, visibly,
in Silver.

### Silver - `silver/sp500_prices`

Grain: one row per ticker per trading day. **Primary key: (`ticker`, `trade_date`).**

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | STRING | **PK.** Upper-cased, trimmed symbol |
| `trade_date` | DATE | **PK.** Trading date |
| `open` | DOUBLE | Opening price (> 0) |
| `high` | DOUBLE | Daily high (>= `low`) |
| `low` | DOUBLE | Daily low |
| `close` | DOUBLE | Closing price (> 0) |
| `volume` | BIGINT | Shares traded (>= 0, integral) |
| `row_hash` | STRING | `sha2` of the typed values; MERGE only updates when it changes |
| `source_batch_id` | STRING | Bronze batch that produced the current row |
| `source_batch_date` | DATE | `batch_date` of that batch (older batches can never overwrite newer data) |
| `load_timestamp` | TIMESTAMP | When the row was last inserted/updated in Silver |

Validation (any failure sends the row to quarantine with the reasons):

| Reason | Rule |
| --- | --- |
| `MISSING_TICKER` | ticker is null/blank |
| `INVALID_DATE` | `date` is null or cannot be cast to a date |
| `INVALID_OPEN/HIGH/LOW/CLOSE` | null or not numeric |
| `NAN_<col>` / `NON_POSITIVE_<col>` | price is NaN or <= 0 |
| `INVALID_VOLUME` / `NEGATIVE_VOLUME` / `NON_INTEGER_VOLUME` | volume null, non-numeric, < 0, or fractional |
| `HIGH_LESS_THAN_LOW` | `high < low` |

If the same (ticker, date) arrives in several batches, the one with the newest `batch_date`
(then newest `load_timestamp`) wins.

### Quarantine - `quarantine/sp500_prices_rejected`

Primary key: `record_key`. Same columns as Bronze (contract columns kept as raw strings so the row
can be fixed and replayed) plus:

| Column | Type | Description |
| --- | --- | --- |
| `reject_stage` | STRING | `BRONZE_TO_SILVER` |
| `reject_reason` | STRING | `;`-separated list of the reasons above |
| `load_timestamp` | TIMESTAMP | When the row was quarantined |

### Operational tables

**`ops/pipeline_execution_logs`** - one row per processed raw file (Raw-to-Bronze) or Bronze batch (Bronze-to-Silver), for both full and incremental loads. Primary key: (`run_id`, `pipeline_layer`, `batch_id`, `start_time`).

| Column | Type | Description |
| --- | --- | --- |
| `run_id` | STRING | Correlation id of one pipeline invocation |
| `pipeline_layer` | STRING | `RAW_TO_BRONZE` or `BRONZE_TO_SILVER` |
| `load_type` | STRING | `full` / `incremental` |
| `batch_id` | STRING | Batch processed |
| `parameter` | STRING | The file path (Raw-to-Bronze) or Bronze batch (Bronze-to-Silver) processed |
| `start_time` / `end_time` | TIMESTAMP | Execution window |
| `duration_seconds` | DOUBLE | `end_time - start_time` |
| `status` | STRING | `SUCCESS`, `FAILURE` or `SKIPPED` (missing file with `--skip-missing`) |
| `rows_read` | BIGINT | Rows read from the source of that step |
| `rows_inserted` | BIGINT | Rows inserted by the MERGE |
| `rows_updated` | BIGINT | Rows updated by the MERGE |
| `rows_quarantined` | BIGINT | Rows rejected (Bronze-to-Silver) |
| `error_message` | STRING | Exception text on failure, reason on skip |
| `load_timestamp` | TIMESTAMP | When the log row was written |

**`ops/schema_drift_log`** - one row per drift per file: `run_id`, `batch_id`, `source_file`,
`drift_type` (`NEW_COLUMN` / `MISSING_COLUMN` / `TYPE_CHANGE`), `column_name`, `expected_type`,
`actual_type`, `action_taken`, `load_timestamp`.

---

## Execution guide

### Setup

Databricks (Community Edition or Azure Databricks): clone this repo into **Repos / Git folders**,
make the raw Parquet files available to the cluster (S3 with `s3a://...`, or upload to DBFS /
a volume), then run the notebooks in `notebooks/`. Delta Lake is built in.

Local: `pip install -r requirements.txt` (adds `pyspark` and `delta-spark`; needs Java 17+).

### Parameters

| Parameter (CLI / notebook widget) | Used by | Meaning |
| --- | --- | --- |
| `--layer` | both | `raw_to_bronze`, `bronze_to_silver` or `all` |
| `--load-type` | raw_to_bronze (required), bronze_to_silver (optional filter) | `full` or `incremental` |
| `--raw-path` | raw_to_bronze | Root of the raw files (`.../full/full_load.parquet`, `.../incremental/YYYY-MM-DD/data.parquet`) |
| `--lakehouse-root` | both | Where the Delta tables live (or env `LAKEHOUSE_ROOT`) |
| `--run-date` | both | One batch date `YYYY-MM-DD`; **default = today** |
| `--start-date` / `--end-date` | both | Inclusive **backfill window** (give both) |
| `--batch-id` | bronze_to_silver | Promote exactly one Bronze batch, e.g. `incremental_20240105` |
| `--source-file` | raw_to_bronze | Read this exact file instead of the default layout |
| `--skip-missing` | raw_to_bronze | Log `SKIPPED` rather than `FAILURE` for dates with no file (weekends/holidays) |
| `--run-id` | both | Optional correlation id for the logs |

### Standard incremental load (daily run)

```bash
# Raw -> Bronze -> Silver for one day (omit --run-date to use today)
python -m pipeline.run_pipeline --layer all --load-type incremental \
    --raw-path s3a://my-bucket/raw/sp500 \
    --lakehouse-root dbfs:/FileStore/lakehouse \
    --run-date 2024-01-05
```

### Initial full load

```bash
python -m pipeline.run_pipeline --layer all --load-type full \
    --raw-path s3a://my-bucket/raw/sp500 --lakehouse-root dbfs:/FileStore/lakehouse \
    --run-date 2024-01-01
```

### Backfills

```bash
# Raw -> Bronze for a whole month of incremental partitions (weekends logged as SKIPPED)
python -m pipeline.run_pipeline --layer raw_to_bronze --load-type incremental \
    --raw-path s3a://my-bucket/raw/sp500 --lakehouse-root dbfs:/FileStore/lakehouse \
    --start-date 2024-01-01 --end-date 2024-01-31 --skip-missing

# Bronze -> Silver for any historical window that is already in Bronze
python -m pipeline.run_pipeline --layer bronze_to_silver \
    --lakehouse-root dbfs:/FileStore/lakehouse \
    --start-date 2024-01-01 --end-date 2024-01-31

# Re-promote a single batch (e.g. after fixing a validation rule)
python -m pipeline.run_pipeline --layer bronze_to_silver \
    --lakehouse-root dbfs:/FileStore/lakehouse --batch-id full_20240101

# Re-process one specific file
python -m pipeline.run_pipeline --layer raw_to_bronze --load-type incremental --run-date 2024-01-05 \
    --source-file s3a://my-bucket/other/location/data.parquet --lakehouse-root dbfs:/FileStore/lakehouse
```

In a notebook, the same call is `run_pipeline(spark, layer=..., load_type=..., start_date=..., ...)`
(see `notebooks/`); the widgets map one-to-one to the parameters above. Running a backfill twice is
safe - that is the point of the MERGE design.

### Checking a run

```sql
SELECT pipeline_layer, batch_id, parameter, start_time, end_time, status, rows_inserted, rows_updated
FROM delta.`<lakehouse_root>/ops/pipeline_execution_logs`
ORDER BY start_time DESC;
```

### Tests

```bash
pip install pytest
pytest tests -q
```

* `tests/test_transforms.py`, `tests/test_config_and_audit.py` - casting, validation, quarantine,
  drift detection, hashing, parameter handling, audit records (plain Spark).
* `tests/test_delta_integration.py` - runs the real pipeline against Delta: idempotent re-runs,
  in-place updates, stale-batch protection, backfill with missing days, drift end-to-end,
  `load_timestamp` and log completeness. Skipped automatically when Delta cannot be loaded.

---

# Phase 1 - Raw ingestion

This project focuses on one dataset: daily S&P 500 stock prices from Yahoo Finance using the
`yfinance` Python package.

The ingestion scripts create Parquet files with this schema:

| Column | Description |
| --- | --- |
| `ticker` | Yahoo Finance ticker symbol |
| `date` | Trading date |
| `open` | Opening price |
| `high` | Daily high |
| `low` | Daily low |
| `close` | Closing price |
| `volume` | Shares traded |

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file with AWS settings:

```text
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_REGION=...
S3_BUCKET=your-bucket
FOLDER_PREFIX=raw/sp500
```

`BUCKET_NAME` is also supported as a fallback if `S3_BUCKET` is not set.

## Full Load

Writes a local 10-ticker sample:

```bash
python ingestion/full_load.py
```

The script downloads daily OHLCV from `2019-01-01` through `2024-01-01` and writes:

```text
sample_data/full_load_sample.parquet
```

Run the full S&P 500 version and upload it to S3:

```bash
python ingestion/full_load.py --upload-s3
```

That single command writes the local sample and uploads the full dataset.

S3 output:

```text
s3://your-bucket/<FOLDER_PREFIX>/full/full_load.parquet
```

## Incremental Load

Writes a local 10-ticker sample for the latest available trading day:

```bash
python ingestion/incremental_load.py
```

Output:

```text
sample_data/incremental_load_sample.parquet
```

Run the full S&P 500 incremental version and upload it to S3:

```bash
python ingestion/incremental_load.py --upload-s3
```

That single command writes the local sample and uploads the full incremental dataset.

S3 output:

```text
s3://your-bucket/<FOLDER_PREFIX>/incremental/YYYY-MM-DD/data.parquet
```

AWS credentials should be configured through the normal AWS environment variables, AWS CLI profile,
an IAM role, or the local `.env` file.
