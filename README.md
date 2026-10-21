# S&P 500 Lakehouse — Phase 2: Bronze and Silver

Two free sources: **yfinance OHLCV prices** and a **one-time Wikipedia company directory snapshot**. Processing stops at Silver for this assignment.

```text
yfinance -> date-exact raw files -> Git daily slices -> Bronze prices (APPEND)
Wikipedia -> committed company_metadata.csv ------------> Bronze companies (OVERWRITE once)
                                                             |
                                     casts + validation + quarantine
                                                             |
                                    company INNER JOIN on ticker
                                                             |
                                 Silver prices/companies (MERGE INTO)
                                      every operation -> audit logs
```

## Repository structure

- `ingestion/market_data.py`: canonical extraction, retries, progress bars, resumable full-load cache, JSONL and S3 upload.
- `ingestion/company_metadata.py`: one-time Wikipedia scrape; daily extraction never scrapes Wikipedia.
- `data/company_metadata.csv`, `.manifest.json`: committed 503-company snapshot with sectors/sub-industries and provenance.
- `data/price_slices/`: versioned one-day raw inputs.
- `data/ingestion_runs/`: extraction dates, counts, missing tickers, hashes and optional S3 locations.
- `data/ingestion_cursor.json`: next sequential date, initially **2026-10-01**.
- `dav-medallion-phase2/pipeline/`: explicit contracts, readers, Delta storage, pipeline and auditing.
- `dav-medallion-phase2/notebooks/`: manual Bronze/Silver notebooks and executable grading demonstration.
- `.github/workflows/daily-incremental.yml`: midnight one-day extraction and publication.
- `dav-medallion-phase2/notebooks/03_demo_idempotency_and_audit.py`: executable serverless grading proof, with JSON output saved to a Volume.

## Extraction periods

Full load: **2022-10-01 through 2026-09-30 inclusive**. yfinance's exclusive end is `2026-10-01`. The locally executed full extraction produced **498,238 observations** and uploaded them to S3; one unavailable ticker (`VYLR`) is recorded in the manifest. The directory is today's fixed constituent snapshot, not historical index membership; newly listed companies have shorter histories.

Incremental extraction starts at **2026-10-01**, and fetches exactly `[process_date, process_date + 1 day)`. The October 1 test produced **503 observations** and uploaded them to S3.

```bash
pip install -r requirements.txt
python ingestion/market_data.py --load-type full --process-date 2026-09-30 --upload-s3
python ingestion/market_data.py --load-type incremental --process-date 2026-10-01
python scripts/stage_incremental_slice.py --process-date 2026-10-01
```

For optional local S3 uploads, use `.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, `S3_BUCKET`, `AWS_REGION`/`S3_REGION`, `FOLDER_PREFIX`) and `--upload-s3`. Default full output: `raw_data/full/full_load.jsonl`; incremental: `raw_data/incremental/YYYY-MM-DD/data.jsonl`. Large extracts and caches are gitignored; daily slices/manifests are versioned.

## Schemas and table storage

All schemas are explicit `StructType`/`StructField` contracts in `pipeline/contracts.py` and `pipeline/schemas.py`. JSONL reads use an explicit text schema then explicit JSON parsing. CSV headers and Parquet footers are metadata-inspected before constructing an explicit schema for the Spark reader. No Spark schema inference is used.

This pipeline targets **Databricks Free Edition serverless**. Use managed tables: `lakehouse_root=uc:<catalog>.dav_phase2`, and raw files in `/Volumes/<catalog>/<schema>/<volume>/...`. The table names below live in that schema. It does not create a local Spark session, cache/persist DataFrames, use RDDs/JVM-only Delta APIs, or store Delta tables inside a Volume. Databricks provides PySpark, Delta Lake and PyArrow; do not install a pinned local Spark/Delta runtime in the notebook.

Use a fresh schema when migrating from the earlier PR's incompatible table contracts. Python schema changes do not populate old persisted rows automatically.

### Bronze: `bronze_prices`

Grain: one **raw arrival** per append. Primary key: `bronze_row_id`. Replays intentionally create additional Bronze arrivals. Deterministic `record_key` identifies equivalent raw payloads within a batch for quarantine/reconciliation; it is not unique in append-only Bronze.

| Column | Type | Meaning |
|---|---|---|
| bronze_row_id | STRING | UUID arrival primary key |
| record_key | STRING | SHA-256 of batch, source file and raw payload |
| ticker | STRING | Original source symbol |
| date | STRING | Original trading date |
| open | STRING | Raw opening price |
| high | STRING | Raw high |
| low | STRING | Raw low |
| close | STRING | Raw closing price |
| adj_close | STRING | Raw adjusted close; optional for older files |
| volume | STRING | Raw volume |
| raw_payload | STRING | Original JSON; CSV/Parquet rows normalized to JSON |
| rescued_data | STRING | JSON containing additional source fields |
| load_type | STRING | full / incremental |
| batch_date | DATE | Logical process date |
| run_id | STRING | Invocation correlation ID |
| source_file | STRING | Physical raw file lineage |
| batch_id | STRING | `<load_type>_<YYYYMMDD>` |
| load_timestamp | TIMESTAMP | Arrival processing time |
| extra_<name>_<hash> | STRING | Dynamically added source field; retained with mergeSchema |

### Bronze: `bronze_companies`

Grain: one company in the committed snapshot. Primary key: `ticker`. Written with overwrite on initialization or explicit `refresh_metadata=true`; ordinary price runs reuse it.

| Column | Type | Meaning |
|---|---|---|
| ticker | STRING | Uppercase Yahoo-compatible ticker, primary key |
| company_name | STRING | Company name |
| sector | STRING | GICS sector |
| sub_industry | STRING | GICS sub-industry |
| snapshot_file | STRING | CSV file lineage |
| row_hash | STRING | Content hash |
| source_file | STRING | Wikipedia source URL |
| batch_id | STRING | Snapshot-content-based ID |
| load_timestamp | TIMESTAMP | Snapshot ingestion time |

### Silver: `silver_companies`

Primary key: `ticker`. Same columns/types as `bronze_companies`, with cleaned values and a Silver processing `load_timestamp`. Content-hash MERGE leaves unchanged rows, including timestamps, untouched on replay.

### Silver: `silver_prices`

Grain: one enriched company/trading-day observation. Composite primary key: **(`ticker`, `trade_date`)**. `trade_date` is the typed source `date`.

| Column | Type | Meaning |
|---|---|---|
| ticker | STRING | Clean uppercase symbol, PK |
| trade_date | DATE | Trading date, PK |
| open | DOUBLE | Valid positive opening price |
| high | DOUBLE | Valid positive high |
| low | DOUBLE | Valid positive low |
| close | DOUBLE | Valid positive close |
| adj_close | DOUBLE | Adjusted close; close fallback only when absent |
| volume | BIGINT | Nonnegative integral volume |
| company_name | STRING | Company directory enrichment |
| sector | STRING | GICS sector |
| sub_industry | STRING | GICS sub-industry |
| metadata_source_file | STRING | Directory source URL |
| metadata_batch_id | STRING | Directory snapshot identity |
| source_batch_date | DATE | Latest accepted logical price batch |
| row_hash | STRING | Typed business/enrichment content hash |
| source_file | STRING | Original raw price-file lineage |
| batch_id | STRING | Accepted price batch |
| load_timestamp | TIMESTAMP | Last Silver insert/update time |

### Quarantine and operational tables

`quarantine_prices`: primary key `record_key STRING`; raw price columns (all STRING), `raw_payload STRING`, `rescued_data STRING`, `reject_reason STRING`, `reject_stage STRING`, `batch_date DATE`, `source_file STRING`, `batch_id STRING`, `load_timestamp TIMESTAMP`. Bad JSON/malformed CSV, invalid/null dates, missing tickers, nonnumeric/nonpositive/non-finite prices, invalid/fractional/negative/overflow volume, high below low, and unmatched company metadata are isolated. Exact rejected replays are deduplicated with MERGE. Historical rejects remain available for inspection.

`pipeline_execution_logs`: append-only operational records; logical identity `(run_id, pipeline_layer, start_time, parameter_processed)`.

| Columns | Type |
|---|---|
| run_id, pipeline_layer, load_type, batch_id | STRING |
| parameter, parameter_processed | STRING |
| start_time, end_time, load_timestamp | TIMESTAMP |
| duration_seconds | DOUBLE |
| status, error_message | STRING |
| rows_read, rows_inserted, rows_updated, rows_quarantined | BIGINT |

Statuses are `SUCCESS`, `FAILURE`, `SKIPPED`. Each file/table operation is audited, including full and incremental loads, plus a `PIPELINE` invocation row. Invocation metrics aggregate operations; use individual layer rows for table-specific counts. Audit persistence failure raises an error instead of reporting silent success.

`schema_drift_log`: `run_id`, `batch_id`, `source_file`, `drift_type`, `column_name`, `expected_type`, `actual_type`, `action_taken` (STRING) and `load_timestamp TIMESTAMP`. Added columns are evolved into Bronze and retained in the original payload; uncastable values are quarantined at Silver.

## Manual Databricks run guide

1. Pull `phase-2` in the connected Git Folder (or `main` after merge).
2. Create a managed Volume through Catalog, and upload `data/company_metadata.csv` plus the desired price file. Full data is also available locally at `raw_data/full/full_load.jsonl` or at the S3 URI in its committed manifest.
3. Open `dav-medallion-phase2/notebooks/01_raw_to_bronze.py`; run the widget-definition cell, fill widgets, then run the remaining cells.
4. Open `02_bronze_to_silver.py`, use the same process date/load type/table root, and run it.
5. Inspect displayed tables, quarantine and execution logs. No Databricks API token is required for manual execution.

| Widget | Full load | One-day incremental | Historical backfill |
|---|---|---|---|
| process_date | 2026-09-30 | 2026-10-01 | Any available older slice date |
| load_type | full | incremental | Match the historical raw file |
| source_file | Full file `/Volumes/.../full_load.jsonl` | `/Volumes/.../2026-10-01.jsonl` | Exact historical file path |
| file_format | jsonl / csv / parquet / auto | Same choices | Same choices |
| metadata_file | Uploaded company CSV | Same CSV, or blank after initialization | Same behavior |
| lakehouse_root | uc:catalog.dav_phase2 | Same | Same |

**Manual fallback:** `source_file` always overrides automatic folder lookup. It works for full and incremental inputs without changing code. Select `file_format` explicitly for folders/nonstandard extensions. Without `source_file`, set `raw_path`; lookup is `full/full_load.jsonl` or `incremental/<process_date>/data.jsonl`. Use uploaded Volume paths. S3 paths require separately configured storage permissions; a Windows local path cannot be read by cloud Spark.

For a window, leave `process_date`, `batch_id`, and `source_file` blank; supply inclusive `start_date`/`end_date` and a partitioned `raw_path`. For Silver-only replay, supply a `batch_id` such as `full_20260930`. The pipeline selects by batch date; full historical trading dates live inside the one full batch.

The notebooks call `pipeline.engine.run_pipeline(spark, ...)` using Databricks' existing session. Both functions and widgets accept full/incremental parameters and manual file paths.

## MERGE, replays and robustness

Bronze retains raw arrivals, including duplicates and replays. Silver deduplicates MERGE sources, INNER JOINs valid companies, inserts new keys and updates changed existing keys. Identical same-batch input leaves **all Silver columns** unchanged, including timestamps. Newer batches advance accepted lineage even if values match; older batches cannot overwrite newer observations. Within a batch, latest ingestion wins, with deterministic payload-key tie-breaking. Logical batch dates order observations; same-date cross-source precedence should be controlled by source ordering.

Bad records do not crash the valid part of a batch. Failed file operations are logged and later backfill files continue. Malformed company snapshots fail their audited operation instead of overwriting a good snapshot. Unsupported file formats/physical structures are reported as file failures; supported scalar schema additions/type changes are handled by evolution/quarantine.

## Midnight GitHub Action — sequential historical dates

Schedule: **00:00 UTC daily**, not guaranteed to start at exactly that second (GitHub scheduling can be delayed). It uses `data/ingestion_cursor.json`, **not today's date**: October 1 first, October 2 next, then subsequent calendar slices. NYSE non-trading days produce committed `SKIPPED` manifests and empty slices. A publication failure leaves the cursor unchanged. Manual workflow input can replay one date without advancing a different cursor date.

Allow Actions write access to repository contents. Protected branches may require bot permission or a dedicated ingestion branch. No AWS secrets or variables are needed for the workflow.

The workflow commits `data/price_slices/<date>.jsonl`, its manifest, and the next cursor directly to the repository. Databricks is intentionally run manually. Scheduled workflows run from the default branch: merge this PR to activate scheduling, or use workflow_dispatch on phase-2 for testing. The locally tested October 1 input does not advance the committed cursor: the first scheduled run can reproduce October 1.

## Execution proof

Run `03_demo_idempotency_and_audit.py` in Databricks with a writable Volume input directory and a fresh dedicated demo schema. It executes full and incremental loads, exact rerun equality, in-place update, raw/Silver historical backfills, added-column evolution, bad-record quarantine, stale-batch protection and audit assertions. It displays schemas/tables/logs and writes `phase2_evidence.json` to the selected input directory. Export the executed notebook with outputs or commit the downloaded evidence; the unexecuted notebook alone is not proof.

Execution verification happens in Databricks serverless using the demo notebook. Source review alone does not establish runtime correctness; save the real notebook outputs/JSON or record the demonstration before submitting.

In notebook SQL cells, after the run notebooks create validation views:

```sql
DESCRIBE TABLE phase2_bronze_prices;
DESCRIBE TABLE phase2_silver_prices;
SELECT ticker, trade_date, COUNT(*) AS n FROM phase2_silver_prices GROUP BY ticker, trade_date HAVING COUNT(*) > 1;
SELECT pipeline_layer, parameter_processed, start_time, end_time, status, rows_inserted, rows_updated FROM pipeline_execution_logs ORDER BY start_time DESC;
SELECT * FROM phase2_quarantine;
SELECT * FROM schema_drift_log;
```
