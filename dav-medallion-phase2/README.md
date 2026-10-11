# Phase 2 — Core Pipeline Engineering

The complete Bronze/Silver data dictionaries, incremental/manual/backfill instructions,
GitHub Actions configuration and executable-proof instructions are in the
[repository README](../README.md).

Entry points:

1. `notebooks/01_raw_to_bronze.py` — append raw prices and initialize the company snapshot.
2. `notebooks/02_bronze_to_silver.py` — cast, quarantine, enrich and MERGE.
3. `notebooks/03_demo_idempotency_and_audit.py` — runnable rubric demonstration.
4. `notebooks/04_run_pipeline_job.py` — optional single-notebook manual run of both steps.

Use `process_date`, `load_type`, `source_file` (manual override), `metadata_file`,
`file_format`, and `lakehouse_root`. Free Edition uses `/Volumes/...` raw files
and `uc:<catalog>.<schema>` managed Delta tables. No Gold layer is implemented.
