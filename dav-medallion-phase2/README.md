# Pipeline package — Bronze, Silver and Gold

The complete Bronze/Silver data dictionaries, incremental/manual/backfill instructions,
GitHub Actions configuration and executable-proof instructions are in the
[repository README](../README.md).

Entry points:

1. `notebooks/00_end_to_end.py` — one invocation through Bronze, Silver and Gold.
2. `notebooks/01_raw_to_bronze.py` — append raw prices and initialize the company snapshot.
3. `notebooks/02_bronze_to_silver.py` — cast, quarantine, enrich, MERGE, and refresh Gold.
4. `notebooks/03_demo_idempotency_and_audit.py` — runnable proof with Gold reconciliation assertions.
5. `notebooks/04_silver_to_gold.py` — full Gold synchronization from existing Silver.

Use `process_date`, `load_type`, `source_file` (manual override), `metadata_file`,
`file_format`, and `lakehouse_root`. Free Edition uses `/Volumes/...` raw files
and `uc:<catalog>.<schema>` managed Delta tables. Databricks supplies the runtime;
no local Spark setup or caching/persist is used. Gold is implemented in
`pipeline/gold.py`; see `docs/gold-model-and-power-bi.md` at the repository root.
