# Bronze layer

Append-only ingestion from Yahoo Finance (`yfinance`).

| Script | Purpose | Phase |
| --- | --- | --- |
| `01_ingest_full_load.py` | 5-year historical backfill | 2 |
| `02_ingest_incremental.py` | Daily incremental append | 2 |
| `03_ingest_company_profile.py` | Company reference JSON | 2 |

Phase 1 deliverables: sample payloads in `/sample_data/` and specification in `docs/project-proposal.md`.
