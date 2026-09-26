# Sample data (Phase 1)

These files satisfy the **Phase 1** requirement for committed **full-load** and **incremental-load** raw payloads.

| File | Load type |
| --- | --- |
| `full_load_sample.csv` | Full load — 5 years × 10 tickers |
| `incremental_load_sample.csv` | Incremental — recent trading days |
| `company_profile_sample.json` | Company reference (dimension source) |
| `bronze_sample.csv` | Small Bronze preview (200 rows) |
| `silver_sample.csv` | Silver-schema preview |

Regenerate from Yahoo Finance:

```bash
pip install -r requirements.txt
python scripts/generate_sample_data.py
```

Data is for **educational use**; Yahoo Finance retains rights to underlying market data.
