# Sample data (CSV)

## Setup

```bash
pip install -r requirements.txt
python scripts/fetch_sp500_tickers.py
```

## Full load (run once — ~20–35 min, do not cancel mid-chunk)

```bash
python -u scripts/generate_sample_data.py --mode full --end-date 2026-09-28
```

Watch progress:

```powershell
Get-Content sample_data\download_progress.log -Wait -Tail 5
```

## Incremental (separate run, after full finishes)

```bash
python -u scripts/generate_sample_data.py --mode incremental --start-date 2026-09-29
```

| File | Purpose |
| --- | --- |
| `full_load_sample.csv` | Full load through **2026-09-28** |
| `incremental_load_sample.csv` | New rows only, from **2026-09-29** |
| `MANIFEST.json` | Row counts and dates |

Never slice incremental from the full CSV.
