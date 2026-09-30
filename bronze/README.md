# Bronze layer

S&P 500 ingestion from `data/sp500_tickers.csv` via batched `yf.download` → Bronze CSV/Delta append by `_ingest_date`.

Sample payloads: `sample_data/full_load_sample.csv` and `incremental_load_sample.csv` (see `sample_data/README.md`).
