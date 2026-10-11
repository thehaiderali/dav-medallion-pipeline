# Project Proposal

> Historical Phase 1 proposal. Current ingestion dates, filenames and the
> Databricks serverless Bronze/Silver implementation are documented in the
> [repository README](../README.md). The legacy scripts mentioned below have
> been consolidated into `ingestion/market_data.py`.

## Dataset

This project ingests daily S&P 500 stock price data from Yahoo Finance using the open-source
`yfinance` Python package. The dataset contains public market data only and does not include PII.

## Source

| Item | Detail |
| --- | --- |
| Ticker universe | S&P 500 ticker list from Wikipedia |
| Price source | Yahoo Finance through `yfinance` |
| Historical range | `2019-01-01` to `2024-01-01` |
| Incremental range | Latest available trading day through `period="1d"` |

## Schema

| Column | Description |
| --- | --- |
| `ticker` | Stock ticker |
| `date` | Trading date |
| `open` | Opening price |
| `high` | Daily high |
| `low` | Daily low |
| `close` | Closing price |
| `volume` | Trading volume |

## Ingestion Design

### Full Load

`ingestion/full_load.py` downloads approximately five years of OHLCV data for the selected tickers.
By default, it writes a 10-ticker sample to:

```text
sample_data/full_load_sample.parquet
```

For the complete S&P 500 universe, run with `--upload-s3`. The full file is uploaded to:

```text
s3://your-bucket/<FOLDER_PREFIX>/full/full_load.parquet
```

### Incremental Load

`ingestion/incremental_load.py` downloads the latest available daily OHLCV record for each selected
ticker using `yf.download(ticker, period="1d")`. By default, it writes a 10-ticker sample to:

```text
sample_data/incremental_load_sample.parquet
```

For the complete S&P 500 universe, run with `--upload-s3`. The full file is uploaded to
a date-partitioned S3 path:

```text
s3://your-bucket/<FOLDER_PREFIX>/incremental/YYYY-MM-DD/data.parquet
```

AWS connection details are loaded from `.env`, including `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_BUCKET` or `BUCKET_NAME`, and `FOLDER_PREFIX`.

## Storage Format

Parquet is used because it is compressed, columnar, and works well with analytics engines such as
Spark, Athena, DuckDB, and Pandas.

## Security

The dataset contains only public stock market prices. No customer records, user accounts, emails,
payment details, or personal identifiers are collected.
