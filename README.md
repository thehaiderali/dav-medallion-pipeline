# S&P 500 OHLCV Dataset Ingestion

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

## Structure

```text
dav-medallion-pipeline/
├── ingestion/
│   ├── full_load.py
│   └── incremental_load.py
├── sample_data/
│   ├── full_load_sample.parquet
│   └── incremental_load_sample.parquet
├── data/
│   └── sp500_tickers.csv
├── docs/
│   └── project-proposal.md
├── requirements.txt
└── README.md
```

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
