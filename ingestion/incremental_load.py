"""
Incremental ingestion for S&P 500 daily OHLCV data.

Default behavior writes a small 10-ticker Parquet sample to:
    sample_data/incremental_load_sample.parquet

Use --upload-s3 for the full S&P 500 incremental pull:
    python ingestion/incremental_load.py --upload-s3

Set S3_BUCKET before uploading. The object key is date partitioned:
    raw/sp500/incremental/YYYY-MM-DD/data.parquet
"""

from __future__ import annotations

import argparse
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = ROOT / "sample_data" / "incremental_load_sample.parquet"
TICKER_SOURCE = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SAMPLE_TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "JPM", "JNJ", "XOM"]

load_dotenv(ROOT / ".env")


def env_value(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def s3_key(*parts: str) -> str:
    prefix = os.getenv("FOLDER_PREFIX", "raw/sp500").strip("/")
    suffix = "/".join(part.strip("/") for part in parts)
    return f"{prefix}/{suffix}" if prefix else suffix


def get_sp500_tickers() -> list[str]:
    return pd.read_csv(ROOT / "data/company_metadata.csv")["ticker"].tolist()


def download_latest_ohlcv(ticker: str, process_date: date | None = None) -> pd.DataFrame:
    selected = process_date or date.today()
    raw = yf.download(ticker, start=str(selected), end=str(selected + timedelta(days=1)), progress=False, auto_adjust=False)
    if raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    frame = raw.reset_index()
    frame.columns = [str(column).lower().replace(" ", "_") for column in frame.columns]
    frame["ticker"] = ticker

    if "date" not in frame.columns and "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})

    columns = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    return frame[columns]


def build_dataset(tickers: list[str], process_date: date | None = None) -> pd.DataFrame:
    frames = []
    for ticker in tickers:
        print(f"Downloading {ticker}")
        frame = download_latest_ohlcv(ticker, process_date)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError("No incremental price data was downloaded.")

    data = pd.concat(frames, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"]).dt.date
    return data.sort_values(["ticker", "date"]).reset_index(drop=True)


def upload_to_s3(local_path: Path, bucket: str, partition_date: str) -> None:
    import boto3

    key = s3_key("incremental", partition_date, "data.parquet")
    client = boto3.client("s3", region_name=env_value("AWS_REGION", "S3_REGION"))
    client.upload_file(str(local_path), bucket, key)
    print(f"Uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-size", type=int, default=10, help="Number of tickers in the local sample.")
    parser.add_argument("--upload-s3", action="store_true", help="Upload the generated file to S3.")
    parser.add_argument("--s3-bucket", default=env_value("S3_BUCKET", "BUCKET_NAME"))
    parser.add_argument("--partition-date", default=date.today().isoformat())
    parser.add_argument("--process-date", help="Exact trading slice YYYY-MM-DD; overrides partition-date")
    args = parser.parse_args()

    sample_tickers = SAMPLE_TICKERS[: args.sample_size]
    selected = date.fromisoformat(args.process_date or args.partition_date)
    data = build_dataset(sample_tickers, selected)

    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    data.to_parquet(SAMPLE_PATH, index=False)
    print(f"Wrote {len(data):,} rows to {SAMPLE_PATH}")

    if args.upload_s3:
        if not args.s3_bucket:
            raise SystemExit("Set S3_BUCKET or pass --s3-bucket before using --upload-s3.")
        with tempfile.TemporaryDirectory() as tmp:
            full_path = Path(tmp) / "data.parquet"
            full_data = build_dataset(get_sp500_tickers(), selected)
            full_data.to_parquet(full_path, index=False)
            upload_to_s3(full_path, args.s3_bucket, str(selected))


if __name__ == "__main__":
    main()
