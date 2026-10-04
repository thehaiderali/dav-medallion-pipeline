"""
Full-load ingestion for S&P 500 daily OHLCV data.

Default behavior writes a small 10-ticker Parquet sample to:
    sample_data/full_load_sample.parquet

Use --upload-s3 for the full S&P 500 pull and S3 upload:
    python ingestion/full_load.py --upload-s3

Set S3_BUCKET before uploading. The object key is:
    raw/sp500/full/full_load.parquet
"""

from __future__ import annotations

import argparse
import os
import tempfile
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = ROOT / "sample_data" / "full_load_sample.parquet"
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
    response = requests.get(
        TICKER_SOURCE,
        headers={"User-Agent": "dav-dataset-ingestion/1.0"},
        timeout=120,
    )
    response.raise_for_status()
    table = pd.read_html(StringIO(response.text))[0]
    return table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()


def download_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame:
    raw = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    if raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    frame = raw.reset_index()
    frame.columns = [str(column).lower().replace(" ", "_") for column in frame.columns]
    frame["ticker"] = ticker

    if "date" not in frame.columns and "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})

    columns = ["ticker", "date", "open", "high", "low", "close", "volume"]
    return frame[columns]


def build_dataset(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    frames = []
    for ticker in tickers:
        print(f"Downloading {ticker}")
        frame = download_ohlcv(ticker, start=start, end=end)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError("No price data was downloaded.")

    data = pd.concat(frames, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"]).dt.date
    return data.sort_values(["ticker", "date"]).reset_index(drop=True)


def upload_to_s3(local_path: Path, bucket: str, key: str) -> None:
    import boto3

    client = boto3.client("s3", region_name=env_value("AWS_REGION", "S3_REGION"))
    client.upload_file(str(local_path), bucket, key)
    print(f"Uploaded s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--end", default="2024-01-01")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of tickers in the local sample.")
    parser.add_argument("--upload-s3", action="store_true", help="Upload the generated file to S3.")
    parser.add_argument("--s3-bucket", default=env_value("S3_BUCKET", "BUCKET_NAME"))
    args = parser.parse_args()

    sample_tickers = SAMPLE_TICKERS[: args.sample_size]
    data = build_dataset(tickers=sample_tickers, start=args.start, end=args.end)

    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    data.to_parquet(SAMPLE_PATH, index=False)
    print(f"Wrote {len(data):,} rows to {SAMPLE_PATH}")

    if args.upload_s3:
        if not args.s3_bucket:
            raise SystemExit("Set S3_BUCKET or pass --s3-bucket before using --upload-s3.")

        with tempfile.TemporaryDirectory() as tmp:
            full_path = Path(tmp) / "full_load.parquet"
            full_data = build_dataset(get_sp500_tickers(), start=args.start, end=args.end)
            full_data.to_parquet(full_path, index=False)
            upload_to_s3(full_path, args.s3_bucket, s3_key("full", "full_load.parquet"))


if __name__ == "__main__":
    main()
