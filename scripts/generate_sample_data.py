"""
Download Yahoo Finance samples for Phase 1 proposal artifacts.

Writes:
  sample_data/full_load_sample.csv      — 5y history, all 10 tickers (full-load payload shape)
  sample_data/incremental_load_sample.csv — last 5 trading days (incremental payload shape)
  sample_data/company_profile_sample.json — static reference data snapshot
  sample_data/bronze_sample.csv         — small Bronze-style subset (README quick reference)
  sample_data/silver_sample.csv         — cleansed subset aligned to Silver schema
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "JPM", "XOM", "PG", "DIS"]
OUT_DIR = Path(__file__).resolve().parents[1] / "sample_data"

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

# Fallback when live profile API is unavailable (public reference data).
STATIC_PROFILES: list[dict] = [
    {
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "sector": "Information Technology",
        "industry": "Consumer Electronics",
        "country": "United States",
        "website": "https://www.apple.com",
        "employees": 161000,
    },
    {
        "ticker": "MSFT",
        "company_name": "Microsoft Corporation",
        "sector": "Information Technology",
        "industry": "Software—Infrastructure",
        "country": "United States",
        "website": "https://www.microsoft.com",
        "employees": 228000,
    },
    {
        "ticker": "NVDA",
        "company_name": "NVIDIA Corporation",
        "sector": "Information Technology",
        "industry": "Semiconductors",
        "country": "United States",
        "website": "https://www.nvidia.com",
        "employees": 36000,
    },
    {
        "ticker": "GOOGL",
        "company_name": "Alphabet Inc.",
        "sector": "Communication Services",
        "industry": "Internet Content & Information",
        "country": "United States",
        "website": "https://abc.xyz",
        "employees": 182000,
    },
    {
        "ticker": "DIS",
        "company_name": "The Walt Disney Company",
        "sector": "Communication Services",
        "industry": "Entertainment",
        "country": "United States",
        "website": "https://www.thewaltdisneycompany.com",
        "employees": 225000,
    },
    {
        "ticker": "AMZN",
        "company_name": "Amazon.com, Inc.",
        "sector": "Consumer Discretionary",
        "industry": "Internet Retail",
        "country": "United States",
        "website": "https://www.amazon.com",
        "employees": 1525000,
    },
    {
        "ticker": "TSLA",
        "company_name": "Tesla, Inc.",
        "sector": "Consumer Discretionary",
        "industry": "Auto Manufacturers",
        "country": "United States",
        "website": "https://www.tesla.com",
        "employees": 125665,
    },
    {
        "ticker": "JPM",
        "company_name": "JPMorgan Chase & Co.",
        "sector": "Financials",
        "industry": "Banks—Diversified",
        "country": "United States",
        "website": "https://www.jpmorganchase.com",
        "employees": 317233,
    },
    {
        "ticker": "PG",
        "company_name": "The Procter & Gamble Company",
        "sector": "Consumer Staples",
        "industry": "Household & Personal Products",
        "country": "United States",
        "website": "https://www.pg.com",
        "employees": 107000,
    },
    {
        "ticker": "XOM",
        "company_name": "Exxon Mobil Corporation",
        "sector": "Energy",
        "industry": "Oil & Gas Integrated",
        "country": "United States",
        "website": "https://corporate.exxonmobil.com",
        "employees": 62000,
    },
]


def attach_ingest_metadata(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = df.copy()
    df["ticker"] = ticker
    df["dividends"] = 0.0
    df["stock_splits"] = 0.0
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_ingest_date"] = datetime.now(timezone.utc).date().isoformat()
    df["source"] = "yfinance"
    return df


def fetch_chart_range(ticker: str, range_: str) -> pd.DataFrame:
    response = requests.get(
        YAHOO_CHART.format(ticker=ticker),
        params={"interval": "1d", "range": range_},
        headers=HEADERS,
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    adj = result["indicators"]["adjclose"][0]["adjclose"]
    timestamps = result["timestamp"]

    frame = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(timestamps, unit="s", utc=True).date,
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["close"],
            "adj_close": adj,
            "volume": quote["volume"],
        }
    )
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    frame["volume"] = frame["volume"].fillna(0).astype("int64")
    return attach_ingest_metadata(frame, ticker)


def download_ticker(ticker: str, range_: str) -> pd.DataFrame:
    try:
        period = "5y" if range_ == "5y" else "5d"
        raw = yf.download(ticker, period=period, progress=False, auto_adjust=False)
        if raw is not None and not raw.empty:
            df = raw.reset_index()
            df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
            if "date" in df.columns:
                df = df.rename(columns={"date": "trade_date"})
            if "adj close" in " ".join(str(c) for c in raw.columns):
                pass
            for col in list(df.columns):
                if col.replace("_", " ") == "adj close" or col == "adj_close":
                    continue
            if "adj_close" not in df.columns:
                close_col = [c for c in df.columns if "adj" in c and "close" in c]
                if close_col:
                    df = df.rename(columns={close_col[0]: "adj_close"})
            if "adj_close" not in df.columns:
                df["adj_close"] = df["close"]
            if "dividends" not in df.columns:
                df["dividends"] = 0.0
            if "stock_splits" not in df.columns:
                df["stock_splits"] = 0.0
            keep = [
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "dividends",
                "stock_splits",
            ]
            df = df[[c for c in keep if c in df.columns]]
            return attach_ingest_metadata(df, ticker)
    except Exception as exc:  # noqa: BLE001
        print(f"[yfinance] {ticker} failed ({exc}); using chart API")

    return fetch_chart_range(ticker, range_)


def build_silver_sample(bronze: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "trade_date",
        "ticker",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    ]
    silver = bronze[cols].copy()
    silver["ticker"] = silver["ticker"].str.upper().str.strip()
    silver["currency"] = "USD"
    silver["source"] = "yfinance"
    silver["silver_loaded_at"] = datetime.now(timezone.utc).isoformat()
    for price_col in ("open", "high", "low", "close"):
        silver[price_col] = silver[price_col].round(4)
    silver = silver.drop_duplicates(subset=["trade_date", "ticker"])
    return silver


def build_company_profiles() -> list[dict]:
    profiles: list[dict] = []
    ingested_at = datetime.now(timezone.utc).isoformat()
    for ticker in TICKERS:
        try:
            info = yf.Ticker(ticker, session=requests.Session()).info
            if info and info.get("longName"):
                profiles.append(
                    {
                        "ticker": ticker,
                        "company_name": info.get("longName") or info.get("shortName"),
                        "sector": info.get("sector"),
                        "industry": info.get("industry"),
                        "country": info.get("country"),
                        "website": info.get("website"),
                        "employees": info.get("fullTimeEmployees"),
                        "ingested_at": ingested_at,
                    }
                )
                time.sleep(1)
                continue
        except Exception:
            pass

        static = next(row for row in STATIC_PROFILES if row["ticker"] == ticker)
        profiles.append({**static, "ingested_at": ingested_at})
    return profiles


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    full_frames: list[pd.DataFrame] = []
    for ticker in TICKERS:
        print(f"Full load sample: {ticker}")
        full_frames.append(download_ticker(ticker, "5y"))
        time.sleep(1)

    full_load = pd.concat(full_frames, ignore_index=True)
    full_path = OUT_DIR / "full_load_sample.csv"
    full_load.to_csv(full_path, index=False)
    size_mb = full_path.stat().st_size / (1024 * 1024)
    print(f"Wrote {full_path} ({len(full_load):,} rows, {size_mb:.2f} MB)")

    incremental_frames: list[pd.DataFrame] = []
    for ticker in TICKERS:
        print(f"Incremental sample: {ticker}")
        incremental_frames.append(download_ticker(ticker, "5d"))
        time.sleep(1)

    incremental = pd.concat(incremental_frames, ignore_index=True)
    inc_path = OUT_DIR / "incremental_load_sample.csv"
    incremental.to_csv(inc_path, index=False)
    inc_kb = inc_path.stat().st_size / 1024
    print(f"Wrote {inc_path} ({len(incremental):,} rows, {inc_kb:.1f} KB)")

    bronze_sample = full_load.head(200)
    bronze_sample.to_csv(OUT_DIR / "bronze_sample.csv", index=False)

    silver_sample = build_silver_sample(bronze_sample)
    silver_sample.to_csv(OUT_DIR / "silver_sample.csv", index=False)

    profiles = build_company_profiles()
    profile_path = OUT_DIR / "company_profile_sample.json"
    profile_path.write_text(json.dumps(profiles, indent=2), encoding="utf-8")
    print(f"Wrote {profile_path} ({len(profiles)} companies)")


if __name__ == "__main__":
    main()
