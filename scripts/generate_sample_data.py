"""
S&P 500 sample data (CSV only).

Wikipedia ticker list + batched yf.download. Two separate runs (instructor incremental rule).

  python -u scripts/generate_sample_data.py --mode full --end-date 2026-09-28
  python -u scripts/generate_sample_data.py --mode incremental --start-date 2026-09-29

Progress: sample_data/download_progress.log
"""

from __future__ import annotations

import argparse
import json
import time
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore", category=FutureWarning)

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "sample_data"
TICKER_CSV = ROOT / "data" / "sp500_tickers.csv"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
FULL_CSV = OUT_DIR / "full_load_sample.csv"
INCREMENTAL_CSV = OUT_DIR / "incremental_load_sample.csv"
MANIFEST = OUT_DIR / "MANIFEST.json"
PROGRESS_LOG = OUT_DIR / "download_progress.log"

CHUNK_SIZE = 100
CHUNK_PAUSE_SEC = 25.0

COLUMNS = [
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "stock_splits",
    "ticker",
    "ingested_at",
    "_ingest_date",
    "source",
]


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with PROGRESS_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def load_tickers() -> list[str]:
    if not TICKER_CSV.exists():
        raise SystemExit(f"Missing {TICKER_CSV}. Run: python scripts/fetch_sp500_tickers.py")
    tickers = pd.read_csv(TICKER_CSV)["ticker"].astype(str).str.replace(".", "-", regex=False).tolist()
    log(f"Loaded {len(tickers)} tickers from {TICKER_CSV.name}")
    return tickers


def meta() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {"ingested_at": now.isoformat(), "_ingest_date": now.date().isoformat(), "source": "yfinance"}


def long_frame(raw: pd.DataFrame, tickers: list[str], end: date) -> pd.DataFrame:
    m = meta()
    parts: list[pd.DataFrame] = []
    if raw is None or raw.empty:
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        for sym in tickers:
            try:
                sub = raw[sym].copy()
            except KeyError:
                continue
            if sub.dropna(how="all").empty:
                continue
            sub = sub.reset_index()
            sub.columns = [str(c).lower().replace(" ", "_") for c in sub.columns]
            if "date" in sub.columns:
                sub = sub.rename(columns={"date": "trade_date"})
            if "adj_close" not in sub.columns and "close" in sub.columns:
                sub["adj_close"] = sub["close"]
            if "dividends" not in sub.columns:
                sub["dividends"] = 0.0
            if "stock_splits" not in sub.columns:
                sub["stock_splits"] = 0.0
            sub["trade_date"] = pd.to_datetime(sub["trade_date"]).dt.date
            sub = sub[sub["trade_date"] <= end]
            sub["ticker"] = sym.upper()
            for k, v in m.items():
                sub[k] = v
            parts.append(sub[[c for c in COLUMNS if c in sub.columns]])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def download_batch(tickers: list[str], start: date, end: date, chunk_size: int, pause: float) -> pd.DataFrame:
    chunks = [tickers[i : i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    log(f"Plan: {len(chunks)} yf.download call(s), pause {pause}s")
    frames: list[pd.DataFrame] = []
    got: set[str] = set()

    for n, chunk in enumerate(chunks, 1):
        log(f"Chunk {n}/{len(chunks)} — downloading {len(chunk)} symbols …")
        t0 = time.time()
        raw = yf.download(
            chunk,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=False,
            progress=True,
        )
        frame = long_frame(raw, chunk, end)
        log(f"Chunk {n} done in {time.time() - t0:.0f}s — {len(frame):,} rows")
        if not frame.empty:
            frames.append(frame)
            got.update(frame["ticker"].unique())
        if n < len(chunks):
            log(f"Sleep {pause}s (rate limit)")
            time.sleep(pause)

    missing = [t for t in tickers if t.upper() not in got]
    if missing:
        (OUT_DIR / "download_failures.txt").write_text("\n".join(missing), encoding="utf-8")
        log(f"{len(missing)} symbols missing — see download_failures.txt")

    if not frames:
        raise SystemExit("No data returned. Wait 30–60 min and run once (Yahoo rate limit).")
    out = pd.concat(frames, ignore_index=True)
    log(f"Total: {len(out):,} rows, {out['ticker'].nunique()} symbols")
    return out


def previews(full: pd.DataFrame) -> None:
    head = full.head(200)
    head.to_csv(OUT_DIR / "bronze_sample.csv", index=False)
    s = head[["trade_date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]].copy()
    s["currency"] = "USD"
    s["source"] = "yfinance"
    s.to_csv(OUT_DIR / "silver_sample.csv", index=False)


def company_preview() -> None:
    rows = pd.read_csv(TICKER_CSV).head(20)
    ts = datetime.now(timezone.utc).isoformat()
    data = [
        {"ticker": r.ticker, "company_name": r.company_name, "sector": r.sector, "ingested_at": ts}
        for r in rows.itertuples(index=False)
    ]
    (OUT_DIR / "company_profile_sample.json").write_text(json.dumps(data, indent=2), encoding="utf-8")


def save_manifest(**fields: object) -> None:
    data = json.loads(MANIFEST.read_text(encoding="utf-8")) if MANIFEST.exists() else {}
    data.update(fields)
    MANIFEST.write_text(json.dumps(data, indent=2), encoding="utf-8")


def run_full(end: date, chunk: int, pause: float) -> None:
    PROGRESS_LOG.write_text("", encoding="utf-8")
    log(f"FULL LOAD through {end} -> {FULL_CSV.name}")
    tickers = load_tickers()
    start = end - timedelta(days=365 * 5 + 30)
    full = download_batch(tickers, start, end, chunk, pause)
    full.to_csv(FULL_CSV, index=False)
    previews(full)
    company_preview()
    save_manifest(
        full_load_cutoff_date=end.isoformat(),
        full_load_row_count=len(full),
        full_load_trade_date_max=str(full["trade_date"].max()),
        symbols_downloaded=int(full["ticker"].nunique()),
    )
    log(f"Wrote {FULL_CSV}")


def run_incremental(start: date, chunk: int, pause: float) -> None:
    if not FULL_CSV.exists():
        raise SystemExit(f"Run full load first ({FULL_CSV.name}).")
    full_max = pd.to_datetime(pd.read_csv(FULL_CSV, usecols=["trade_date"])["trade_date"].max()).date()
    if start <= full_max:
        raise SystemExit(f"--start-date must be after full max {full_max}")
    log(f"INCREMENTAL from {start} -> {INCREMENTAL_CSV.name}")
    tickers = load_tickers()
    inc = download_batch(tickers, start, date.today(), chunk, pause)
    if (pd.to_datetime(inc["trade_date"]).dt.date <= full_max).any():
        raise SystemExit("Incremental overlaps full load.")
    inc.to_csv(INCREMENTAL_CSV, index=False)
    save_manifest(
        incremental_start_date=start.isoformat(),
        incremental_row_count=len(inc),
        incremental_trade_date_min=str(inc["trade_date"].min()),
        incremental_trade_date_max=str(inc["trade_date"].max()),
    )
    log(f"Wrote {INCREMENTAL_CSV}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["full", "incremental"], required=True)
    p.add_argument("--end-date", type=date.fromisoformat)
    p.add_argument("--start-date", type=date.fromisoformat)
    p.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    p.add_argument("--pause-sec", type=float, default=CHUNK_PAUSE_SEC)
    args = p.parse_args()
    if args.mode == "full":
        if not args.end_date:
            raise SystemExit("--end-date required")
        run_full(args.end_date, args.chunk_size, args.pause_sec)
    else:
        if not args.start_date:
            raise SystemExit("--start-date required")
        run_incremental(args.start_date, args.chunk_size, args.pause_sec)


if __name__ == "__main__":
    main()
