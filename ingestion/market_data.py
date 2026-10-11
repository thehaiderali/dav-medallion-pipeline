"""Date-exact yfinance extraction, JSONL raw contracts, S3 upload and run manifests."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from tqdm.auto import tqdm

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
FULL_START = "2022-10-01"
FULL_END_EXCLUSIVE = "2026-10-01"
INCREMENTAL_START = date(2026, 10, 1)
COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
LOG = logging.getLogger("market_ingestion")


def extract(start: str, end: str, tickers: list[str], cache_root: Path | None = None) -> tuple[pd.DataFrame, list[str]]:
    """Retry each ticker; record empty/delisted tickers, never fabricate observations."""
    frames, missing = [], []
    progress = tqdm(tickers, desc=f"Prices {start} to {end}", unit="ticker", dynamic_ncols=True)
    rows_downloaded = 0
    batch = {}
    for index, ticker in enumerate(progress, 1):
        progress.set_postfix(ticker=ticker, rows=rows_downloaded, missing=len(missing))
        cache_file = cache_root / f"{ticker}.parquet" if cache_root else None
        if cache_file and cache_file.exists():
            cached = pd.read_parquet(cache_file)
            frames.append(cached)
            rows_downloaded += len(cached)
            LOG.info("[%s/%s] %s: resumed %s cached rows", index, len(tickers), ticker, len(cached))
            continue
        if ticker not in batch:
            chunk = [t for t in tickers[index - 1:index + 23] if not cache_root or not (cache_root / f"{t}.parquet").exists()]
            combined = yf.download(chunk, start=start, end=end, auto_adjust=False, group_by="ticker", progress=False, threads=4, timeout=15)
            for symbol in chunk:
                if isinstance(combined.columns, pd.MultiIndex) and symbol in combined.columns.get_level_values(0):
                    batch[symbol] = combined[symbol].dropna(how="all")
                else:
                    batch[symbol] = pd.DataFrame()
        raw = batch.pop(ticker, pd.DataFrame())
        for attempt in range(3):
            if not raw.empty:
                break
            raw = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False, threads=False, timeout=15)
            if not raw.empty:
                break
            LOG.warning("%s: attempt %s/3 returned no data", ticker, attempt + 1)
            time.sleep(attempt + 1)
        if raw.empty:
            missing.append(ticker)
            LOG.warning("%s: no observations after retries; recorded in manifest", ticker)
            continue
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        frame = raw.reset_index()
        frame.columns = [str(c).lower().replace(" ", "_") for c in frame.columns]
        frame = frame.rename(columns={"datetime": "date"})
        frame["ticker"] = ticker
        frame["date"] = pd.to_datetime(frame["date"]).dt.date.astype(str)
        if "adj_close" not in frame:
            frame["adj_close"] = frame["close"]
        frames.append(frame[COLUMNS])
        if cache_file:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            frame[COLUMNS].to_parquet(cache_file, index=False)
        rows_downloaded += len(frame)
        LOG.info("[%s/%s] %s: %s rows downloaded (total=%s)", index, len(tickers), ticker, len(frame), rows_downloaded)
        if index % 25 == 0:
            LOG.info("Progress: %.1f%%, %s/%s tickers, %s rows, %s missing", 100 * index / len(tickers), index, len(tickers), rows_downloaded, len(missing))
    if not frames:
        return pd.DataFrame(columns=COLUMNS), missing
    return pd.concat(frames, ignore_index=True).sort_values(["ticker", "date"]), missing


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load-type", choices=["full", "incremental"], required=True)
    parser.add_argument("--process-date", default=date.today().isoformat())
    parser.add_argument("--start", default=FULL_START)
    parser.add_argument("--end", default=FULL_END_EXCLUSIVE, help="Exclusive yfinance end date")
    parser.add_argument("--metadata", type=Path, default=ROOT / "data/company_metadata.csv")
    parser.add_argument("--output-root", type=Path, default=ROOT / "raw_data")
    parser.add_argument("--manifest-root", type=Path, default=ROOT / "data/ingestion_runs")
    parser.add_argument("--upload-s3", action="store_true")
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET") or os.getenv("BUCKET_NAME"))
    parser.add_argument("--sample-size", type=int, default=0, help="0 = all snapshot tickers")
    args = parser.parse_args(argv)
    process_date = date.fromisoformat(args.process_date)
    if args.load_type == "incremental" and process_date < INCREMENTAL_START:
        parser.error("Incremental extraction starts on 2026-10-01")
    if process_date > date.today():
        parser.error("process_date cannot be in the future")
    start, end = (args.start, args.end) if args.load_type == "full" else (process_date.isoformat(), (process_date + timedelta(days=1)).isoformat())
    if date.fromisoformat(end) <= date.fromisoformat(start):
        parser.error("end must be after start")
    tickers = pd.read_csv(args.metadata)["ticker"].tolist()
    if args.sample_size:
        tickers = tickers[:args.sample_size]
    LOG.info("Starting %s extraction: %s to %s (end exclusive), %s tickers", args.load_type, start, end, len(tickers))
    cache_root = args.output_root / ".cache" / f"{start}_{end}" if args.load_type == "full" else None
    trading_session = args.load_type == "full" or not mcal.get_calendar("NYSE").schedule(start_date=process_date, end_date=process_date).empty
    if trading_session:
        frame, missing = extract(start, end, tickers, cache_root)
    else:
        LOG.info("%s is not a NYSE trading session; skipping extraction", process_date)
        frame, missing = pd.DataFrame(columns=COLUMNS), []
    if len(missing) > max(3, len(tickers) // 10) and trading_session:
        raise RuntimeError(f"Too many unavailable tickers ({len(missing)}/{len(tickers)}); refusing incomplete upload")
    # Weekends have no daily slice; exchange holidays produce no observations too.
    expected_session = trading_session
    if frame.empty and expected_session:
        raise RuntimeError("No observations downloaded. Verify market holiday/API availability before retrying.")
    relative = "full/full_load.jsonl" if args.load_type == "full" else f"incremental/{process_date}/data.jsonl"
    output = args.output_root / relative
    output.parent.mkdir(parents=True, exist_ok=True)
    # JSON primitives are stringified into a stable raw contract; Silver owns typing.
    lines = []
    for row in frame.to_dict("records"):
        lines.append(json.dumps({k: None if pd.isna(v) else str(v) for k, v in row.items()}, allow_nan=False, sort_keys=True))
    LOG.info("Writing %s observations to %s", len(lines), output)
    output.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    payload_hash = hashlib.sha256(output.read_bytes()).hexdigest()
    manifest = {"load_type": args.load_type, "process_date": str(process_date), "start": start, "end_exclusive": end, "rows": len(frame), "tickers_requested": len(tickers), "missing_tickers": missing, "sha256": payload_hash, "raw_file": relative, "status": "SUCCESS" if len(frame) else "SKIPPED", "extracted_at": datetime.now(timezone.utc).isoformat()}
    if args.upload_s3 and len(frame):
        if not args.s3_bucket:
            parser.error("Set S3_BUCKET/BUCKET_NAME or --s3-bucket")
        import boto3
        client = boto3.client("s3", region_name=os.getenv("AWS_REGION") or os.getenv("S3_REGION"))
        prefix = os.getenv("FOLDER_PREFIX", "raw/sp500").strip("/")
        key = f"{prefix}/{relative}" if prefix else relative
        LOG.info("Uploading %s bytes to S3", output.stat().st_size)
        with tqdm(total=output.stat().st_size, desc="S3 upload", unit="B", unit_scale=True, dynamic_ncols=True) as upload_progress:
            client.upload_file(str(output), args.s3_bucket, key, Callback=upload_progress.update)
        client.put_object(Bucket=args.s3_bucket, Key=key + ".manifest.json", Body=json.dumps(manifest).encode(), ContentType="application/json")
        manifest["s3_uri"] = f"s3://{args.s3_bucket}/{key}"
    args.manifest_root.mkdir(parents=True, exist_ok=True)
    manifest_path = args.manifest_root / f"{args.load_type}_{process_date}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    LOG.info("Finished in %.1fs: rows=%s missing_tickers=%s status=%s", time.monotonic() - started, len(frame), len(missing), manifest["status"])
    print(json.dumps({"rows": len(frame), "missing_tickers": len(missing), "manifest": str(manifest_path), "status": manifest["status"]}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
