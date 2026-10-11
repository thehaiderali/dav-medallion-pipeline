"""One-time, free Wikipedia company snapshot. Never scraped by daily ingestion."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
SOURCE_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_PATH = ROOT / "data" / "company_metadata.csv"


def scrape(output: Path = DEFAULT_PATH) -> None:
    response = requests.get(SOURCE_URL, headers={"User-Agent": "Mozilla/5.0 DAV educational lakehouse"}, timeout=90)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    table = next(t for t in tables if {"Symbol", "Security", "GICS Sector", "GICS Sub-Industry"} <= set(t.columns))
    frame = table[["Symbol", "Security", "GICS Sector", "GICS Sub-Industry"]].copy()
    frame.columns = ["ticker", "company_name", "sector", "sub_industry"]
    frame["ticker"] = frame["ticker"].str.replace(".", "-", regex=False).str.upper().str.strip()
    if frame.ticker.duplicated().any() or frame.isna().any().any() or len(frame) < 450:
        raise ValueError("Wikipedia snapshot failed completeness/uniqueness checks")
    frame = frame.sort_values("ticker").reset_index(drop=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    manifest = {"source_url": SOURCE_URL, "captured_at": datetime.now(timezone.utc).isoformat(), "rows": len(frame), "columns": list(frame.columns)}
    output.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {len(frame)} companies to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_PATH)
    args = parser.parse_args()
    scrape(args.output)
