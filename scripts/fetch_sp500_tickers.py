"""Write data/sp500_tickers.csv from Wikipedia."""

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "sp500_tickers.csv"
URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def main() -> None:
    html = requests.get(URL, headers={"User-Agent": "dav-medallion-pipeline/1.0"}, timeout=120).text
    table = pd.read_html(StringIO(html))[0]
    out = table.rename(
        columns={"Symbol": "ticker", "GICS Sector": "sector", "Security": "company_name"}
    )[["ticker", "company_name", "sector"]]
    out["ticker"] = out["ticker"].astype(str).str.replace(".", "-", regex=False)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"Wrote {len(out)} tickers to {OUT}")


if __name__ == "__main__":
    main()
