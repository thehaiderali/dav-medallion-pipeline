"""Advance the committed one-day cursor only after a successful workflow run."""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def advance(process_date: str, cursor_path: Path = ROOT / "data/ingestion_cursor.json"):
    cursor = json.loads(cursor_path.read_text(encoding="utf-8"))
    selected = date.fromisoformat(process_date)
    if selected != date.fromisoformat(cursor["next_process_date"]):
        print("Manual slice did not match cursor; sequential cursor left unchanged")
        return
    cursor["next_process_date"] = str(selected + timedelta(days=1))
    cursor["last_completed_date"] = str(selected)
    cursor_path.write_text(json.dumps(cursor, indent=2) + "\n", encoding="utf-8")
    print(f"Next scheduled slice: {cursor['next_process_date']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--process-date", required=True)
    advance(parser.parse_args().process_date)
