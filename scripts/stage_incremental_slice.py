"""Copy one successfully extracted raw slice into the repository for versioning."""
import argparse
import shutil
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--process-date", required=True)
    args = parser.parse_args()
    selected = str(date.fromisoformat(args.process_date))
    source = ROOT / "raw_data/incremental" / selected / "data.jsonl"
    destination = ROOT / "data/price_slices" / f"{selected}.jsonl"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    print(f"Prepared repository slice: {destination}")
