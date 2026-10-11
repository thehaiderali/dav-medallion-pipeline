"""Native-Python syntax/configuration checks; never claims to execute Spark."""
from __future__ import annotations

import ast
import csv
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    count = 0
    for directory in [ROOT / "dav-medallion-phase2", ROOT / "ingestion", ROOT / "scripts"]:
        for file in directory.rglob("*.py"):
            if "__pycache__" not in file.parts:
                tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
                for node in ast.walk(tree):
                    if isinstance(node, ast.keyword) and node.arg == "inferSchema":
                        raise AssertionError(f"Schema inference configured in {file}")
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "option" and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "inferSchema":
                        raise AssertionError(f"Schema inference configured in {file}")
                count += 1
    companies = list(csv.DictReader((ROOT / "data/company_metadata.csv").open(encoding="utf-8")))
    assert len(companies) >= 450 and len({r["ticker"] for r in companies}) == len(companies)
    assert all(all(r[c] for c in ["ticker", "company_name", "sector", "sub_industry"]) for r in companies)
    cursor = json.loads((ROOT / "data/ingestion_cursor.json").read_text())
    assert cursor["next_process_date"]
    workflow = yaml.safe_load((ROOT / ".github/workflows/daily-incremental.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["schedule"][0]["cron"] == "0 0 * * *"
    assert "today" not in next(s["run"] for s in workflow["jobs"]["ingest"]["steps"] if s.get("id") == "params")
    print(f"PASS: {count} Python files parsed; no schema inference options; {len(companies)} unique companies; midnight sequential-cursor workflow parsed")


if __name__ == "__main__":
    main()
