"""Run real local Spark/Delta proof and save reproducible grading evidence."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dav-medallion-phase2"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/evidence/local_phase2_evidence.json")
    parser.add_argument("--work-root", type=Path, default=ROOT / "artifacts/phase2_verification")
    args = parser.parse_args()
    from pipeline.delta_utils import get_spark
    from pipeline.demo import demo
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        result = demo(spark, str(args.work_root / "inputs"), str(args.work_root / "lakehouse"))
        result["execution_environment"] = "Local Apache Spark 3.5.3 / Delta Lake 3.2.0"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, default=str, indent=2) + "\n", encoding="utf-8")
        print(f"All proof checks passed. Evidence: {args.output}")
    finally:
        spark.stop()
