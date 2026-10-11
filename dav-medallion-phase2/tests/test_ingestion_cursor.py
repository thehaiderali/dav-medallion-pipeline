"""Cursor progression is independent of today's date and supports manual retries."""
import importlib.util
import json
from pathlib import Path


def test_cursor_advances_only_the_expected_slice(tmp_path):
    path = Path(__file__).resolve().parents[2] / "scripts/advance_ingestion_cursor.py"
    spec = importlib.util.spec_from_file_location("advance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    cursor = tmp_path / "cursor.json"
    cursor.write_text(json.dumps({"next_process_date": "2026-10-01"}))
    module.advance("2026-10-09", cursor)
    assert json.loads(cursor.read_text())["next_process_date"] == "2026-10-01"
    module.advance("2026-10-01", cursor)
    assert json.loads(cursor.read_text())["next_process_date"] == "2026-10-02"
    module.advance("2026-10-01", cursor)
    assert json.loads(cursor.read_text())["next_process_date"] == "2026-10-02"
