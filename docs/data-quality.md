# Data quality rules

Enforced in `silver/03_data_quality_checks.py` before Gold promotion (Phase 2).

| Rule ID | Dimension | Check | Threshold | Action |
| --- | --- | --- | --- | --- |
| DQ-01 | Uniqueness | No duplicate `(trade_date, ticker)` in Silver | 0 duplicates | Fail job; run MERGE repair |
| DQ-02 | Completeness | Required price fields non-null | 100% for promoted rows | Drop nulls; log count |
| DQ-03 | Validity | `low <= open, close <= high` | 100% | Quarantine to `silver/_quarantine` |
| DQ-04 | Validity | `volume >= 0` | 100% | Null negative volumes; log |
| DQ-05 | Referential | Fact tickers exist in `company_dim` | 100% | Quarantine unknown tickers |
| DQ-06 | Freshness | Latest U.S. trading day present in Gold | Lag ≤ 1 business day | Fail scheduled job |
| DQ-07 | Volume | Daily Silver row count | 10 rows on full trading day | Warning if mismatch |

Sample validation can be run locally against `sample_data/silver_sample.csv` once the check notebook is implemented.
