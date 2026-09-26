# Data dictionary

Column-level definitions for Bronze samples, Silver tables, and Gold tables. Authoritative schemas for implemented tables also appear in the root `README.md`.

## Bronze — stock prices (raw)

Source: Yahoo Finance via `yfinance`. Sample file: `sample_data/full_load_sample.csv`.

| Column | Type | Description |
| --- | --- | --- |
| `trade_date` | date | Trading session date |
| `open` | float | Opening price (USD) |
| `high` | float | Intraday high |
| `low` | float | Intraday low |
| `close` | float | Closing price |
| `adj_close` | float | Split/dividend-adjusted close |
| `volume` | int | Share volume |
| `dividends` | float | Dividend amount on ex-date (often 0) |
| `stock_splits` | float | Split ratio (often 0) |
| `ticker` | string | Symbol |
| `ingested_at` | timestamp (ISO) | UTC ingest time |
| `_ingest_date` | date | Partition key — calendar date of load |
| `source` | string | `yfinance` |

## Bronze — company profile (raw JSON)

Sample: `sample_data/company_profile_sample.json`.

| Field | Type | Description |
| --- | --- | --- |
| `ticker` | string | Symbol |
| `company_name` | string | Legal or long name |
| `sector` | string | GICS-style sector |
| `industry` | string | GICS-style industry |
| `country` | string | Country of incorporation |
| `website` | string | Corporate URL |
| `employees` | int | Approximate headcount |
| `ingested_at` | timestamp (ISO) | Load time |

## Silver — `stock_prices_silver`

See root README **Data Model** section. Key: `(trade_date, ticker)`.

## Silver — `company_dim` (SCD2)

See root README **Data Model** section. Filter `is_current = true` for current attributes.

## Gold tables

See root README for `fact_daily_returns`, `agg_sector_performance`, and `dim_company_summary`.
