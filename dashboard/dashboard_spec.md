# Power BI dashboard specification

**Report name:** Stock Market Medallion Dashboard  
**Data source:** Gold layer (`fact_daily_returns`, `agg_sector_performance`, `dim_company_summary`)

## Page 1 — Price trends

- **Slicer:** `ticker` (multi-select, default all)
- **Visual:** Line chart — `trade_date` vs. `close`; optional overlay `ma_20`, `ma_50`
- **Questions:** Where is price relative to moving averages? Which names trend together?

## Page 2 — Sector performance

- **Slicer:** Date range
- **Visual:** Matrix — rows = `sector`, columns = month, values = `avg_daily_return_pct` (color scale)
- **Visual:** Table — `advancing_count`, `declining_count`, `best_ticker`, `worst_ticker` by day

## Page 3 — Top gainers / losers

- **Slicer:** Single `trade_date` or rolling window
- **Visual:** Bar chart — top 5 and bottom 5 by `daily_return_pct`

## Page 4 — Company summary

- **Visual:** Scatter — X = `annualized_volatility`, Y = `total_return_pct`, size = `total_volume`
- **Visual:** KPI cards — portfolio avg return, best/worst ticker by rank

See `measure_definitions.md` for DAX measures.
