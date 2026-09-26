# DAX measures (planned)

```dax
Total Return % =
    DIVIDE(MAX(fact_daily_returns[cum_return]) - 1, 1)

Avg Daily Return % =
    AVERAGE(fact_daily_returns[daily_return_pct])

Volatility (Ann.) =
    STDEV.S(fact_daily_returns[daily_return]) * SQRT(252)

Sector Avg Return =
    AVERAGE(agg_sector_performance[avg_daily_return_pct])

Advancing Members =
    SUM(agg_sector_performance[advancing_count])
```

Measures rely on metrics pre-computed in Gold to keep the report lightweight.
