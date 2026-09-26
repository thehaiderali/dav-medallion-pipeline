# Silver layer

Cleansed, conformed tables in Unity Catalog schema `dav.silver`.

| Script | Output |
| --- | --- |
| `01_build_stock_prices_silver.py` | `stock_prices_silver` |
| `02_build_company_dim.py` | `company_dim` (SCD2) |
| `03_data_quality_checks.py` | Validation gate |

Preview shape: `sample_data/silver_sample.csv`.
