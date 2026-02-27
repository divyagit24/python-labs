# 🐍 Pandas & SQL-to-Python Data Projects

A collection of practical Python scripts, including translating complex **SQL queries
into Python-based transformations**.

This repository demonstrates hands-on experience in working with
structured datasets, performing data cleaning, transformations,
aggregations, and implementing SQL-style logic using Python.

# 📁 Scripts

| Script | What It Demonstrates |
|---|---|
| `joins_and_merges.py` | INNER, LEFT, RIGHT, FULL OUTER, MULTI-TABLE, ANTI JOIN |
| `pivot_and_reshape.py` | Pivot tables, melt/unpivot, crosstab, stack/unstack, transpose |
| `case_when_derived.py` | CASE WHEN logic, derived columns, COALESCE, conditional aggregation |
| `win_functions.py` | RANK, DENSE_RANK, ROW_NUMBER, cumulative SUM, LAG, LEAD |
| `sql_trans.py` | WHERE, GROUP BY, HAVING, ORDER BY in Pandas |
| `handling_missing_data.py` | fillna, dropna, interpolation strategies |
| `data_pipeline.py` | End-to-end ETL: Extract → Validate → Transform → Aggregate → Load → Report |

---

## 🧠 SQL → Pandas Translation Reference

| SQL | Pandas |
|---|---|
| `INNER JOIN` | `pd.merge(..., how='inner')` |
| `LEFT JOIN` | `pd.merge(..., how='left')` |
| `ANTI JOIN` | LEFT JOIN + filter `_merge == 'left_only'` |
| `CASE WHEN ... THEN` | `np.where()` / `np.select()` |
| `COALESCE(col, default)` | `df[col].fillna(default)` |
| `GROUP BY + HAVING` | `groupby().agg()` + boolean filter |
| `RANK() OVER (PARTITION BY)` | `groupby()[col].rank()` |
| `LAG() / LEAD()` | `groupby()[col].shift(1)` / `.shift(-1)` |
| `PIVOT` | `pivot_table()` |
| `UNPIVOT` | `melt()` |
| `SUM(CASE WHEN ...)` | `groupby().agg(lambda x: x[condition].sum())` |

---

## 🔄 data_pipeline.py — End-to-End ETL

Simulates a production-style data pipeline with 6 stages:

```
EXTRACT → VALIDATE → TRANSFORM → AGGREGATE → LOAD → REPORT
```

Features:
- Data quality checks (nulls, duplicates, referential integrity, valid values)
- Data cleaning and null imputation
- Derived columns using CASE WHEN patterns
- Multi-table joins (orders + customers + products)
- Business metrics: monthly revenue, segment analysis, product performance
- CSV output to `pipeline_output/` directory
- Structured logging with timestamps

---

## 🛠 Tech Stack

- Python 3.x
- Pandas
- NumPy

---

## 🚀 How to Run

```bash
# Install dependencies
pip install pandas numpy

# Run any script directly
python joins_and_merges.py
python pivot_and_reshape.py
python case_when_derived.py
python data_pipeline.py
```

No external datasets required — all scripts use hardcoded sample data and run immediately.

---

