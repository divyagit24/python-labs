"""
pivot_and_reshape.py
--------------------
Demonstrates reshaping operations in Pandas.

SQL Equivalents covered:
    PIVOT        → pivot_table()
    UNPIVOT      → melt()
    CROSSTAB     → pd.crosstab()
    STACK        → stack() / unstack()
    TRANSPOSE    → DataFrame.T
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Sample Data ───────────────────────────────────────────────────────────────

sales = pd.DataFrame({
    "month":      ["Jan", "Jan", "Jan", "Feb", "Feb", "Feb", "Mar", "Mar", "Mar"],
    "region":     ["North", "South", "East", "North", "South", "East", "North", "South", "East"],
    "category":   ["Electronics", "Electronics", "Clothing", "Electronics", "Clothing", "Electronics", "Clothing", "Electronics", "Clothing"],
    "revenue":    [12000, 9500, 7800, 13500, 8200, 8900, 11000, 10200, 9100],
    "units_sold": [45, 38, 62, 51, 33, 40, 42, 44, 71]
})

# Wide format — for melt demo
wide_sales = pd.DataFrame({
    "region":    ["North", "South", "East"],
    "Jan":       [12000, 9500, 7800],
    "Feb":       [13500, 8200, 8900],
    "Mar":       [11000, 10200, 9100]
})


# ── 1. PIVOT TABLE ────────────────────────────────────────────────────────────
# SQL equivalent: SELECT region, SUM(CASE WHEN month='Jan' THEN revenue END) as Jan ...
def pivot_table_demo():
    logging.info("PIVOT TABLE — revenue by region and month")
    result = sales.pivot_table(
        values="revenue",
        index="region",
        columns="month",
        aggfunc="sum",
        fill_value=0,
        margins=True,        # adds row/column totals
        margins_name="Total"
    )
    print("\n--- PIVOT TABLE: Revenue by Region and Month ---")
    print(result)
    return result


# ── 2. PIVOT TABLE with multiple aggregations ─────────────────────────────────
def pivot_multi_agg_demo():
    logging.info("PIVOT TABLE — multiple aggregations (sum + mean)")
    result = sales.pivot_table(
        values=["revenue", "units_sold"],
        index="region",
        columns="category",
        aggfunc={"revenue": "sum", "units_sold": "mean"},
        fill_value=0
    )
    print("\n--- PIVOT TABLE: Revenue (sum) and Units Sold (mean) by Region & Category ---")
    print(result.round(1))
    return result


# ── 3. MELT (UNPIVOT) ─────────────────────────────────────────────────────────
# SQL equivalent: UNPIVOT — converts wide format to long format
def melt_demo():
    logging.info("MELT — wide format to long format (unpivot)")
    result = wide_sales.melt(
        id_vars=["region"],
        value_vars=["Jan", "Feb", "Mar"],
        var_name="month",
        value_name="revenue"
    )
    print("\n--- MELT: Wide → Long Format ---")
    print(f"Wide shape: {wide_sales.shape} → Long shape: {result.shape}")
    print(result.sort_values(["region", "month"]))
    return result


# ── 4. CROSSTAB ───────────────────────────────────────────────────────────────
# Frequency table — how many records per region per category
def crosstab_demo():
    logging.info("CROSSTAB — frequency count of region vs category")
    result = pd.crosstab(
        index=sales["region"],
        columns=sales["category"],
        values=sales["revenue"],
        aggfunc="sum",
        margins=True,
        margins_name="Total"
    )
    print("\n--- CROSSTAB: Total Revenue by Region and Category ---")
    print(result)
    return result


# ── 5. STACK & UNSTACK ────────────────────────────────────────────────────────
def stack_unstack_demo():
    logging.info("STACK / UNSTACK — rotating column levels to row index")

    pivot = sales.pivot_table(
        values="revenue",
        index="region",
        columns="month",
        aggfunc="sum"
    )

    print("\n--- Original Pivot (wide) ---")
    print(pivot)

    stacked = pivot.stack()
    print("\n--- STACK: Pivot → Long Series (region + month as index) ---")
    print(stacked)

    unstacked = stacked.unstack(level=0)
    print("\n--- UNSTACK: Back to wide, now month as index ---")
    print(unstacked)

    return stacked


# ── 6. TRANSPOSE ──────────────────────────────────────────────────────────────
def transpose_demo():
    logging.info("TRANSPOSE — flip rows and columns")
    summary = sales.groupby("region")[["revenue", "units_sold"]].sum()
    print("\n--- Original Summary ---")
    print(summary)
    print("\n--- TRANSPOSED ---")
    print(summary.T)
    return summary.T


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("PIVOT & RESHAPE OPERATIONS IN PANDAS")
    print("=" * 60)

    pivot_table_demo()
    pivot_multi_agg_demo()
    melt_demo()
    crosstab_demo()
    stack_unstack_demo()
    transpose_demo()

    print("\n" + "=" * 60)
    print("All reshape demos completed.")
    print("=" * 60)
