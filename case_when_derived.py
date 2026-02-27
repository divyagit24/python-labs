"""
case_when_derived.py
--------------------
Demonstrates CASE WHEN logic and derived column patterns in Pandas.

SQL Equivalents covered:
    CASE WHEN ... THEN ... END   → np.where() / np.select() / apply()
    Derived columns              → assign()
    Conditional aggregation      → groupby + np.where
    COALESCE                     → fillna()
    NULLIF                       → mask()
    IIF / IF                     → np.where()
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Sample Data ───────────────────────────────────────────────────────────────

orders = pd.DataFrame({
    "order_id":      [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
    "customer_id":   [1,    2,    3,    4,    5,    6,    7,    8],
    "amount":        [250,  1800, 75,   320,  950,  50,   4200, 180],
    "status":        ["delivered", "delivered", "cancelled", "shipped",
                      "delivered", "cancelled", "delivered", "shipped"],
    "days_to_ship":  [3,    5,    None, 2,    7,    None, 1,    4],
    "discount_pct":  [10,   0,    5,    15,   0,    20,   5,    None],
    "region":        ["North", "South", "East", "West", "North", "East", "South", None]
})


# ── 1. Simple CASE WHEN — np.where() ─────────────────────────────────────────
# SQL: CASE WHEN amount > 500 THEN 'High' ELSE 'Low' END AS order_tier
def case_when_simple():
    logging.info("CASE WHEN (simple) — order tier based on amount")
    df = orders.copy()

    df["order_tier"] = np.where(df["amount"] > 500, "High Value", "Standard")

    print("\n--- Simple CASE WHEN: Order Tier ---")
    print(df[["order_id", "amount", "order_tier"]])
    return df


# ── 2. Multi-condition CASE WHEN — np.select() ───────────────────────────────
# SQL: CASE
#        WHEN amount >= 1000 THEN 'Premium'
#        WHEN amount >= 500  THEN 'Standard'
#        WHEN amount >= 100  THEN 'Basic'
#        ELSE 'Micro'
#      END AS order_segment
def case_when_multi():
    logging.info("CASE WHEN (multi-condition) — order segment")
    df = orders.copy()

    conditions = [
        df["amount"] >= 1000,
        df["amount"] >= 500,
        df["amount"] >= 100,
    ]
    choices = ["Premium", "Standard", "Basic"]

    df["order_segment"] = np.select(conditions, choices, default="Micro")

    print("\n--- Multi-Condition CASE WHEN: Order Segment ---")
    print(df[["order_id", "amount", "order_segment"]])
    print("\nSegment distribution:")
    print(df["order_segment"].value_counts())
    return df


# ── 3. CASE WHEN with multiple columns — apply() ─────────────────────────────
# SQL: CASE
#        WHEN status = 'delivered' AND days_to_ship <= 3 THEN 'Fast'
#        WHEN status = 'delivered' AND days_to_ship > 3  THEN 'Slow'
#        WHEN status = 'shipped'                         THEN 'In Transit'
#        ELSE 'Not Shipped'
#      END AS fulfillment_flag
def case_when_multi_column():
    logging.info("CASE WHEN (multi-column logic) — fulfillment flag")
    df = orders.copy()

    def fulfillment_flag(row):
        if row["status"] == "delivered" and row["days_to_ship"] is not None:
            return "Fast Delivery" if row["days_to_ship"] <= 3 else "Slow Delivery"
        elif row["status"] == "shipped":
            return "In Transit"
        elif row["status"] == "cancelled":
            return "Cancelled"
        return "Unknown"

    df["fulfillment_flag"] = df.apply(fulfillment_flag, axis=1)

    print("\n--- Multi-Column CASE WHEN: Fulfillment Flag ---")
    print(df[["order_id", "status", "days_to_ship", "fulfillment_flag"]])
    return df


# ── 4. Derived Columns with assign() ─────────────────────────────────────────
# SQL: SELECT *, amount * (1 - discount_pct/100) AS final_amount,
#             amount * 0.1 AS tax_amount
def derived_columns():
    logging.info("Derived columns — final amount after discount and tax")
    df = orders.copy()

    df = df.assign(
        discount_pct_clean = df["discount_pct"].fillna(0),
        final_amount       = lambda x: (x["amount"] * (1 - x["discount_pct"].fillna(0) / 100)).round(2),
        tax_amount         = lambda x: (x["amount"] * 0.1).round(2),
        profit_estimate    = lambda x: (x["amount"] * 0.25).round(2)
    )

    print("\n--- Derived Columns: Final Amount, Tax, Profit ---")
    print(df[["order_id", "amount", "discount_pct_clean", "final_amount", "tax_amount", "profit_estimate"]])
    return df


# ── 5. COALESCE — fillna() ────────────────────────────────────────────────────
# SQL: COALESCE(discount_pct, 0), COALESCE(region, 'Unknown')
def coalesce_demo():
    logging.info("COALESCE — fill nulls with default values")
    df = orders.copy()

    df["discount_pct"]  = df["discount_pct"].fillna(0)
    df["days_to_ship"]  = df["days_to_ship"].fillna(df["days_to_ship"].median())
    df["region"]        = df["region"].fillna("Unknown")

    print("\n--- COALESCE: Null values replaced ---")
    print(df[["order_id", "discount_pct", "days_to_ship", "region"]])
    print(f"\nNull counts after coalesce:\n{df[['discount_pct','days_to_ship','region']].isna().sum()}")
    return df


# ── 6. Conditional Aggregation ────────────────────────────────────────────────
# SQL: SELECT status,
#             SUM(CASE WHEN amount > 500 THEN amount ELSE 0 END) AS high_value_revenue,
#             COUNT(CASE WHEN amount > 500 THEN 1 END)           AS high_value_count
#      FROM orders GROUP BY status
def conditional_aggregation():
    logging.info("Conditional aggregation — revenue split by value tier")
    df = orders.copy()

    result = df.groupby("status").agg(
        total_revenue      = ("amount", "sum"),
        order_count        = ("order_id", "count"),
        high_value_revenue = ("amount", lambda x: x[x > 500].sum()),
        high_value_count   = ("amount", lambda x: (x > 500).sum()),
        avg_amount         = ("amount", "mean")
    ).round(2).reset_index()

    print("\n--- Conditional Aggregation: Revenue by Status ---")
    print(result)
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("CASE WHEN & DERIVED COLUMNS IN PANDAS")
    print("=" * 60)

    case_when_simple()
    case_when_multi()
    case_when_multi_column()
    derived_columns()
    coalesce_demo()
    conditional_aggregation()

    print("\n" + "=" * 60)
    print("All CASE WHEN demos completed.")
    print("=" * 60)
