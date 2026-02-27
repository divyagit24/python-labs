"""
joins_and_merges.py
-------------------
Demonstrates SQL-style JOIN operations translated into Pandas.

SQL Equivalents covered:
    INNER JOIN   → pd.merge(..., how='inner')
    LEFT JOIN    → pd.merge(..., how='left')
    RIGHT JOIN   → pd.merge(..., how='right')
    FULL OUTER   → pd.merge(..., how='outer')
    SELF JOIN    → merge a DataFrame with itself
    ANTI JOIN    → LEFT JOIN + filter where right side is null
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Sample Data ───────────────────────────────────────────────────────────────

orders = pd.DataFrame({
    "order_id":    [1001, 1002, 1003, 1004, 1005],
    "customer_id": [1,    2,    3,    4,    5],
    "product_id":  [101,  102,  103,  101,  104],
    "amount":      [250,  180,  320,  150,  410],
    "status":      ["delivered", "shipped", "delivered", "cancelled", "delivered"]
})

customers = pd.DataFrame({
    "customer_id": [1, 2, 3, 6, 7],
    "name":        ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "city":        ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
})

products = pd.DataFrame({
    "product_id":   [101, 102, 103, 105],
    "product_name": ["Laptop", "Phone", "Tablet", "Watch"],
    "category":     ["Electronics", "Electronics", "Electronics", "Accessories"],
    "cost":         [800, 500, 300, 200]
})


# ── 1. INNER JOIN ─────────────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o INNER JOIN customers c ON o.customer_id = c.customer_id
def inner_join_demo():
    logging.info("INNER JOIN — only matching rows from both tables")
    result = pd.merge(orders, customers, on="customer_id", how="inner")
    print("\n--- INNER JOIN: Orders with matching Customers ---")
    print(result[["order_id", "customer_id", "name", "city", "amount", "status"]])
    print(f"  Orders: {len(orders)} rows | Customers: {len(customers)} rows | Result: {len(result)} rows")
    return result


# ── 2. LEFT JOIN ──────────────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
def left_join_demo():
    logging.info("LEFT JOIN — all orders, NaN where no matching customer")
    result = pd.merge(orders, customers, on="customer_id", how="left")
    print("\n--- LEFT JOIN: All Orders + Customer info where available ---")
    print(result[["order_id", "customer_id", "name", "city", "amount"]])
    print(f"  Unmatched orders (no customer record): {result['name'].isna().sum()}")
    return result


# ── 3. RIGHT JOIN ─────────────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o RIGHT JOIN customers c ON o.customer_id = c.customer_id
def right_join_demo():
    logging.info("RIGHT JOIN — all customers, NaN where no matching order")
    result = pd.merge(orders, customers, on="customer_id", how="right")
    print("\n--- RIGHT JOIN: All Customers + Order info where available ---")
    print(result[["order_id", "customer_id", "name", "city", "amount"]])
    print(f"  Customers with no orders: {result['order_id'].isna().sum()}")
    return result


# ── 4. FULL OUTER JOIN ────────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.customer_id
def full_outer_join_demo():
    logging.info("FULL OUTER JOIN — all rows from both tables")
    result = pd.merge(orders, customers, on="customer_id", how="outer")
    print("\n--- FULL OUTER JOIN: All Orders and All Customers ---")
    print(result[["order_id", "customer_id", "name", "amount"]])
    return result


# ── 5. MULTI-TABLE JOIN ───────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o
#      JOIN customers c ON o.customer_id = c.customer_id
#      LEFT JOIN products p ON o.product_id = p.product_id
def multi_table_join_demo():
    logging.info("MULTI-TABLE JOIN — orders + customers + products")
    result = (
        pd.merge(orders, customers, on="customer_id", how="inner")
          .merge(products, on="product_id", how="left")
    )
    print("\n--- MULTI-TABLE JOIN: Orders + Customers + Products ---")
    print(result[["order_id", "name", "product_name", "category", "amount", "status"]])
    return result


# ── 6. ANTI JOIN ──────────────────────────────────────────────────────────────
# SQL: SELECT * FROM orders o
#      LEFT JOIN customers c ON o.customer_id = c.customer_id
#      WHERE c.customer_id IS NULL
def anti_join_demo():
    logging.info("ANTI JOIN — orders with NO matching customer record")
    merged = pd.merge(orders, customers, on="customer_id", how="left", indicator=True)
    result = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    print("\n--- ANTI JOIN: Orders with no Customer record ---")
    print(result[["order_id", "customer_id", "amount", "status"]])
    print(f"  {len(result)} orders have no matching customer")
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("SQL JOIN OPERATIONS IN PANDAS")
    print("=" * 60)

    inner_join_demo()
    left_join_demo()
    right_join_demo()
    full_outer_join_demo()
    multi_table_join_demo()
    anti_join_demo()

    print("\n" + "=" * 60)
    print("All join demos completed.")
    print("=" * 60)
