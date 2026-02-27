"""
data_pipeline.py
----------------
An end-to-end mini data pipeline simulating a real-world ETL workflow.

Pipeline stages:
    1. EXTRACT   — load raw data (simulated from hardcoded source)
    2. VALIDATE  — data quality checks before transformation
    3. TRANSFORM — clean, enrich, and reshape data
    4. AGGREGATE — compute business metrics
    5. LOAD      — write output to CSV (simulates loading to a target)
    6. REPORT    — print pipeline summary

Mirrors production patterns used in Snowflake/Databricks pipelines.
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = "pipeline_output"


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: EXTRACT
# ═══════════════════════════════════════════════════════════════════════════════

def extract() -> dict:
    """Simulate extracting raw data from multiple source systems."""
    logger.info("STAGE 1: EXTRACT — loading raw data")

    orders = pd.DataFrame({
        "order_id":    [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
        "customer_id": [1,    2,    3,    4,    5,    2,    3,    6,    7,    1],
        "product_id":  [101,  102,  101,  103,  102,  104,  103,  101,  102,  104],
        "amount":      [250,  180,  320,  None, 950,  75,   410,  220,  None, 600],
        "status":      ["delivered", "shipped", "delivered", "cancelled",
                        "delivered", "delivered", "shipped", "delivered",
                        "cancelled", "delivered"],
        "order_date":  ["2024-01-05", "2024-01-12", "2024-01-18", "2024-01-20",
                        "2024-02-03", "2024-02-14", "2024-02-19", "2024-03-01",
                        "2024-03-10", "2024-03-22"],
        "days_to_ship": [3, 5, 2, None, 4, 1, 6, 3, None, 2]
    })

    customers = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5, 6, 7],
        "name":        ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace"],
        "city":        ["New York", "Los Angeles", "Chicago", "Houston",
                        "Phoenix", "Seattle", "Boston"],
        "segment":     ["Premium", "Standard", "Premium", "Standard",
                        "Basic", "Standard", "Premium"]
    })

    products = pd.DataFrame({
        "product_id":   [101, 102, 103, 104],
        "product_name": ["Laptop", "Phone", "Tablet", "Headphones"],
        "category":     ["Electronics", "Electronics", "Electronics", "Accessories"],
        "unit_cost":    [800, 500, 300, 150]
    })

    logger.info(f"  Extracted {len(orders)} orders, {len(customers)} customers, {len(products)} products")

    return {"orders": orders, "customers": customers, "products": products}


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: VALIDATE
# ═══════════════════════════════════════════════════════════════════════════════

def validate(data: dict) -> dict:
    """Run data quality checks and log issues."""
    logger.info("STAGE 2: VALIDATE — running data quality checks")

    orders    = data["orders"]
    customers = data["customers"]
    products  = data["products"]

    issues = []

    # Check 1: No duplicate order IDs
    dupes = orders["order_id"].duplicated().sum()
    if dupes > 0:
        issues.append(f"Duplicate order_ids: {dupes}")
    else:
        logger.info("  ✅ No duplicate order IDs")

    # Check 2: Null amounts
    null_amounts = orders["amount"].isna().sum()
    logger.info(f"  {'✅' if null_amounts == 0 else '⚠️ '} Null amounts: {null_amounts} (will be imputed)")

    # Check 3: Valid order status
    valid_statuses = {"delivered", "shipped", "cancelled", "processing"}
    invalid_status = (~orders["status"].isin(valid_statuses)).sum()
    if invalid_status > 0:
        issues.append(f"Invalid status values: {invalid_status}")
    else:
        logger.info("  ✅ All order statuses are valid")

    # Check 4: Referential integrity — all order customer_ids exist in customers
    orphan_orders = (~orders["customer_id"].isin(customers["customer_id"])).sum()
    if orphan_orders > 0:
        issues.append(f"Orders with unknown customer_id: {orphan_orders}")
    else:
        logger.info("  ✅ All order customer IDs exist in customers table")

    # Check 5: Referential integrity — products
    orphan_products = (~orders["product_id"].isin(products["product_id"])).sum()
    if orphan_products > 0:
        issues.append(f"Orders with unknown product_id: {orphan_products}")
    else:
        logger.info("  ✅ All order product IDs exist in products table")

    # Check 6: Row counts
    logger.info(f"  ✅ Row counts — orders: {len(orders)}, customers: {len(customers)}, products: {len(products)}")

    if issues:
        logger.warning(f"  ⚠️  Validation issues found: {issues}")
    else:
        logger.info("  ✅ All validation checks passed")

    data["validation_issues"] = issues
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3: TRANSFORM
# ═══════════════════════════════════════════════════════════════════════════════

def transform(data: dict) -> dict:
    """Clean, enrich and reshape raw data into analytics-ready format."""
    logger.info("STAGE 3: TRANSFORM — cleaning and enriching data")

    orders    = data["orders"].copy()
    customers = data["customers"].copy()
    products  = data["products"].copy()

    # 3a. Clean orders
    # Impute null amounts with median
    median_amount = orders["amount"].median()
    orders["amount"] = orders["amount"].fillna(median_amount)
    logger.info(f"  Imputed {data['orders']['amount'].isna().sum()} null amounts with median: ${median_amount}")

    # Impute null days_to_ship with median
    orders["days_to_ship"] = orders["days_to_ship"].fillna(orders["days_to_ship"].median())

    # Parse order_date
    orders["order_date"] = pd.to_datetime(orders["order_date"])
    orders["order_month"] = orders["order_date"].dt.to_period("M").astype(str)
    orders["order_quarter"] = orders["order_date"].dt.to_period("Q").astype(str)

    # 3b. Derive columns
    # Order tier — CASE WHEN equivalent
    conditions = [orders["amount"] >= 500, orders["amount"] >= 200]
    choices    = ["Premium", "Standard"]
    orders["order_tier"] = np.select(conditions, choices, default="Basic")

    # Margin estimate
    orders["margin_pct"] = 0.30  # simplified flat margin
    orders["estimated_margin"] = (orders["amount"] * orders["margin_pct"]).round(2)

    # Fulfillment flag
    orders["fulfillment_flag"] = np.where(
        orders["status"] == "delivered",
        np.where(orders["days_to_ship"] <= 3, "Fast", "Standard"),
        orders["status"].str.title()
    )

    # 3c. Join all tables
    enriched = (
        orders
        .merge(customers, on="customer_id", how="left")
        .merge(products,  on="product_id",  how="left")
    )

    # 3d. Add revenue after cost
    enriched["gross_profit"] = (enriched["amount"] - enriched["unit_cost"]).clip(lower=0).round(2)

    logger.info(f"  Transformed dataset: {len(enriched)} rows, {len(enriched.columns)} columns")
    data["enriched"] = enriched
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4: AGGREGATE
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate(data: dict) -> dict:
    """Compute business metrics from enriched data."""
    logger.info("STAGE 4: AGGREGATE — computing business metrics")

    df = data["enriched"]

    # Metric 1: Monthly revenue summary
    monthly_revenue = (
        df[df["status"] == "delivered"]
        .groupby("order_month")
        .agg(
            total_orders   = ("order_id",       "count"),
            total_revenue  = ("amount",          "sum"),
            avg_order_value= ("amount",          "mean"),
            total_profit   = ("gross_profit",    "sum"),
            unique_customers=("customer_id",     "nunique")
        )
        .round(2)
        .reset_index()
    )

    # Metric 2: Revenue by customer segment
    segment_revenue = (
        df[df["status"] == "delivered"]
        .groupby("segment")
        .agg(
            order_count   = ("order_id",    "count"),
            total_revenue = ("amount",       "sum"),
            avg_revenue   = ("amount",       "mean"),
            total_profit  = ("gross_profit", "sum")
        )
        .round(2)
        .reset_index()
    )

    # Metric 3: Product performance
    product_performance = (
        df[df["status"] == "delivered"]
        .groupby(["product_name", "category"])
        .agg(
            units_sold    = ("order_id",    "count"),
            total_revenue = ("amount",       "sum"),
            avg_price     = ("amount",       "mean"),
            total_profit  = ("gross_profit", "sum")
        )
        .round(2)
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    # Metric 4: Fulfilment summary
    fulfillment_summary = (
        df.groupby("fulfillment_flag")
        .agg(order_count=("order_id", "count"))
        .reset_index()
    )

    logger.info(f"  Monthly revenue: {len(monthly_revenue)} months")
    logger.info(f"  Segment revenue: {len(segment_revenue)} segments")
    logger.info(f"  Product performance: {len(product_performance)} products")

    data["metrics"] = {
        "monthly_revenue":     monthly_revenue,
        "segment_revenue":     segment_revenue,
        "product_performance": product_performance,
        "fulfillment_summary": fulfillment_summary
    }
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 5: LOAD
# ═══════════════════════════════════════════════════════════════════════════════

def load(data: dict) -> dict:
    """Write outputs to CSV files — simulates loading to a data warehouse."""
    logger.info("STAGE 5: LOAD — writing outputs")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    outputs = {
        "enriched_orders":     data["enriched"],
        "monthly_revenue":     data["metrics"]["monthly_revenue"],
        "segment_revenue":     data["metrics"]["segment_revenue"],
        "product_performance": data["metrics"]["product_performance"],
        "fulfillment_summary": data["metrics"]["fulfillment_summary"]
    }

    for name, df in outputs.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"  Written: {path} ({len(df)} rows)")

    data["output_paths"] = list(outputs.keys())
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 6: REPORT
# ═══════════════════════════════════════════════════════════════════════════════

def report(data: dict):
    """Print a pipeline summary report."""
    logger.info("STAGE 6: REPORT — pipeline summary")

    metrics = data["metrics"]

    print("\n" + "=" * 60)
    print("  PIPELINE EXECUTION SUMMARY")
    print("=" * 60)

    print("\n📅 Monthly Revenue:")
    print(metrics["monthly_revenue"].to_string(index=False))

    print("\n👥 Revenue by Customer Segment:")
    print(metrics["segment_revenue"].to_string(index=False))

    print("\n📦 Product Performance:")
    print(metrics["product_performance"].to_string(index=False))

    print("\n🚚 Fulfillment Summary:")
    print(metrics["fulfillment_summary"].to_string(index=False))

    total_rev   = metrics["monthly_revenue"]["total_revenue"].sum()
    total_profit= metrics["monthly_revenue"]["total_profit"].sum()
    total_orders= metrics["monthly_revenue"]["total_orders"].sum()

    print("\n" + "─" * 60)
    print(f"  Total Orders Delivered : {int(total_orders)}")
    print(f"  Total Revenue          : ${total_rev:,.2f}")
    print(f"  Total Estimated Profit : ${total_profit:,.2f}")
    print(f"  Profit Margin          : {(total_profit/total_rev*100):.1f}%")
    print(f"  Validation Issues      : {len(data.get('validation_issues', []))}")
    print(f"  Output Files           : {len(data.get('output_paths', []))}")
    print("─" * 60)
    print(f"  Pipeline completed at  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — Run the full pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    logger.info("=" * 60)
    logger.info("STARTING DATA PIPELINE")
    logger.info("=" * 60)

    try:
        data = extract()
        data = validate(data)
        data = transform(data)
        data = aggregate(data)
        data = load(data)
        report(data)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY ✅")
    except Exception as e:
        logger.error(f"PIPELINE FAILED ❌ — {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
