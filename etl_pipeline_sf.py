"""
snowflake_elt_pipeline.py
--------------------------
Simulates a production-style ELT pipeline targeting Snowflake.

ELT stages covered:
    1. EXTRACT   — Pull raw data from a simulated source system (API/flat file)
    2. LOAD      — Load raw records into a Snowflake RAW / landing schema (simulated)
    3. TRANSFORM — Apply business logic inside Snowflake using SQL-based transformations
    4. VALIDATE  — Run post-transform data quality checks
    5. AUDIT     — Write pipeline run metadata to an audit log table

Design follows medallion architecture:
    RAW (landing) → STAGING (cleaned) → ANALYTICS (business-ready)

Mirrors patterns used in production Snowflake pipelines at scale,
including idempotent loads, merge/upsert logic, and audit logging.
"""

import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

PIPELINE_NAME    = "snowflake_elt_pipeline"
PIPELINE_VERSION = "1.0.0"
RUN_ID           = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATED SNOWFLAKE CONNECTION
# ═══════════════════════════════════════════════════════════════════════════════

class SnowflakeConnection:
    """
    Simulates a Snowflake connector session.

    In production this would use the snowflake-connector-python library:
        import snowflake.connector
        conn = snowflake.connector.connect(
            user=os.environ["SF_USER"],
            password=os.environ["SF_PASSWORD"],
            account=os.environ["SF_ACCOUNT"],
            warehouse="COMPUTE_WH",
            database="ANALYTICS_DB",
            schema="RAW",
            role="TRANSFORMER_ROLE"
        )
    """

    def __init__(self, account: str, database: str, warehouse: str, role: str):
        self.account   = account
        self.database  = database
        self.warehouse = warehouse
        self.role      = role
        self._store    = {}   # in-memory table simulation
        logger.info(f"  [Snowflake] Connected → account={account} | db={database} | wh={warehouse}")

    def execute(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Simulate executing a SQL statement and returning results."""
        logger.info(f"  [Snowflake] EXECUTE: {sql[:120].strip()}{'...' if len(sql) > 120 else ''}")
        return pd.DataFrame()

    def write_table(self, schema: str, table: str, df: pd.DataFrame, mode: str = "append") -> int:
        """Simulate writing a DataFrame to a Snowflake table."""
        key = f"{schema}.{table}"
        if mode == "overwrite" or key not in self._store:
            self._store[key] = df.copy()
        else:
            self._store[key] = pd.concat([self._store[key], df], ignore_index=True)
        logger.info(f"  [Snowflake] WRITE → {key} | rows={len(df)} | mode={mode}")
        return len(df)

    def read_table(self, schema: str, table: str) -> pd.DataFrame:
        """Simulate reading a Snowflake table into a DataFrame."""
        key = f"{schema}.{table}"
        df = self._store.get(key, pd.DataFrame())
        logger.info(f"  [Snowflake] READ  ← {key} | rows={len(df)}")
        return df

    def merge(self, schema: str, table: str, df: pd.DataFrame, merge_key: str) -> dict:
        """
        Simulate a Snowflake MERGE (upsert) operation.

        In production this generates:
            MERGE INTO target USING source
            ON target.key = source.key
            WHEN MATCHED THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT ...
        """
        key = f"{schema}.{table}"
        existing = self._store.get(key, pd.DataFrame())

        if existing.empty:
            self._store[key] = df.copy()
            return {"inserted": len(df), "updated": 0}

        inserted, updated = 0, 0
        existing_keys = set(existing[merge_key].astype(str))

        new_rows    = df[~df[merge_key].astype(str).isin(existing_keys)]
        update_rows = df[df[merge_key].astype(str).isin(existing_keys)]

        existing.set_index(merge_key, inplace=True)
        for _, row in update_rows.iterrows():
            existing.loc[row[merge_key]] = row.drop(merge_key)
            updated += 1

        existing.reset_index(inplace=True)
        self._store[key] = pd.concat([existing, new_rows], ignore_index=True)
        inserted = len(new_rows)

        logger.info(f"  [Snowflake] MERGE → {key} | inserted={inserted} | updated={updated}")
        return {"inserted": inserted, "updated": updated}


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: EXTRACT
# ═══════════════════════════════════════════════════════════════════════════════

def extract(batch_date: str) -> dict:
    """
    Simulate extracting raw data from upstream source systems.
    In production: API calls, JDBC connections, S3 file reads, Kafka consumers.

    Args:
        batch_date: The processing date in YYYY-MM-DD format.

    Returns:
        Dict of raw DataFrames keyed by entity name.
    """
    logger.info(f"STAGE 1: EXTRACT — batch_date={batch_date}")

    np.random.seed(42)
    n_orders = 50

    orders = pd.DataFrame({
        "order_id":       range(5001, 5001 + n_orders),
        "customer_id":    np.random.randint(1, 21, n_orders),
        "product_id":     np.random.randint(101, 121, n_orders),
        "quantity":       np.random.randint(1, 10, n_orders),
        "unit_price":     np.round(np.random.uniform(9.99, 299.99, n_orders), 2),
        "status_cd":      np.random.choice(["01", "02", "03"], n_orders, p=[0.3, 0.6, 0.1]),
        "order_ts":       pd.date_range(start=batch_date, periods=n_orders, freq="30min"),
        "source_system":  "OMS_API",
    })

    # Inject intentional nulls and a duplicate to test validation
    orders.loc[3, "unit_price"]  = None
    orders.loc[7, "customer_id"] = None
    orders = pd.concat([orders, orders.iloc[[0]]], ignore_index=True)  # duplicate row

    customers = pd.DataFrame({
        "customer_id":  range(1, 21),
        "first_name":   [f"First{i}"  for i in range(1, 21)],
        "last_name":    [f"Last{i}"   for i in range(1, 21)],
        "email":        [f"user{i}@example.com" for i in range(1, 21)],
        "region":       np.random.choice(["WEST", "EAST", "CENTRAL", "SOUTH"], 20),
        "customer_tier": np.random.choice(["STANDARD", "PREMIUM", "ELITE"], 20, p=[0.6, 0.3, 0.1]),
    })

    logger.info(f"  Extracted: orders={len(orders)} rows | customers={len(customers)} rows")
    return {"orders": orders, "customers": customers}


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: LOAD (RAW layer)
# ═══════════════════════════════════════════════════════════════════════════════

def load_raw(conn: SnowflakeConnection, raw_data: dict, batch_date: str) -> dict:
    """
    Load extracted data as-is into the RAW schema in Snowflake.
    No transformations applied — preserves source fidelity for replayability.

    Adds pipeline metadata columns before landing.

    Args:
        conn:       Snowflake connection instance.
        raw_data:   Dict of raw DataFrames from extract().
        batch_date: Processing batch date.

    Returns:
        Dict with row counts loaded per entity.
    """
    logger.info("STAGE 2: LOAD — writing to RAW schema")
    counts = {}

    for entity, df in raw_data.items():
        df = df.copy()
        df["_batch_date"]    = batch_date
        df["_loaded_at"]     = datetime.now().isoformat()
        df["_pipeline_name"] = PIPELINE_NAME
        df["_run_id"]        = RUN_ID

        rows = conn.write_table(schema="RAW", table=entity.upper(), df=df, mode="append")
        counts[entity] = rows

    logger.info(f"  RAW load complete: {counts}")
    return counts


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3: TRANSFORM (STAGING → ANALYTICS layers)
# ═══════════════════════════════════════════════════════════════════════════════

STATUS_MAP = {"01": "In Progress", "02": "Completed", "03": "Cancelled"}

def transform(conn: SnowflakeConnection, batch_date: str) -> dict:
    """
    Apply ELT transformations inside Snowflake (simulated with pandas).
    Reads from RAW, cleans and enriches, writes to STAGING then ANALYTICS.

    Transformations applied:
        - Deduplication on primary key
        - Null imputation for recoverable fields
        - Status code decoding
        - Derived columns: total_price, full_name, revenue_tier
        - Customer-order join for analytics layer

    Args:
        conn:       Snowflake connection instance.
        batch_date: Processing batch date — used to filter current batch.

    Returns:
        Dict of transformed DataFrames written to each layer.
    """
    logger.info("STAGE 3: TRANSFORM — RAW → STAGING → ANALYTICS")

    # --- Read from RAW ---
    raw_orders    = conn.read_table("RAW", "ORDERS")
    raw_customers = conn.read_table("RAW", "CUSTOMERS")

    raw_orders    = raw_orders[raw_orders["_batch_date"] == batch_date]

    # ── Staging: Orders ──────────────────────────────────────────────────────
    logger.info("  Transforming: stg_orders")

    stg_orders = raw_orders.copy()

    # Deduplication — keep first occurrence per order_id
    before = len(stg_orders)
    stg_orders = stg_orders.drop_duplicates(subset=["order_id"], keep="first")
    logger.info(f"    Deduplication: removed {before - len(stg_orders)} duplicate(s)")

    # Drop rows with unrecoverable nulls
    stg_orders = stg_orders.dropna(subset=["customer_id"])

    # Impute unit_price nulls with product median (simulated)
    median_price = stg_orders["unit_price"].median()
    null_price_count = stg_orders["unit_price"].isna().sum()
    stg_orders["unit_price"] = stg_orders["unit_price"].fillna(median_price)
    if null_price_count:
        logger.info(f"    Imputed {null_price_count} null unit_price(s) with median={median_price:.2f}")

    # Derived columns
    stg_orders["total_price"]  = (stg_orders["quantity"] * stg_orders["unit_price"]).round(2)
    stg_orders["status_desc"]  = stg_orders["status_cd"].map(STATUS_MAP).fillna("Unknown")
    stg_orders["order_date"]   = pd.to_datetime(stg_orders["order_ts"]).dt.date
    stg_orders["order_hour"]   = pd.to_datetime(stg_orders["order_ts"]).dt.hour
    stg_orders["_transformed_at"] = datetime.now().isoformat()

    conn.write_table("STAGING", "STG_ORDERS", stg_orders, mode="overwrite")

    # ── Staging: Customers ────────────────────────────────────────────────────
    logger.info("  Transforming: stg_customers")

    stg_customers = raw_customers.copy()
    stg_customers["full_name"] = stg_customers["first_name"] + " " + stg_customers["last_name"]
    stg_customers["_transformed_at"] = datetime.now().isoformat()

    conn.write_table("STAGING", "STG_CUSTOMERS", stg_customers, mode="overwrite")

    # ── Analytics: Orders Fact ────────────────────────────────────────────────
    logger.info("  Building: analytics_orders_fact")

    analytics_orders = stg_orders.merge(
        stg_customers[["customer_id", "full_name", "region", "customer_tier"]],
        on="customer_id",
        how="left"
    )

    analytics_orders["revenue_tier"] = pd.cut(
        analytics_orders["total_price"],
        bins=[0, 50, 200, 500, float("inf")],
        labels=["LOW", "MEDIUM", "HIGH", "PREMIUM"]
    ).astype(str)

    merge_result = conn.merge(
        schema="ANALYTICS",
        table="ORDERS_FACT",
        df=analytics_orders,
        merge_key="order_id"
    )

    # ── Analytics: Customer Revenue Summary ───────────────────────────────────
    logger.info("  Building: analytics_customer_revenue")

    customer_revenue = (
        analytics_orders
        .groupby(["customer_id", "full_name", "region", "customer_tier"], as_index=False)
        .agg(
            order_count   = ("order_id",    "count"),
            total_revenue = ("total_price", "sum"),
            avg_order_val = ("total_price", "mean"),
            first_order   = ("order_date",  "min"),
            last_order    = ("order_date",  "max"),
        )
    )
    customer_revenue["total_revenue"] = customer_revenue["total_revenue"].round(2)
    customer_revenue["avg_order_val"] = customer_revenue["avg_order_val"].round(2)

    conn.write_table("ANALYTICS", "CUSTOMER_REVENUE", customer_revenue, mode="overwrite")

    logger.info("  Transform complete.")
    return {
        "stg_orders":        stg_orders,
        "stg_customers":     stg_customers,
        "analytics_orders":  analytics_orders,
        "customer_revenue":  customer_revenue,
        "merge_result":      merge_result
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 4: VALIDATE
# ═══════════════════════════════════════════════════════════════════════════════

def validate(transform_output: dict) -> dict:
    """
    Run post-transform data quality checks on the analytics layer.

    Checks:
        - No null values in critical columns
        - No negative total_price values
        - All status_cd values are in the expected set
        - Customer revenue totals are non-negative
        - Row count sanity (analytics >= 0 rows)

    Args:
        transform_output: Dict of DataFrames from transform().

    Returns:
        Dict with per-check results and overall pass/fail status.
    """
    logger.info("STAGE 4: VALIDATE — post-transform quality checks")

    orders   = transform_output["analytics_orders"]
    revenue  = transform_output["customer_revenue"]
    checks   = []

    def record(check_name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        checks.append({"check": check_name, "status": status, "detail": detail})
        logger.info(f"  [{status}] {check_name}{' — ' + detail if detail else ''}")

    # Null checks on critical columns
    for col in ["order_id", "customer_id", "total_price", "status_desc"]:
        nulls = orders[col].isna().sum()
        record(f"no_nulls:{col}", nulls == 0, f"{nulls} null(s) found" if nulls else "")

    # No negative prices
    neg = (orders["total_price"] < 0).sum()
    record("no_negative_total_price", neg == 0, f"{neg} negative value(s)" if neg else "")

    # Valid status codes
    valid_statuses = set(STATUS_MAP.values()) | {"Unknown"}
    invalid = (~orders["status_desc"].isin(valid_statuses)).sum()
    record("valid_status_desc", invalid == 0, f"{invalid} invalid status(es)" if invalid else "")

    # Non-negative revenue at customer level
    neg_rev = (revenue["total_revenue"] < 0).sum()
    record("non_negative_customer_revenue", neg_rev == 0, f"{neg_rev} negative revenue(s)" if neg_rev else "")

    # Row count sanity
    record("analytics_has_rows", len(orders) > 0, f"{len(orders)} rows")

    results_df  = pd.DataFrame(checks)
    fail_count  = (results_df["status"] == "FAIL").sum()
    overall     = "PASS" if fail_count == 0 else "FAIL"

    logger.info(f"  Validation complete: {len(checks) - fail_count}/{len(checks)} checks passed | Overall: {overall}")
    return {"checks": results_df, "overall": overall, "fail_count": int(fail_count)}


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 5: AUDIT LOG
# ═══════════════════════════════════════════════════════════════════════════════

def write_audit_log(
    conn:             SnowflakeConnection,
    batch_date:       str,
    raw_counts:       dict,
    transform_output: dict,
    validation:       dict,
    start_time:       datetime
) -> None:
    """
    Write pipeline run metadata to an audit log table in Snowflake.
    Supports observability, alerting, and lineage tracking.

    Args:
        conn:             Snowflake connection instance.
        batch_date:       Processing batch date.
        raw_counts:       Row counts from load_raw().
        transform_output: Output dict from transform().
        validation:       Output dict from validate().
        start_time:       Pipeline start timestamp.
    """
    logger.info("STAGE 5: AUDIT LOG — writing run metadata")

    end_time     = datetime.now()
    duration_sec = round((end_time - start_time).total_seconds(), 2)

    audit_record = pd.DataFrame([{
        "run_id":               RUN_ID,
        "pipeline_name":        PIPELINE_NAME,
        "pipeline_version":     PIPELINE_VERSION,
        "batch_date":           batch_date,
        "status":               validation["overall"],
        "raw_orders_loaded":    raw_counts.get("orders", 0),
        "raw_customers_loaded": raw_counts.get("customers", 0),
        "analytics_rows":       len(transform_output["analytics_orders"]),
        "merge_inserted":       transform_output["merge_result"].get("inserted", 0),
        "merge_updated":        transform_output["merge_result"].get("updated", 0),
        "validation_failures":  validation["fail_count"],
        "start_time":           start_time.isoformat(),
        "end_time":             end_time.isoformat(),
        "duration_seconds":     duration_sec,
    }])

    conn.write_table("AUDIT", "PIPELINE_RUN_LOG", audit_record, mode="append")
    logger.info(f"  Audit record written | run_id={RUN_ID} | duration={duration_sec}s | status={validation['overall']}")


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(transform_output: dict, validation: dict) -> None:
    """Print a readable pipeline run summary to stdout."""

    orders  = transform_output["analytics_orders"]
    revenue = transform_output["customer_revenue"]

    print("\n" + "═" * 65)
    print("  SNOWFLAKE ELT PIPELINE — RUN SUMMARY")
    print(f"  Run ID    : {RUN_ID}")
    print(f"  Status    : {validation['overall']}")
    print("═" * 65)

    print("\n[ ORDERS PROCESSED ]")
    print(f"  Total orders    : {len(orders)}")
    print(f"  Total revenue   : ${orders['total_price'].sum():,.2f}")
    print(f"  Avg order value : ${orders['total_price'].mean():,.2f}")

    print("\n  Revenue by status:")
    status_summary = orders.groupby("status_desc")["total_price"].agg(["count", "sum"])
    status_summary.columns = ["orders", "revenue"]
    print(status_summary.to_string())

    print("\n  Revenue tier distribution:")
    print(orders["revenue_tier"].value_counts().to_string())

    print("\n[ TOP 5 CUSTOMERS BY REVENUE ]")
    top5 = revenue.nlargest(5, "total_revenue")[["full_name", "region", "customer_tier", "order_count", "total_revenue"]]
    print(top5.to_string(index=False))

    print("\n[ VALIDATION RESULTS ]")
    print(validation["checks"].to_string(index=False))

    print("\n" + "═" * 65 + "\n")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    start_time = datetime.now()
    batch_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # yesterday's batch

    logger.info(f"{'═' * 55}")
    logger.info(f"  {PIPELINE_NAME} v{PIPELINE_VERSION} | run_id={RUN_ID}")
    logger.info(f"  batch_date={batch_date}")
    logger.info(f"{'═' * 55}")

    # Initialize simulated Snowflake connection
    conn = SnowflakeConnection(
        account   = "myorg-prod.snowflakecomputing.com",
        database  = "ANALYTICS_DB",
        warehouse = "COMPUTE_WH",
        role      = "TRANSFORMER_ROLE"
    )

    # Run pipeline stages
    raw_data         = extract(batch_date)
    raw_counts       = load_raw(conn, raw_data, batch_date)
    transform_output = transform(conn, batch_date)
    validation       = validate(transform_output)
    write_audit_log(conn, batch_date, raw_counts, transform_output, validation, start_time)

    # Print summary
    print_summary(transform_output, validation)
