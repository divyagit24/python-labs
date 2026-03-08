"""
time_series_analysis.py
-----------------------
Demonstrates time series analysis patterns in Pandas.

SQL Equivalents covered:
    DATE_TRUNC / GROUP BY month     → resample() / dt.to_period()
    LAG for period-over-period      → shift()
    Rolling averages                → rolling().mean()
    Cumulative metrics              → cumsum()
    Month-over-Month growth         → pct_change()
    Year-over-Year comparison       → merge on month
    Moving averages                 → rolling windows
    Seasonality detection           → groupby month/weekday
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Sample Data ───────────────────────────────────────────────────────────────
# Simulate 2 years of daily sales data

np.random.seed(42)

date_range = pd.date_range(start="2022-01-01", end="2023-12-31", freq="D")

# Simulate realistic sales with seasonality and trend
trend     = np.linspace(1000, 1500, len(date_range))
seasonal  = 200 * np.sin(2 * np.pi * np.arange(len(date_range)) / 365)
noise     = np.random.normal(0, 100, len(date_range))
sales     = (trend + seasonal + noise).clip(min=100).round(2)
orders    = (sales / np.random.uniform(40, 60, len(date_range))).astype(int).clip(min=1)

df = pd.DataFrame({
    "date":     date_range,
    "revenue":  sales,
    "orders":   orders,
    "region":   np.random.choice(["North", "South", "East", "West"], len(date_range))
})

df["year"]    = df["date"].dt.year
df["month"]   = df["date"].dt.month
df["weekday"] = df["date"].dt.day_name()
df["quarter"] = df["date"].dt.to_period("Q").astype(str)


# ── 1. Monthly Revenue Summary ────────────────────────────────────────────────
# SQL: SELECT DATE_TRUNC('month', date), SUM(revenue), COUNT(orders)
#      FROM sales GROUP BY 1 ORDER BY 1
def monthly_summary():
    logging.info("Monthly revenue summary")

    monthly = (
        df.groupby(df["date"].dt.to_period("M"))
        .agg(
            total_revenue = ("revenue", "sum"),
            total_orders  = ("orders",  "sum"),
            avg_order_value=("revenue", "mean"),
            days_active   = ("date",    "count")
        )
        .round(2)
        .reset_index()
    )
    monthly["date"] = monthly["date"].astype(str)

    print("\n--- Monthly Revenue Summary (first 6 months) ---")
    print(monthly.head(6).to_string(index=False))
    return monthly


# ── 2. Month-over-Month Growth ────────────────────────────────────────────────
# SQL: LAG(revenue) OVER (ORDER BY month) to compute growth %
def month_over_month_growth():
    logging.info("Month-over-Month revenue growth")

    monthly = (
        df.groupby(df["date"].dt.to_period("M"))["revenue"]
        .sum()
        .reset_index()
    )
    monthly.columns = ["month", "revenue"]
    monthly["prev_month_revenue"] = monthly["revenue"].shift(1)
    monthly["mom_growth_pct"] = (
        (monthly["revenue"] - monthly["prev_month_revenue"])
        / monthly["prev_month_revenue"] * 100
    ).round(2)

    print("\n--- Month-over-Month Revenue Growth ---")
    print(monthly.tail(6).to_string(index=False))
    return monthly


# ── 3. Year-over-Year Comparison ──────────────────────────────────────────────
# SQL: Compare same month across years
def year_over_year_comparison():
    logging.info("Year-over-Year revenue comparison")

    monthly = (
        df.groupby(["year", "month"])["revenue"]
        .sum()
        .round(2)
        .reset_index()
    )

    yr_2022 = monthly[monthly["year"] == 2022][["month", "revenue"]].rename(columns={"revenue": "revenue_2022"})
    yr_2023 = monthly[monthly["year"] == 2023][["month", "revenue"]].rename(columns={"revenue": "revenue_2023"})

    yoy = pd.merge(yr_2022, yr_2023, on="month")
    yoy["yoy_growth_pct"] = (
        (yoy["revenue_2023"] - yoy["revenue_2022"])
        / yoy["revenue_2022"] * 100
    ).round(2)
    yoy["yoy_diff"] = (yoy["revenue_2023"] - yoy["revenue_2022"]).round(2)

    print("\n--- Year-over-Year Comparison (2022 vs 2023) ---")
    print(yoy.to_string(index=False))
    return yoy


# ── 4. Rolling Averages ───────────────────────────────────────────────────────
# SQL: AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
def rolling_averages():
    logging.info("Rolling averages — 7-day and 30-day")

    daily = df[["date", "revenue"]].copy().sort_values("date")

    daily["rolling_7d"]  = daily["revenue"].rolling(window=7,  min_periods=1).mean().round(2)
    daily["rolling_30d"] = daily["revenue"].rolling(window=30, min_periods=1).mean().round(2)
    daily["rolling_90d"] = daily["revenue"].rolling(window=90, min_periods=1).mean().round(2)

    print("\n--- Rolling Averages (sample) ---")
    print(daily[["date", "revenue", "rolling_7d", "rolling_30d", "rolling_90d"]].iloc[29:36].to_string(index=False))
    return daily


# ── 5. Cumulative Revenue ─────────────────────────────────────────────────────
# SQL: SUM(revenue) OVER (ORDER BY date) AS cumulative_revenue
def cumulative_revenue():
    logging.info("Cumulative revenue by year")

    daily = df[["date", "revenue", "year"]].copy().sort_values("date")

    daily["cumulative_revenue"] = daily.groupby("year")["revenue"].cumsum().round(2)

    print("\n--- Cumulative Revenue by Year (last 5 days of 2022) ---")
    sample = daily[daily["year"] == 2022].tail(5)
    print(sample[["date", "revenue", "cumulative_revenue"]].to_string(index=False))

    # Annual totals
    annual = daily.groupby("year")["revenue"].sum().round(2).reset_index()
    print("\n--- Annual Revenue Totals ---")
    print(annual.to_string(index=False))
    return daily


# ── 6. Seasonality Analysis ───────────────────────────────────────────────────
# Which months and weekdays perform best?
def seasonality_analysis():
    logging.info("Seasonality — by month and weekday")

    # By month
    by_month = (
        df.groupby("month")["revenue"]
        .agg(avg_revenue="mean", total_revenue="sum")
        .round(2)
        .reset_index()
    )
    by_month["month_name"] = pd.to_datetime(by_month["month"], format="%m").dt.strftime("%B")

    print("\n--- Average Revenue by Month (Seasonality) ---")
    print(by_month[["month_name", "avg_revenue", "total_revenue"]].to_string(index=False))

    # By weekday
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    by_weekday = (
        df.groupby("weekday")["revenue"]
        .mean()
        .round(2)
        .reindex(weekday_order)
        .reset_index()
    )
    by_weekday.columns = ["weekday", "avg_revenue"]

    print("\n--- Average Revenue by Weekday ---")
    print(by_weekday.to_string(index=False))
    return by_month, by_weekday


# ── 7. Quarterly Performance ──────────────────────────────────────────────────
def quarterly_performance():
    logging.info("Quarterly performance summary")

    quarterly = (
        df.groupby("quarter")
        .agg(
            total_revenue  = ("revenue", "sum"),
            total_orders   = ("orders",  "sum"),
            avg_daily_rev  = ("revenue", "mean"),
            peak_day_rev   = ("revenue", "max")
        )
        .round(2)
        .reset_index()
    )

    quarterly["prev_quarter_rev"] = quarterly["total_revenue"].shift(1)
    quarterly["qoq_growth_pct"] = (
        (quarterly["total_revenue"] - quarterly["prev_quarter_rev"])
        / quarterly["prev_quarter_rev"] * 100
    ).round(2)

    print("\n--- Quarterly Performance ---")
    print(quarterly.to_string(index=False))
    return quarterly


# ── 8. Regional Time Series ───────────────────────────────────────────────────
def regional_monthly_trend():
    logging.info("Regional monthly revenue trend")

    regional = (
        df.groupby([df["date"].dt.to_period("M"), "region"])["revenue"]
        .sum()
        .round(2)
        .reset_index()
    )
    regional["date"] = regional["date"].astype(str)

    # Pivot to wide format for easy comparison
    pivot = regional.pivot_table(
        index="date",
        columns="region",
        values="revenue",
        aggfunc="sum"
    ).round(2)

    print("\n--- Regional Monthly Revenue (first 6 months) ---")
    print(pivot.head(6))
    return pivot


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("TIME SERIES ANALYSIS IN PANDAS")
    print("=" * 60)
    print(f"Dataset: {len(df):,} daily records from {df['date'].min().date()} to {df['date'].max().date()}")

    monthly_summary()
    month_over_month_growth()
    year_over_year_comparison()
    rolling_averages()
    cumulative_revenue()
    seasonality_analysis()
    quarterly_performance()
    regional_monthly_trend()

    print("\n" + "=" * 60)
    print("All time series demos completed.")
    print("=" * 60)
