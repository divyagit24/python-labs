import pandas as pd

# Sample dataset
data = {
    "customer_id": [1, 2, 3, 4, 5],
    "state": ["CA", "NY", "CA", "TX", "CA"],
    "sales": [200, 150, 300, 100, 250]
}

df = pd.DataFrame(data)

# Equivalent SQL:
# SELECT state, SUM(sales) as total_sales
# FROM table
# WHERE sales > 150
# GROUP BY state
# HAVING total_sales > 400
# ORDER BY total_sales DESC;

filtered = df[df["sales"] > 150]

grouped = (
    filtered
    .groupby("state")
    .agg(total_sales=("sales", "sum"))
    .reset_index()
)

result = grouped[grouped["total_sales"] > 400] \
            .sort_values(by="total_sales", ascending=False)

print(result)