import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

def rank_employees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns rank to employees within their department based on salary.
    Equivalent to SQL window function: RANK() OVER(PARTITION BY department ORDER BY salary DESC)
    """
    try:
        logging.info("Ranking employees within departments")
        df = df.sort_values(["department","salary"], ascending=[True, False])
        df["rank_within_dept"] = df.groupby("department").cumcount() + 1
        logging.info("Ranking completed")
        return df
    except Exception as e:
        logging.error(f"Error in ranking calculation: {e}")
        raise

if __name__ == "__main__":
    data = {
        "department": ["A","A","A","B","B"],
        "employee": ["E1","E2","E3","E4","E5"],
        "salary": [5000,6000,5500,7000,7200]
    }
    df = pd.DataFrame(data)
    ranked_df = rank_employees(df)
    print(ranked_df)
