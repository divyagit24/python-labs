import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

def window_functions_demo(df: pd.DataFrame) -> pd.DataFrame:

    try:
        logging.info("Applying window functions")
        
        # Sort by department and salary descending
        df = df.sort_values(["department", "salary"], ascending=[True, False]).reset_index(drop=True)
        
        # RANK()
        df["rank"] = df.groupby("department")["salary"].rank(method="min", ascending=False)
        
        # DENSE_RANK()
        df["dense_rank"] = df.groupby("department")["salary"].rank(method="dense", ascending=False)
        
        # ROW_NUMBER()
        df["row_number"] = df.groupby("department").cumcount() + 1
        
        # Cumulative SUM
        df["cumulative_salary"] = df.groupby("department")["salary"].cumsum()
        
        # LAG: previous salary
        df["lag_salary"] = df.groupby("department")["salary"].shift(1)
        
        # LEAD: next salary
        df["lead_salary"] = df.groupby("department")["salary"].shift(-1)
        
        logging.info("Window functions applied successfully")
        return df
    except Exception as e:
        logging.error(f"Error in window functions: {e}")
        raise

if __name__ == "__main__":
    data = {
        "department": ["A","A","A","B","B","B","C","C"],
        "employee": ["E1","E2","E3","E4","E5","E6","E7","E8"],
        "salary": [5000,6000,5500,7000,7200,7100,4000,4200]
    }

    df = pd.DataFrame(data)
    result_df = window_functions_demo(df)
    print(result_df)
