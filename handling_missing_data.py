import pandas as pd
import numpy as np


def create_sample_dataframe() -> pd.DataFrame:
    """Create sample dataframe with missing values"""
    data = {
        "Name": ["Alice", "Bob", "Charlie", "David", "Eva"],
        "Age": [25, np.nan, 30, np.nan, 28],
        "Salary": [50000, 60000, np.nan, 80000, np.nan],
        "Experience": [2, 5, np.nan, 8, 4],
    }

    df = pd.DataFrame(data)
    return df


def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values using different strategies"""
    
    # Fill numeric columns with mean
    df["Age"] = df["Age"].fillna(df["Age"].mean())
    
    # Fill Salary with median
    df["Salary"] = df["Salary"].fillna(df["Salary"].median())
    
    # Fill Experience with 0
    df["Experience"] = df["Experience"].fillna(0)

    return df


def drop_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that contain any missing values."""

    return df.dropna()


def interpolate_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Interpolate numeric missing values"""
    
    numeric_cols = df.select_dtypes(include=np.number).columns
    df[numeric_cols] = df[numeric_cols].interpolate()

    return df


def main():
    print("\n=== Original Data ===\n")
    df = create_sample_dataframe()
    print(df)

    print("\n=== Fill Missing Values ===\n")
    filled_df = fill_missing_values(df.copy())
    print(filled_df)

    print("\n=== Drop Missing Values ===\n")
    dropped_df = drop_missing_values(df.copy())
    print(dropped_df)

    print("\n=== Interpolated Values ===\n")
    interpolated_df = interpolate_missing_values(df.copy())
    print(interpolated_df)


if __name__ == "__main__":
    main()