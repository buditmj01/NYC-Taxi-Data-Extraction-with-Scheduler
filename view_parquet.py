# view_parquet.py
"""
Simple Parquet File Viewer for NYC Taxi Data

Displays data types and report-related columns (7 columns) with first 5 rows
for both Green and Yellow taxi datasets using PyArrow & Pandas.

Report-related columns:
- VendorID
- pickup/dropoff datetime
- passenger_count
- trip_distance
- fare_amount
- total_amount
"""

from pathlib import Path
import pandas as pd

# File paths
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data" / "raw"
GREEN_FILE = str(DATA_DIR / "green_tripdata_2025-09.parquet")
YELLOW_FILE = str(DATA_DIR / "yellow_tripdata_2025-09.parquet")


def main():
    """Load and display taxi data with data types and sample rows."""

    # Load data
    green_df = pd.read_parquet(GREEN_FILE)
    yellow_df = pd.read_parquet(YELLOW_FILE)

    # ========================================================================
    # GREEN TAXI DATA
    # ========================================================================
    print("=" * 70)
    print("GREEN TAXI DATA")
    print("=" * 70)
    print(f"Total Rows: {len(green_df):,}")
    print(f"Total Columns: {len(green_df.columns)}\n")

    # Show data types
    print("Data Types:")
    for col_name, col_type in green_df.dtypes.items():
        print(f"  {col_name}: {col_type}")

    # Show columns used in reports
    print("\nFirst 5 rows (report-related columns):")
    selected_cols = [
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount"
    ]
    print(green_df[selected_cols].head(5).to_string(index=False))

    # ========================================================================
    # YELLOW TAXI DATA
    # ========================================================================
    print("\n" + "=" * 70)
    print("YELLOW TAXI DATA")
    print("=" * 70)
    print(f"Total Rows: {len(yellow_df):,}")
    print(f"Total Columns: {len(yellow_df.columns)}\n")

    # Show data types
    print("Data Types:")
    for col_name, col_type in yellow_df.dtypes.items():
        print(f"  {col_name}: {col_type}")

    # Show columns used in reports
    print("\nFirst 5 rows (report-related columns):")
    selected_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount"
    ]
    print(yellow_df[selected_cols].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
