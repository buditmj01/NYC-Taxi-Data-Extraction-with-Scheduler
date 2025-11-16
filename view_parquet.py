# view_parquet.py
"""
Simple Parquet File Viewer for NYC Taxi Data

Displays data types and report-related columns (7 columns) with first 5 rows
for both Green and Yellow taxi datasets.

Report-related columns:
- VendorID
- pickup/dropoff datetime
- passenger_count
- trip_distance
- fare_amount
- total_amount
"""

from pathlib import Path
from pyspark.sql import SparkSession

# File paths
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data" / "raw"
GREEN_FILE = str(DATA_DIR / "green_tripdata_2025-09.parquet")
YELLOW_FILE = str(DATA_DIR / "yellow_tripdata_2025-09.parquet")


def main():
    """Load and display taxi data with data types and sample rows."""

    # Initialize Spark
    spark = SparkSession.builder.appName("Taxi_Data_Viewer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load data
    green_df = spark.read.parquet(GREEN_FILE)
    yellow_df = spark.read.parquet(YELLOW_FILE)

    # ========================================================================
    # GREEN TAXI DATA
    # ========================================================================
    print("=" * 70)
    print("GREEN TAXI DATA")
    print("=" * 70)
    print(f"Total Rows: {green_df.count():,}\n")

    # Show data types
    print("Data Types:")
    for col_name, col_type in green_df.dtypes:
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
    green_df.select(selected_cols).show(5, truncate=False)

    # ========================================================================
    # YELLOW TAXI DATA
    # ========================================================================
    print("\n" + "=" * 70)
    print("YELLOW TAXI DATA")
    print("=" * 70)
    print(f"Total Rows: {yellow_df.count():,}\n")

    # Show data types
    print("Data Types:")
    for col_name, col_type in yellow_df.dtypes:
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
    yellow_df.select(selected_cols).show(5, truncate=False)

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()
