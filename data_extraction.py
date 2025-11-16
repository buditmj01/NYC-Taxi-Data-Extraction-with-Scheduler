"""
NYC Taxi Data Extraction Pipeline

Combines Green and Yellow taxi data from parquet files.
- Daily: Processes a single date and saves to Parquet file
- Daily-Range: Processes multiple consecutive days in one command (efficient!)
- Weekly: Sends accumulated daily data to PostgreSQL database or saves as Parquet

Usage:
    # Process single day
    python data_extraction.py --mode daily --date 2025-09-01

    # Process 7 days in one command (recommended for weekly prep)
    python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

    # Save weekly data to PostgreSQL (default)
    python data_extraction.py --mode weekly

    # Save weekly data to Parquet file instead
    python data_extraction.py --mode weekly --output parquet
"""

import os
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, month, year, weekofyear, to_date, concat_ws, lit, current_timestamp
)

# ============================================================================
# CONFIGURATION
# ============================================================================
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Input files
GREEN_FILE = str(RAW_DATA_DIR / "green_tripdata_2025-09.parquet")
YELLOW_FILE = str(RAW_DATA_DIR / "yellow_tripdata_2025-09.parquet")

# Daily output directory
DAILY_OUTPUT_DIR = PROCESSED_DATA_DIR / "daily"

# PostgreSQL Configuration (from environment or defaults)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# JDBC URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def create_processed_dir():
    """Create processed data directory if it doesn't exist."""
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Processed directory ready: {PROCESSED_DATA_DIR}")
    print(f"✓ Daily directory ready: {DAILY_OUTPUT_DIR}\n")


def load_data(spark):
    """Load green and yellow taxi data."""
    print("=" * 70)
    print("LOADING DATA")
    print("=" * 70)
    
    print(f"Loading Green taxi data: {GREEN_FILE}")
    green_df = spark.read.parquet(GREEN_FILE)
    print(f"  → Rows: {green_df.count():,}")
    
    print(f"\nLoading Yellow taxi data: {YELLOW_FILE}")
    yellow_df = spark.read.parquet(YELLOW_FILE)
    print(f"  → Rows: {yellow_df.count():,}\n")
    
    return green_df, yellow_df


def normalize_columns(green_df, yellow_df):
    """
    Normalize column names so both datasets can be combined.
    
    Green taxi columns:
      - lpep_pickup_datetime, lpep_dropoff_datetime
    
    Yellow taxi columns:
      - tpep_pickup_datetime, tpep_dropoff_datetime
    """
    print("=" * 70)
    print("NORMALIZING COLUMNS")
    print("=" * 70)
    
    # Rename green columns to match yellow
    green_normalized = green_df.withColumnRenamed(
        "lpep_pickup_datetime", "pickup_datetime"
    ).withColumnRenamed(
        "lpep_dropoff_datetime", "dropoff_datetime"
    ).withColumn(
        "taxi_type", lit("Green")
    )
    
    # Rename yellow columns
    yellow_normalized = yellow_df.withColumnRenamed(
        "tpep_pickup_datetime", "pickup_datetime"
    ).withColumnRenamed(
        "tpep_dropoff_datetime", "dropoff_datetime"
    ).withColumn(
        "taxi_type", lit("Yellow")
    )
    
    print("✓ Column names normalized")
    print("✓ Added 'taxi_type' column\n")
    
    return green_normalized, yellow_normalized


def combine_data(green_df, yellow_df):
    """Combine green and yellow data using union."""
    print("=" * 70)
    print("COMBINING DATA")
    print("=" * 70)
    
    # Get common columns
    common_cols = set(green_df.columns) & set(yellow_df.columns)
    common_cols_list = sorted(list(common_cols))
    
    # Select common columns and union
    combined_df = green_df.select(common_cols_list).unionByName(
        yellow_df.select(common_cols_list)
    )
    
    print(f"✓ Data combined")
    print(f"  → Total rows: {combined_df.count():,}")
    print(f"  → Total columns: {len(combined_df.columns)}\n")
    
    return combined_df


def filter_by_date(df, target_date):
    """Filter data by specific date."""
    print("=" * 70)
    print(f"FILTERING DATA - Date: {target_date}")
    print("=" * 70)

    filtered_df = df.filter(
        to_date(col("pickup_datetime")) == target_date
    )

    row_count = filtered_df.count()
    print(f"✓ Filtered to date: {target_date}")
    print(f"  → Rows after filter: {row_count:,}\n")

    return filtered_df, row_count


def save_daily_parquet(df, date_str):
    """Save daily data to parquet file as a single file (not a folder)."""
    import shutil

    print("=" * 70)
    print("SAVING DAILY DATA TO PARQUET")
    print("=" * 70)

    # Use temporary folder for Spark output
    temp_output = DAILY_OUTPUT_DIR / f"_temp_taxi_data_{date_str}"
    final_output = DAILY_OUTPUT_DIR / f"taxi_data_{date_str}.parquet"

    # Remove existing files if they exist
    if temp_output.exists():
        shutil.rmtree(temp_output)
    if final_output.exists():
        if final_output.is_dir():
            shutil.rmtree(final_output)
        else:
            final_output.unlink()

    # Save as single file using coalesce(1)
    df.coalesce(1).write.mode("overwrite").parquet(str(temp_output))

    # Find the part file and rename it
    part_files = list(temp_output.glob("part-*.parquet"))
    if part_files:
        shutil.move(str(part_files[0]), str(final_output))
        # Clean up temp folder
        shutil.rmtree(temp_output)
    else:
        # Fallback: rename the whole folder
        shutil.move(str(temp_output), str(final_output))

    file_size = final_output.stat().st_size if final_output.is_file() else get_file_size(str(final_output))

    print(f"✓ Saved to: {final_output}")
    print(f"  → File type: {'Single file' if final_output.is_file() else 'Folder'}")
    print(f"  → Size: {format_bytes(file_size) if final_output.is_file() else get_file_size(str(final_output))}\n")

    return final_output


def load_weekly_data(spark):
    """Load all daily parquet files for the week."""
    print("=" * 70)
    print("LOADING WEEKLY DATA FROM DAILY FILES")
    print("=" * 70)

    daily_files = list(DAILY_OUTPUT_DIR.glob("taxi_data_*.parquet"))

    if not daily_files:
        print("✗ No daily files found")
        return None

    print(f"Found {len(daily_files)} daily file(s):")
    for f in sorted(daily_files):
        print(f"  - {f.name}")

    # Read all daily files
    dfs = []
    for file in daily_files:
        df = spark.read.parquet(str(file))
        dfs.append(df)

    # Union all dataframes
    if len(dfs) == 1:
        combined_df = dfs[0]
    else:
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)

    row_count = combined_df.count()
    print(f"\n✓ Combined {len(dfs)} daily file(s)")
    print(f"  → Total rows: {row_count:,}\n")

    return combined_df


def get_week_number_from_daily_files():
    """Extract week number from daily files."""
    daily_files = list(DAILY_OUTPUT_DIR.glob("taxi_data_*.parquet"))

    if not daily_files:
        return None

    # Get first date from first file
    first_file = sorted(daily_files)[0]
    date_str = first_file.stem.replace("taxi_data_", "")

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # Get week number (1-5 for weeks in the month)
        week_of_month = (date_obj.day - 1) // 7 + 1
        month_name = date_obj.strftime("%B").lower()
        year = date_obj.year

        return f"week_{week_of_month}_{month_name}_{year}"
    except:
        return "weekly_taxi_data"


def save_to_postgres(df, table_name, mode="overwrite"):
    """Save data to PostgreSQL database."""
    print("=" * 70)
    print("SAVING TO POSTGRESQL")
    print("=" * 70)

    try:
        print(f"Database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        print(f"Table: {table_name}")
        print(f"Mode: {mode}")

        # Write to PostgreSQL
        df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()

        row_count = df.count()
        print(f"✓ Successfully saved {row_count:,} rows to PostgreSQL")
        print(f"✓ Table '{table_name}' created/updated\n")

    except Exception as e:
        print(f"✗ Failed to save to PostgreSQL: {str(e)}")
        print("  Make sure PostgreSQL is running and accessible")
        print(f"  Connection: {POSTGRES_HOST}:{POSTGRES_PORT}")
        print("  Data is still saved to Parquet files\n")
        raise


def format_bytes(size_bytes):
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def save_weekly_parquet(df, week_name):
    """Save weekly data to parquet file as a single file (not a folder).

    Args:
        df: DataFrame to save
        week_name: Week identifier (e.g., 'week_1_september_2025')

    Returns:
        Path to saved parquet file
    """
    import shutil

    print("=" * 70)
    print("SAVING WEEKLY PARQUET")
    print("=" * 70)

    # Create weekly output directory
    weekly_output_dir = PROCESSED_DATA_DIR / "weekly"
    weekly_output_dir.mkdir(parents=True, exist_ok=True)

    # Use temporary folder for Spark output
    temp_output = weekly_output_dir / f"_temp_{week_name}"
    final_output = weekly_output_dir / f"{week_name}.parquet"

    print(f"Output: {final_output}\n")

    # Remove existing files if they exist
    if temp_output.exists():
        shutil.rmtree(temp_output)
    if final_output.exists():
        if final_output.is_dir():
            shutil.rmtree(final_output)
        else:
            final_output.unlink()

    # Save as single file using coalesce(1)
    df.coalesce(1).write.mode("overwrite").parquet(str(temp_output))

    # Find the part file and rename it
    part_files = list(temp_output.glob("part-*.parquet"))
    if part_files:
        shutil.move(str(part_files[0]), str(final_output))
        # Clean up temp folder
        shutil.rmtree(temp_output)
    else:
        # Fallback: rename the whole folder
        shutil.move(str(temp_output), str(final_output))

    row_count = df.count()
    file_size = final_output.stat().st_size if final_output.is_file() else 0

    print(f"✓ Successfully saved {row_count:,} rows")
    print(f"  File: {final_output.name}")
    print(f"  File type: {'Single file' if final_output.is_file() else 'Folder'}")
    print(f"  Size: {format_bytes(file_size) if final_output.is_file() else get_file_size(str(final_output))}\n")

    return final_output


def get_file_size(file_path):
    """Get human-readable file size."""
    try:
        size_bytes = sum(
            f.stat().st_size
            for f in Path(file_path).rglob("*")
            if f.is_file()
        )

        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024

        return f"{size_bytes:.2f} TB"
    except:
        return "unknown"


def print_summary(combined_df):
    """Print summary statistics."""
    print("=" * 70)
    print("DATA SUMMARY")
    print("=" * 70)
    
    print(f"Total Rows: {combined_df.count():,}")
    print(f"Total Columns: {len(combined_df.columns)}")
    print(f"\nColumns: {', '.join(combined_df.columns)}\n")
    
    # Show sample
    print("Sample data (first 3 rows):")
    selected_cols = ["taxi_type", "pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    combined_df.select(selected_cols).show(3, truncate=False)


def download_postgres_jdbc():
    """Download PostgreSQL JDBC driver if not present."""
    import urllib.request

    jdbc_dir = ROOT_DIR / "jdbc"
    jdbc_file = jdbc_dir / "postgresql-42.6.0.jar"

    if jdbc_file.exists():
        return str(jdbc_file)

    print("Downloading PostgreSQL JDBC driver...")
    jdbc_dir.mkdir(exist_ok=True)

    jdbc_url = "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
    urllib.request.urlretrieve(jdbc_url, str(jdbc_file))

    print(f"✓ JDBC driver downloaded to: {jdbc_file}\n")
    return str(jdbc_file)


def process_daily(spark, date_str):
    """Process daily data and save to Parquet."""
    print(f"\n{'='*70}")
    print(f"DAILY PROCESSING MODE - Date: {date_str}")
    print(f"{'='*70}\n")

    # Load raw data
    green_df, yellow_df = load_data(spark)

    # Normalize columns
    green_norm, yellow_norm = normalize_columns(green_df, yellow_df)

    # Combine data
    combined_df = combine_data(green_norm, yellow_norm)

    # Filter by date
    filtered_df, row_count = filter_by_date(combined_df, date_str)

    if row_count == 0:
        print(f"⚠ No data found for date: {date_str}")
        print("  Skipping save operation\n")
        return

    # Save daily parquet
    output_file = save_daily_parquet(filtered_df, date_str)

    # Print summary
    print_summary(filtered_df)

    print("=" * 70)
    print("✓ DAILY PROCESSING COMPLETED")
    print("=" * 70)
    print(f"Date: {date_str}")
    print(f"Rows: {row_count:,}")
    print(f"Output: {output_file}\n")


def process_daily_range(spark, start_date_str, num_days=7):
    """Process multiple consecutive days in one command.

    Args:
        spark: SparkSession
        start_date_str: Starting date in YYYY-MM-DD format
        num_days: Number of consecutive days to process (default: 7)
    """
    print(f"\n{'='*70}")
    print(f"DAILY RANGE PROCESSING MODE - {num_days} days starting from {start_date_str}")
    print(f"{'='*70}\n")

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"✗ Invalid date format: {start_date_str}")
        print("  Expected format: YYYY-MM-DD")
        return

    # Load raw data once (more efficient than loading for each day)
    green_df, yellow_df = load_data(spark)

    # Normalize columns once
    green_norm, yellow_norm = normalize_columns(green_df, yellow_df)

    # Combine data once
    combined_df = combine_data(green_norm, yellow_norm)

    # Process each day
    processed_count = 0
    skipped_count = 0

    for day_offset in range(num_days):
        current_date = start_date + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")

        print(f"\n{'─'*70}")
        print(f"Processing day {day_offset + 1}/{num_days}: {date_str}")
        print(f"{'─'*70}\n")

        # Filter by date
        filtered_df, row_count = filter_by_date(combined_df, date_str)

        if row_count == 0:
            print(f"⚠ No data found for date: {date_str}")
            print("  Skipping this date\n")
            skipped_count += 1
            continue

        # Save daily parquet
        output_file = save_daily_parquet(filtered_df, date_str)

        print(f"✓ Saved {row_count:,} rows to {output_file.name}")
        processed_count += 1

    # Summary
    print(f"\n{'='*70}")
    print("✓ DAILY RANGE PROCESSING COMPLETED")
    print(f"{'='*70}")
    print(f"Start date: {start_date_str}")
    print(f"Days requested: {num_days}")
    print(f"Days processed: {processed_count}")
    print(f"Days skipped: {skipped_count}")
    print(f"\nNext step: Run weekly mode to upload to PostgreSQL")
    print(f"  → python data_extraction.py --mode weekly\n")


def process_weekly(spark, output_format="postgres"):
    """Load weekly data from daily files and save to PostgreSQL or Parquet.

    Args:
        spark: SparkSession
        output_format: 'postgres' or 'parquet' (default: 'postgres')
    """
    print(f"\n{'='*70}")
    print(f"WEEKLY PROCESSING MODE - Output: {output_format.upper()}")
    print(f"{'='*70}\n")

    # Load all daily parquet files
    weekly_df = load_weekly_data(spark)

    if weekly_df is None:
        print("✗ No daily data found. Run daily processing first.")
        return

    # Determine week name based on week
    week_name = get_week_number_from_daily_files()
    if week_name is None:
        week_name = "weekly_taxi_data"

    print(f"Week name: {week_name}\n")

    # Save based on output format
    if output_format == "parquet":
        output_file = save_weekly_parquet(weekly_df, week_name)
    else:  # postgres
        save_to_postgres(weekly_df, week_name, mode="overwrite")

    # Print summary
    print_summary(weekly_df)

    print("=" * 70)
    print("✓ WEEKLY PROCESSING COMPLETED")
    print("=" * 70)
    if output_format == "parquet":
        print(f"Output file: {output_file}")
        print(f"Location: data/processed/weekly/\n")
    else:
        print(f"PostgreSQL table: {week_name}")
        print(f"Database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}\n")


def main():
    """Main execution pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="NYC Taxi Data Extraction Pipeline")
    parser.add_argument(
        "--mode",
        choices=["daily", "daily-range", "weekly"],
        required=True,
        help="Processing mode: 'daily' for single day, 'daily-range' for multiple days, 'weekly' to send to PostgreSQL"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Date to process in YYYY-MM-DD format (required for daily and daily-range modes)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of consecutive days to process in daily-range mode (default: 7)"
    )
    parser.add_argument(
        "--output",
        choices=["postgres", "parquet"],
        default="postgres",
        help="Output format for weekly mode: 'postgres' (database) or 'parquet' (file) (default: postgres)"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.mode in ["daily", "daily-range"] and not args.date:
        parser.error("--date is required when mode is 'daily' or 'daily-range'")

    print("\n")
    print("█" * 70)
    print("  NYC TAXI DATA EXTRACTION PIPELINE")
    print("█" * 70)
    print("\n")

    # Download JDBC driver (needed for weekly mode)
    jdbc_path = download_postgres_jdbc()

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Taxi_Data_Extraction") \
        .config("spark.jars", jdbc_path) \
        .config("spark.driver.extraClassPath", jdbc_path) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Create directories
        create_processed_dir()

        # Execute based on mode
        if args.mode == "daily":
            process_daily(spark, args.date)
        elif args.mode == "daily-range":
            process_daily_range(spark, args.date, args.days)
        elif args.mode == "weekly":
            process_weekly(spark, args.output)

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}\n")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
