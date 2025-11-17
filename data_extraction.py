"""
NYC Taxi Data Extraction Pipeline

Combines Green and Yellow taxi data from parquet files using PyArrow & Pandas.
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
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from sqlalchemy import create_engine

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

# SQLAlchemy URL
SQLALCHEMY_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
LOG_FILE = ROOT_DIR / "report.log"

def setup_logging():
    """Configure logging to write to both file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [DATA_EXTRACTION] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def create_processed_dir():
    """Create processed data directory if it doesn't exist."""
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DAILY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Processed directory ready: {PROCESSED_DATA_DIR}")
    print(f"✓ Daily directory ready: {DAILY_OUTPUT_DIR}\n")


def load_data():
    """Load green and yellow taxi data."""
    print("=" * 70)
    print("LOADING DATA")
    print("=" * 70)

    print(f"Loading Green taxi data: {GREEN_FILE}")
    green_df = pd.read_parquet(GREEN_FILE)
    print(f"  → Rows: {len(green_df):,}")

    print(f"\nLoading Yellow taxi data: {YELLOW_FILE}")
    yellow_df = pd.read_parquet(YELLOW_FILE)
    print(f"  → Rows: {len(yellow_df):,}\n")

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
    green_normalized = green_df.rename(columns={
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime"
    }).copy()
    green_normalized["taxi_type"] = "Green"

    # Rename yellow columns
    yellow_normalized = yellow_df.rename(columns={
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime"
    }).copy()
    yellow_normalized["taxi_type"] = "Yellow"

    print("✓ Column names normalized")
    print("✓ Added 'taxi_type' column\n")

    return green_normalized, yellow_normalized


def combine_data(green_df, yellow_df):
    """Combine green and yellow data using concat."""
    print("=" * 70)
    print("COMBINING DATA")
    print("=" * 70)

    # Get common columns
    common_cols = sorted(list(set(green_df.columns) & set(yellow_df.columns)))

    # Select common columns and concatenate
    combined_df = pd.concat([
        green_df[common_cols],
        yellow_df[common_cols]
    ], ignore_index=True)

    print(f"✓ Data combined")
    print(f"  → Total rows: {len(combined_df):,}")
    print(f"  → Total columns: {len(combined_df.columns)}\n")

    return combined_df


def filter_by_date(df, target_date):
    """Filter data by specific date."""
    print("=" * 70)
    print(f"FILTERING DATA - Date: {target_date}")
    print("=" * 70)

    # Convert pickup_datetime to date
    df['pickup_date'] = pd.to_datetime(df['pickup_datetime']).dt.date

    # Convert target_date string to date object
    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()

    filtered_df = df[df['pickup_date'] == target_date_obj].copy()

    # Drop the temporary column
    filtered_df = filtered_df.drop(columns=['pickup_date'])

    row_count = len(filtered_df)
    print(f"✓ Filtered to date: {target_date}")
    print(f"  → Rows after filter: {row_count:,}\n")

    return filtered_df, row_count


def save_daily_parquet(df, date_str):
    """Save daily data to parquet file as a single file."""
    print("=" * 70)
    print("SAVING DAILY DATA TO PARQUET")
    print("=" * 70)

    final_output = DAILY_OUTPUT_DIR / f"taxi_data_{date_str}.parquet"

    # Remove existing file if it exists
    if final_output.exists():
        final_output.unlink()

    # Save using PyArrow
    table = pa.Table.from_pandas(df)
    pq.write_table(table, str(final_output))

    file_size = final_output.stat().st_size

    print(f"✓ Saved to: {final_output}")
    print(f"  → File type: Single file")
    print(f"  → Size: {format_bytes(file_size)}\n")

    return final_output


def load_weekly_data():
    """Load all daily parquet files for the week."""
    print("=" * 70)
    print("LOADING WEEKLY DATA FROM DAILY FILES")
    print("=" * 70)

    daily_files = sorted(list(DAILY_OUTPUT_DIR.glob("taxi_data_*.parquet")))

    if not daily_files:
        print("✗ No daily files found")
        return None

    print(f"Found {len(daily_files)} daily file(s):")
    for f in daily_files:
        print(f"  - {f.name}")

    # Read all daily files
    dfs = []
    for file in daily_files:
        df = pd.read_parquet(str(file))
        dfs.append(df)

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    row_count = len(combined_df)
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


def save_to_postgres(df, table_name, mode="replace"):
    """Save data to PostgreSQL database using SQLAlchemy."""
    print("=" * 70)
    print("SAVING TO POSTGRESQL")
    print("=" * 70)

    try:
        print(f"Database: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        print(f"Table: {table_name}")
        print(f"Mode: {mode}")

        # Create SQLAlchemy engine
        engine = create_engine(SQLALCHEMY_URL)

        # Write to PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists=mode,
            index=False,
            method='multi',
            chunksize=10000
        )

        row_count = len(df)
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
    """Save weekly data to parquet file as a single file.

    Args:
        df: DataFrame to save
        week_name: Week identifier (e.g., 'week_1_september_2025')

    Returns:
        Path to saved parquet file
    """
    print("=" * 70)
    print("SAVING WEEKLY PARQUET")
    print("=" * 70)

    # Create weekly output directory
    weekly_output_dir = PROCESSED_DATA_DIR / "weekly"
    weekly_output_dir.mkdir(parents=True, exist_ok=True)

    final_output = weekly_output_dir / f"{week_name}.parquet"

    print(f"Output: {final_output}\n")

    # Remove existing file if it exists
    if final_output.exists():
        if final_output.is_dir():
            import shutil
            shutil.rmtree(final_output)
        else:
            final_output.unlink()

    # Save using PyArrow
    table = pa.Table.from_pandas(df)
    pq.write_table(table, str(final_output))

    row_count = len(df)
    file_size = final_output.stat().st_size

    print(f"✓ Successfully saved {row_count:,} rows")
    print(f"  File: {final_output.name}")
    print(f"  File type: Single file")
    print(f"  Size: {format_bytes(file_size)}\n")

    return final_output


def print_summary(combined_df):
    """Print summary statistics."""
    print("=" * 70)
    print("DATA SUMMARY")
    print("=" * 70)

    print(f"Total Rows: {len(combined_df):,}")
    print(f"Total Columns: {len(combined_df.columns)}")
    print(f"\nColumns: {', '.join(combined_df.columns)}\n")

    # Show sample
    print("Sample data (first 3 rows):")
    selected_cols = ["taxi_type", "pickup_datetime", "trip_distance", "fare_amount", "total_amount"]
    print(combined_df[selected_cols].head(3).to_string(index=False))
    print()


def process_daily(date_str):
    """Process daily data and save to Parquet."""
    print(f"\n{'='*70}")
    print(f"DAILY PROCESSING MODE - Date: {date_str}")
    print(f"{'='*70}\n")

    # Load raw data
    green_df, yellow_df = load_data()

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


def process_daily_range(start_date_str, num_days=7):
    """Process multiple consecutive days in one command.

    Args:
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
    green_df, yellow_df = load_data()

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


def process_weekly(output_format="postgres"):
    """Load weekly data from daily files and save to PostgreSQL or Parquet.

    Args:
        output_format: 'postgres' or 'parquet' (default: 'postgres')
    """
    print(f"\n{'='*70}")
    print(f"WEEKLY PROCESSING MODE - Output: {output_format.upper()}")
    print(f"{'='*70}\n")

    # Load all daily parquet files
    weekly_df = load_weekly_data()

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
        save_to_postgres(weekly_df, week_name, mode="replace")

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
    # Setup logging
    logger = setup_logging()

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

    logger.info("=" * 70)
    logger.info("NYC TAXI DATA EXTRACTION PIPELINE - STARTED")
    logger.info("=" * 70)
    logger.info(f"Mode: {args.mode}")
    if args.date:
        logger.info(f"Date: {args.date}")
    if args.mode == "daily-range":
        logger.info(f"Days: {args.days}")
    if args.mode == "weekly":
        logger.info(f"Output: {args.output}")

    print("\n")
    print("█" * 70)
    print("  NYC TAXI DATA EXTRACTION PIPELINE")
    print("█" * 70)
    print("\n")

    try:
        # Create directories
        create_processed_dir()

        # Execute based on mode
        if args.mode == "daily":
            logger.info(f"Processing daily mode for date: {args.date}")
            process_daily(args.date)
            logger.info(f"Daily processing completed successfully for {args.date}")
        elif args.mode == "daily-range":
            logger.info(f"Processing daily-range mode: {args.days} days starting from {args.date}")
            process_daily_range(args.date, args.days)
            logger.info(f"Daily-range processing completed successfully for {args.days} days")
        elif args.mode == "weekly":
            logger.info(f"Processing weekly mode with output: {args.output}")
            process_weekly(args.output)
            logger.info(f"Weekly processing completed successfully to {args.output}")

        logger.info("=" * 70)
        logger.info("NYC TAXI DATA EXTRACTION PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        print(f"\n✗ ERROR: {str(e)}\n")
        raise


if __name__ == "__main__":
    main()
