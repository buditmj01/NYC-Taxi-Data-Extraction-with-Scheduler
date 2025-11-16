# NYC Taxi Data Pipeline - PostgreSQL Workflow

This document describes the complete workflow for processing NYC taxi data using PostgreSQL as the data source.

## Documentation Overview

- **[README.md](../README.md)** - Quick start guide and project overview
- **[POSTGRESQL_WORKFLOW.md](POSTGRESQL_WORKFLOW.md)** (this file) - Complete step-by-step workflow
- **[FEATURES_SUMMARY.md](FEATURES_SUMMARY.md)** - Comprehensive feature documentation

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA PIPELINE WORKFLOW                      │
└─────────────────────────────────────────────────────────────────┘

1. RAW DATA EXTRACTION (data_extraction.py)
   ├── Daily Mode: Process one date → Save to Parquet
   │   └── data/processed/daily/taxi_data_YYYY-MM-DD.parquet
   └── Weekly Mode: Combine daily files → Send to PostgreSQL
       └── PostgreSQL table: week_N_month_year

2. DATA PROCESSING (data_pipeline.py)
   ├── Read from PostgreSQL weekly table
   ├── Clean and validate data
   ├── Compute aggregations
   └── Save results to output/ directory

3. REPORTING (send_weekly_report.py)
   ├── Read aggregated CSVs from output/
   ├── Generate weekly summary report
   └── Send to Discord and/or Gmail
```

## Prerequisites

1. **PostgreSQL Database**
   ```bash
   # Start PostgreSQL using Docker
   ./setup_postgres.sh
   ```

2. **Environment Variables**
   Create a `.env` file (copy from `.env.example`):
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. **Python Dependencies**
   ```bash
   pip install pyspark pandas requests
   ```

## Quick Reference

### Processing Modes

| Mode | Command | Description |
|------|---------|-------------|
| **daily** | `--mode daily --date YYYY-MM-DD` | Process a single day |
| **daily-range** | `--mode daily-range --date YYYY-MM-DD --days N` | Process N consecutive days (default: 7) |
| **weekly** | `--mode weekly [--output postgres\|parquet]` | Upload to PostgreSQL or save as Parquet (default: postgres) |

### Quick Start Example

```bash
# Process a week in one command
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Upload to PostgreSQL
python data_extraction.py --mode weekly

# Generate reports
python data_pipeline.py --table week_1_september_2025

# Send reports
python send_weekly_report.py --week "week_1_september_2025"
```

## Step-by-Step Workflow

### Step 1: Extract and Load Data to PostgreSQL

Process data on a **daily basis**, then at the end of the week, send to PostgreSQL:

#### Daily Processing (Run daily)

**Option 1: Process one day at a time**
```bash
# Process data for a specific date
python data_extraction.py --mode daily --date 2025-09-01
python data_extraction.py --mode daily --date 2025-09-02
python data_extraction.py --mode daily --date 2025-09-03
# ... continue for all days of the week
```

**Option 2: Process 7 days in one command (NEW!)**
```bash
# Process 7 consecutive days starting from 2025-09-01
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Or use default (7 days)
python data_extraction.py --mode daily-range --date 2025-09-01

# Custom number of days (e.g., 5 days)
python data_extraction.py --mode daily-range --date 2025-09-01 --days 5
```

**Benefits of daily-range mode:**
- Processes multiple days in one command
- More efficient (loads raw data only once)
- Reduces manual work
- Perfect for weekly data preparation

This saves daily data to: `data/processed/daily/taxi_data_YYYY-MM-DD.parquet` (single file, not a folder)

#### Weekly Upload (Run at end of week)

**Option 1: Upload to PostgreSQL (default)**
```bash
# Combine all daily files and upload to PostgreSQL
python data_extraction.py --mode weekly

# Or explicitly specify postgres output
python data_extraction.py --mode weekly --output postgres
```

**Option 2: Save to Parquet file (NEW!)**
```bash
# Combine all daily files and save as a single parquet file
python data_extraction.py --mode weekly --output parquet
# → Creates: data/processed/weekly/week_1_september_2025.parquet (single file)
```

**When to use Parquet output:**
- For backup and archival purposes
- When PostgreSQL is not available
- For data sharing or portability
- For testing without database dependencies

**Note:** All parquet files are saved as single files (not folders) for easier handling.

This creates a PostgreSQL table (if using postgres output) with automatic naming:
- **Week 1 (days 1-7)**: `week_1_september_2025`
- **Week 2 (days 8-14)**: `week_2_september_2025`
- **Week 3 (days 15-21)**: `week_3_september_2025`
- **Week 4 (days 22-28)**: `week_4_september_2025`
- **Week 5 (days 29-30)**: `week_5_september_2025`

### Step 2: Process Weekly Data

Run the data pipeline to read from PostgreSQL and generate aggregations:

```bash
# Process default table (week_1_september_2025)
python data_pipeline.py

# Or specify a different weekly table
python data_pipeline.py --table week_2_september_2025
```

**What this does:**
1. Downloads PostgreSQL JDBC driver (auto, one-time)
2. Connects to PostgreSQL database
3. Loads data from specified weekly table
4. Cleans and validates data
5. Computes aggregations:
   - Trips per day
   - Revenue per day
   - Peak hours (all 24 hours)
   - Daily average metrics (distance, fare, total amount, passenger count, trip duration)
   - Anomaly detection (trip count deviation, revenue drops, high passenger count)
6. Saves results to `output/{week_name}/` directory with automatic filename generation:
   - Format: `{YYYY}_{MM}_week{N}_{metric}.csv`
   - Example: `2025_09_week1_trips_per_day.csv`
   - Each week has its own folder for easy organization

### Step 3: Send Weekly Report

Generate and send weekly summary report:

```bash
# Dry run (preview only)
python send_weekly_report.py --week "week_1_september_2025" --dry-run

# Send to Discord only
python send_weekly_report.py --week "week_1_september_2025" --discord-only

# Send to Gmail only
python send_weekly_report.py --week "week_1_september_2025" --gmail-only

# Send to both Discord and Gmail
python send_weekly_report.py --week "week_1_september_2025"
```

## Directory Structure

```
Capstone 1/
├── data/
│   ├── raw/                      # Raw parquet files
│   │   ├── green_tripdata_2025-09.parquet
│   │   └── yellow_tripdata_2025-09.parquet
│   └── processed/
│       ├── daily/                # Daily processed data (single files)
│       │   ├── taxi_data_2025-09-01.parquet
│       │   ├── taxi_data_2025-09-02.parquet
│       │   └── ...
│       └── weekly/               # Weekly combined data (single files)
│           ├── week_1_september_2025.parquet
│           ├── week_2_september_2025.parquet
│           └── ...
│
├── output/                       # Aggregation results (from data_pipeline.py)
│   ├── week_1_september_2025/   # Week-specific folder
│   │   ├── 2025_09_week1_trips_per_day.csv
│   │   ├── 2025_09_week1_revenue_per_day.csv
│   │   ├── 2025_09_week1_peak_hour_per_day.csv
│   │   ├── 2025_09_week1_daily_avg_metrics.csv
│   │   └── 2025_09_week1_anomaly_monitoring.csv
│   └── week_2_september_2025/   # Another week
│       ├── 2025_09_week2_trips_per_day.csv
│       └── ...
│
├── jdbc/                         # PostgreSQL JDBC driver (auto-downloaded)
│   └── postgresql-42.6.0.jar
│
├── data_extraction.py            # Extract and load to PostgreSQL
├── data_pipeline.py              # Process data from PostgreSQL
├── send_weekly_report.py         # Send weekly reports
├── setup_postgres.sh             # PostgreSQL Docker setup
├── .env                          # Environment variables (DO NOT COMMIT)
└── .env.example                  # Environment variables template
```

## PostgreSQL Database

### Connection Details
```
Host: localhost
Port: 5432
Database: nyc_taxi_db
User: postgres
Password: postgres
```

### Table Schema

Each weekly table has the following columns (normalized from Green and Yellow taxi data):

```sql
-- Example: week_1_september_2025 table
CREATE TABLE week_1_september_2025 (
    VendorID BIGINT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    RatecodeID DOUBLE PRECISION,
    store_and_fwd_flag STRING,
    PULocationID BIGINT,
    DOLocationID BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    Airport_fee DOUBLE PRECISION,
    taxi_type STRING
);
```

### Querying PostgreSQL

```sql
-- View all weekly tables
SELECT tablename FROM pg_tables WHERE schemaname = 'public';

-- Count rows in a weekly table
SELECT COUNT(*) FROM week_1_september_2025;

-- View sample data
SELECT * FROM week_1_september_2025 LIMIT 10;

-- Get taxi type breakdown
SELECT taxi_type, COUNT(*) as trip_count
FROM week_1_september_2025
GROUP BY taxi_type;

-- Get daily trip counts
SELECT DATE(pickup_datetime) as date, COUNT(*) as trips
FROM week_1_september_2025
GROUP BY DATE(pickup_datetime)
ORDER BY date;
```

## Output File Organization

### Automatic Filename Generation

All CSV outputs follow a standardized naming convention for easy identification and organization:

**Filename Format**: `{YYYY}_{MM}_week{N}_{metric}.csv`

**Examples**:
- `2025_09_week1_trips_per_day.csv` - Week 1 of September 2025
- `2025_09_week2_revenue_per_day.csv` - Week 2 of September 2025
- `2025_01_week1_anomaly_monitoring.csv` - Week 1 of January 2025

**Benefits**:
1. **Chronological sorting**: Files automatically sort by year and month
2. **Easy searching**: Quickly find files by year, month, or week number
3. **Clear identification**: Week and metric are immediately visible
4. **Consistent format**: All files follow the same pattern

### Folder Structure

Each week's data is organized in its own folder:

```
output/
├── week_1_september_2025/
│   ├── 2025_09_week1_trips_per_day.csv
│   ├── 2025_09_week1_revenue_per_day.csv
│   ├── 2025_09_week1_peak_hour_per_day.csv
│   ├── 2025_09_week1_daily_avg_metrics.csv
│   └── 2025_09_week1_anomaly_monitoring.csv
├── week_2_september_2025/
│   ├── 2025_09_week2_trips_per_day.csv
│   └── ...
└── week_1_january_2025/
    ├── 2025_01_week1_trips_per_day.csv
    └── ...
```

## Output CSV Schemas

**Date Format Standard**: All CSV files use the standardized date format `YYYY-MM-DD` (ISO 8601) for consistency and easy sorting.

### 1. trips_per_day.csv
**Filename**: `{YYYY}_{MM}_week{N}_trips_per_day.csv`
```csv
date,taxi_type,total_trips
2025-09-01,Green,15234
2025-09-01,Yellow,48567
```

### 2. revenue_per_day.csv
**Filename**: `{YYYY}_{MM}_week{N}_revenue_per_day.csv`
```csv
date,taxi_type,total_revenue_per_day
2025-09-01,Green,234567.89
2025-09-01,Yellow,876543.21
```

### 3. peak_hour_per_day.csv
**Filename**: `{YYYY}_{MM}_week{N}_peak_hour_per_day.csv`
```csv
date,taxi_type,pickup_hour,trips_per_hour
2025-09-01,Green,0,234
2025-09-01,Green,1,156
...
```

### 4. daily_avg_metrics.csv
**Filename**: `{YYYY}_{MM}_week{N}_daily_avg_metrics.csv`
```csv
date,taxi_type,avg_trip_distance,avg_fare_amount,avg_total_amount,avg_passenger_count,avg_trip_duration_minutes
2025-09-01,Green,3.45,12.34,15.67,1.45,18.23
```

### 5. anomaly_monitoring.csv
**Filename**: `{YYYY}_{MM}_week{N}_anomaly_monitoring.csv`
```csv
date,taxi_type,daily_trips,total_revenue_per_day,avg_passenger_count,avg_trips,stddev_trips,pct_change_trips,is_anomaly
2025-09-01,Green,15234,234567.89,1.45,14500.00,1234.56,5.06,False
2025-09-02,Green,12000,189000.00,1.38,14500.00,1234.56,-21.23,True
2025-09-03,Green,14800,228000.00,2.35,14500.00,1234.56,2.07,True
```

**Anomaly Detection Conditions:**
An anomaly is flagged (`is_anomaly=True`) when **any** of the following conditions are met:
1. **Trip count deviation**: Daily trips exceed ±2 standard deviations from the weekly average
2. **Revenue drop**: Revenue decreases by more than 20% compared to the previous day
3. **High passenger count**: Average passenger count exceeds 2 passengers per trip (unusual for typical taxi usage)

## Troubleshooting

### PostgreSQL Connection Issues

**Problem**: `Connection refused` or `FATAL: password authentication failed`

**Solution**:
1. Check PostgreSQL is running: `docker ps`
2. Verify credentials in `.env` file
3. Restart PostgreSQL: `docker restart nyc-taxi-postgres`

### JDBC Driver Issues

**Problem**: `java.lang.ClassNotFoundException: org.postgresql.Driver`

**Solution**: Delete `jdbc/` folder and re-run the pipeline (auto-downloads)

### Empty Table After Weekly Upload

**Problem**: Weekly upload completes but table is empty

**Solution**:
1. Check daily files exist: `ls -lh data/processed/daily/`
2. Verify date range in daily files
3. Re-run weekly mode with verbose output

### Report Generation Fails

**Problem**: `FileNotFoundError` when running send_weekly_report.py

**Solution**:
1. Run data_pipeline.py first to generate CSV files
2. Check `output/{week_name}/` directory contains all 5 CSV files
3. Verify CSV files are not empty
4. Ensure week folder exists (e.g., `output/week_1_september_2025/`)

## Complete Example Workflow

### Method 1: Using Daily-Range Mode (Recommended)

```bash
# Week 1 of September 2025 (Days 1-7)

# Step 1: Process 7 days in one command
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7
# → Processes dates 2025-09-01 through 2025-09-07
# → Saves 7 daily parquet files

# Step 2: Weekly upload (end of week)
python data_extraction.py --mode weekly
# → Creates table: week_1_september_2025

# Step 3: Process data from PostgreSQL
python data_pipeline.py --table week_1_september_2025
# → Creates folder: output/week_1_september_2025/
# → Generates CSV files:
#    - 2025_09_week1_trips_per_day.csv
#    - 2025_09_week1_revenue_per_day.csv
#    - 2025_09_week1_peak_hour_per_day.csv
#    - 2025_09_week1_daily_avg_metrics.csv
#    - 2025_09_week1_anomaly_monitoring.csv

# Step 4: Send weekly report
python send_weekly_report.py --week "week_1_september_2025"
# → Reads CSV files from output/week_1_september_2025/
# → Sends report via Discord and Gmail
```

### Method 2: Using Daily Mode (One Day at a Time)

```bash
# Week 1 of September 2025 (Days 1-7)

# Step 1: Daily extraction (run each day)
python data_extraction.py --mode daily --date 2025-09-01
python data_extraction.py --mode daily --date 2025-09-02
python data_extraction.py --mode daily --date 2025-09-03
python data_extraction.py --mode daily --date 2025-09-04
python data_extraction.py --mode daily --date 2025-09-05
python data_extraction.py --mode daily --date 2025-09-06
python data_extraction.py --mode daily --date 2025-09-07

# Step 2: Weekly upload (end of week)
python data_extraction.py --mode weekly
# → Creates table: week_1_september_2025

# Step 3: Process data from PostgreSQL
python data_pipeline.py --table week_1_september_2025
# → Creates folder: output/week_1_september_2025/
# → Generates CSV files

# Step 4: Send weekly report
python send_weekly_report.py --week "week_1_september_2025"
# → Sends report via Discord and Gmail
```

**Note:** Method 1 (daily-range) is recommended as it's more efficient and requires less manual work.

### Method 3: Using Parquet Output (No PostgreSQL Required)

```bash
# Week 1 of September 2025 (Days 1-7)

# Step 1: Process 7 days in one command
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7
# → Processes dates 2025-09-01 through 2025-09-07
# → Saves 7 daily parquet files

# Step 2: Combine to weekly parquet file (instead of PostgreSQL)
python data_extraction.py --mode weekly --output parquet
# → Creates single file: data/processed/weekly/week_1_september_2025.parquet

# Step 3: Process data from weekly parquet file
# Note: You'll need to modify data_pipeline.py to read from parquet instead of PostgreSQL
# Or upload the parquet to PostgreSQL later if needed
```

**When to use this method:**
- PostgreSQL is not available or not set up
- For backup and archival purposes
- For testing or development without database
- For data portability and sharing

## Environment Variables Reference

```bash
# Discord Configuration
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Gmail Configuration
GMAIL_SENDER_EMAIL=your-email@gmail.com
GMAIL_SENDER_PASSWORD=your-app-password
GMAIL_RECIPIENT_EMAILS=recipient1@gmail.com,recipient2@gmail.com
GMAIL_SMTP_SERVER=smtp.gmail.com
GMAIL_SMTP_PORT=587

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Notes

1. **Daily Processing**: Must be run daily to capture incremental data
2. **Weekly Upload**: Automatically names table based on first date in daily files
3. **Data Persistence**: PostgreSQL data persists in Docker volume
4. **Cleanup**: Remove daily files after successful weekly upload if needed
5. **Automation**: Can be scheduled with cron or systemd timers (documentation available)
