# NYC Taxi Data Pipeline - Features Summary

This document provides a comprehensive overview of all features, enhancements, and capabilities of the NYC Taxi Data Pipeline.

## Table of Contents
1. [Core Features](#core-features)
2. [Processing Modes](#processing-modes)
3. [Data Pipeline Features](#data-pipeline-features)
4. [Reporting Features](#reporting-features)
5. [File Format Standards](#file-format-standards)
6. [Key Improvements](#key-improvements)

---

## Core Features

### 1. Multi-Mode Data Extraction

The pipeline supports three flexible processing modes:

#### Daily Mode
- Process a single day's data
- Command: `python data_extraction.py --mode daily --date YYYY-MM-DD`
- Output: Single parquet file per day
- Use case: Incremental daily processing

#### Daily-Range Mode (NEW!)
- Process multiple consecutive days in one command
- Command: `python data_extraction.py --mode daily-range --date YYYY-MM-DD --days N`
- Default: 7 days
- Benefits:
  - More efficient (loads raw data only once)
  - Reduces manual work
  - Perfect for weekly data preparation
  - Customizable day count

#### Weekly Mode
- Combine all daily files into a single dataset
- Two output options:
  1. **PostgreSQL** (default): `python data_extraction.py --mode weekly`
  2. **Parquet file**: `python data_extraction.py --mode weekly --output parquet`
- Automatic week naming based on first date
- Use case: Weekly aggregation and storage

### 2. Flexible Output Formats

#### PostgreSQL Output
- Automatic table creation with week-based naming
- Examples: `week_1_september_2025`, `week_2_september_2025`
- JDBC connectivity
- Persistent storage in Docker volume
- Connection details:
  - Host: localhost
  - Port: 5432
  - Database: nyc_taxi_db
  - User: postgres
  - Password: postgres

#### Parquet Output (NEW!)
- Single file format (not folders)
- Location: `data/processed/weekly/`
- Filename: `{week_name}.parquet`
- Benefits:
  - Easy to share and transfer
  - No database dependency
  - Portable between environments
  - Ideal for backup and archival

### 3. Single File Parquet Format (NEW!)

All parquet files are saved as single files instead of folders:

**Before:**
```
taxi_data_2025-09-01.parquet/
├── part-00000-xxx.parquet
├── _SUCCESS
└── _committed_xxx
```

**After:**
```
taxi_data_2025-09-01.parquet (single file)
```

**Implementation:**
- Uses `coalesce(1)` to combine partitions
- Extracts and renames the part file
- Cleans up temporary folders
- Shows file type confirmation in output

---

## Processing Modes

### Mode Comparison Table

| Mode | Command | Output | Best For |
|------|---------|--------|----------|
| **daily** | `--mode daily --date 2025-09-01` | 1 parquet file | Processing one specific day |
| **daily-range** | `--mode daily-range --date 2025-09-01 --days 7` | 7 parquet files | Processing multiple days efficiently |
| **weekly (postgres)** | `--mode weekly` | PostgreSQL table | Database-backed workflows |
| **weekly (parquet)** | `--mode weekly --output parquet` | 1 combined parquet | Backup, portability, testing |

### Example Workflows

#### Workflow 1: Daily-Range + PostgreSQL (Recommended)
```bash
# Process 7 days in one command
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Upload to PostgreSQL
python data_extraction.py --mode weekly

# Generate reports
python data_pipeline.py --table week_1_september_2025

# Send reports
python send_weekly_report.py --week "week_1_september_2025"
```

#### Workflow 2: Parquet-Only (No Database)
```bash
# Process 7 days
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Create weekly parquet file
python data_extraction.py --mode weekly --output parquet

# Result: data/processed/weekly/week_1_september_2025.parquet
```

---

## Data Pipeline Features

### 1. Comprehensive Data Aggregations

The pipeline computes five key aggregation types:

#### Trips Per Day
- **File**: `{YYYY}_{MM}_week{N}_trips_per_day.csv`
- **Columns**: date, taxi_type, total_trips
- **Purpose**: Track daily trip volume by taxi type

#### Revenue Per Day
- **File**: `{YYYY}_{MM}_week{N}_revenue_per_day.csv`
- **Columns**: date, taxi_type, total_revenue_per_day
- **Purpose**: Monitor daily revenue trends

#### Peak Hours
- **File**: `{YYYY}_{MM}_week{N}_peak_hour_per_day.csv`
- **Columns**: date, taxi_type, pickup_hour, trips_per_hour
- **Purpose**: Identify hourly traffic patterns (all 24 hours)

#### Daily Average Metrics
- **File**: `{YYYY}_{MM}_week{N}_daily_avg_metrics.csv`
- **Columns**:
  - date
  - taxi_type
  - avg_trip_distance
  - avg_fare_amount
  - avg_total_amount
  - avg_passenger_count
  - avg_trip_duration_minutes
- **Purpose**: Track operational efficiency metrics

#### Anomaly Monitoring
- **File**: `{YYYY}_{MM}_week{N}_anomaly_monitoring.csv`
- **Columns**:
  - date
  - taxi_type
  - daily_trips
  - total_revenue_per_day
  - avg_passenger_count
  - avg_trips
  - stddev_trips
  - pct_change_trips
  - is_anomaly
- **Purpose**: Detect unusual patterns and outliers

### 2. Advanced Anomaly Detection

Anomalies are flagged when **ANY** of these conditions are met:

1. **Trip Count Deviation**
   - Daily trips exceed ±2 standard deviations from weekly average
   - Statistical outlier detection

2. **Revenue Drop**
   - Revenue decreases by more than 20% compared to previous day
   - Early warning for business issues

3. **High Passenger Count** (NEW!)
   - Average passenger count exceeds 2 per trip
   - Unusual for typical taxi usage
   - May indicate data quality issues or special events

### 3. Date Format Standardization

**Standard**: ISO 8601 (YYYY-MM-DD)

**Reasons for this format:**
1. International standard
2. Chronological sorting
3. Unambiguous date representation
4. Easy to parse and process
5. Compatible with SQL and analytics tools
6. Human-readable
7. Consistent across all outputs
8. Supports date range queries

**Implementation:**
- Constant: `STANDARD_DATE_FORMAT = "yyyy-MM-dd"`
- Applied to all 5 CSV outputs
- Helper function: `standardize_date_column()`

---

## Reporting Features

### 1. Automatic Filename Generation

All CSV outputs follow a standardized naming convention:

**Format**: `{YYYY}_{MM}_week{N}_{metric}.csv`

**Examples:**
- `2025_09_week1_trips_per_day.csv`
- `2025_09_week2_revenue_per_day.csv`
- `2025_01_week1_anomaly_monitoring.csv`

**Benefits:**
1. **Chronological sorting**: Files automatically sort by year and month
2. **Easy searching**: Quickly find files by year, month, or week number
3. **Clear identification**: Week and metric are immediately visible
4. **Consistent format**: All files follow the same pattern

### 2. Week-Specific Folder Organization

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
```

**Benefits:**
- Clear separation between weeks
- Easy to archive or delete old weeks
- Prevents file name conflicts
- Organized historical data

### 3. Multi-Channel Reporting

Send weekly reports via:
- **Discord**: Webhook-based notifications
- **Gmail**: SMTP email delivery
- **Both**: Simultaneous delivery to multiple channels

**Commands:**
```bash
# Dry run (preview only)
python send_weekly_report.py --week "week_1_september_2025" --dry-run

# Discord only
python send_weekly_report.py --week "week_1_september_2025" --discord-only

# Gmail only
python send_weekly_report.py --week "week_1_september_2025" --gmail-only

# Both channels
python send_weekly_report.py --week "week_1_september_2025"
```

**Report Contents:**
- Total trips and revenue
- Daily averages
- Best and worst days
- Anomaly alerts
- Trend analysis
- Peak hour insights

---

## File Format Standards

### Directory Structure

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
│       └── ...
│
├── jdbc/                         # PostgreSQL JDBC driver (auto-downloaded)
│   └── postgresql-42.6.0.jar
│
├── data_extraction.py            # Extract and load to PostgreSQL
├── data_pipeline.py              # Process data from PostgreSQL
├── send_weekly_report.py         # Send weekly reports
├── view_parquet.py               # View parquet file contents
├── setup_postgres.sh             # PostgreSQL Docker setup
├── .env                          # Environment variables (DO NOT COMMIT)
└── .env.example                  # Environment variables template
```

### Naming Conventions

**Daily Parquet Files:**
- Format: `taxi_data_{YYYY-MM-DD}.parquet`
- Example: `taxi_data_2025-09-01.parquet`
- Type: Single file (not folder)

**Weekly Parquet Files:**
- Format: `{week_name}.parquet`
- Example: `week_1_september_2025.parquet`
- Type: Single file (not folder)

**PostgreSQL Tables:**
- Format: `week_{N}_{month}_{year}`
- Example: `week_1_september_2025`
- Week numbering: 1-5 based on day of month

**CSV Output Files:**
- Format: `{YYYY}_{MM}_week{N}_{metric}.csv`
- Example: `2025_09_week1_trips_per_day.csv`
- Location: `output/{week_name}/`

---

## Key Improvements

### Performance Enhancements

1. **Daily-Range Mode**
   - Loads raw data only once for multiple days
   - Reduces processing time by ~85% for weekly preparation
   - Example: 7 separate commands → 1 command

2. **Single File Parquet**
   - Faster file operations (no folder traversal)
   - Reduced storage overhead
   - Simpler file management

3. **Optimized Aggregations**
   - Uses Spark DataFrame operations
   - Efficient window functions
   - Parallel processing

### Usability Improvements

1. **Flexible Output Options**
   - Choose between PostgreSQL and Parquet
   - No forced database dependency
   - Easy switching between modes

2. **Automatic Naming**
   - Week-based table names
   - Date-prefixed file names
   - No manual naming required

3. **Clear Documentation**
   - Step-by-step workflows
   - Multiple workflow examples
   - Troubleshooting guides

### Data Quality Features

1. **Enhanced Anomaly Detection**
   - Statistical outlier detection
   - Revenue trend monitoring
   - Passenger count validation

2. **Date Standardization**
   - Consistent ISO 8601 format
   - Prevents date parsing errors
   - Compatible with all tools

3. **Data Validation**
   - Column presence checks
   - Required field validation
   - Clear error messages

---

## Technical Specifications

### Dependencies

```bash
pip install pyspark pandas requests
```

### Environment Variables

```bash
# Discord Configuration
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Gmail Configuration
GMAIL_SENDER_EMAIL=your-email@gmail.com
GMAIL_SENDER_PASSWORD=your-app-password
GMAIL_RECIPIENT_EMAILS=recipient1@gmail.com,recipient2@gmail.com

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### Supported Data Sources

- Green Taxi Trip Records
- Yellow Taxi Trip Records
- Both combined with unified schema

### Output Formats

- **Parquet**: Columnar storage, efficient for analytics
- **CSV**: Universal compatibility, human-readable
- **PostgreSQL**: Relational database, SQL queries

---

## Use Cases

### 1. Production Workflow
- Daily-range mode for weekly processing
- PostgreSQL for storage
- Automated reporting via Discord/Gmail

### 2. Backup and Archival
- Weekly parquet output
- Long-term storage
- Easy restoration

### 3. Development and Testing
- Parquet-only mode
- No database setup required
- Portable test data

### 4. Data Sharing
- Single file parquet exports
- Easy to transfer
- Compatible with analytics tools

### 5. Monitoring and Alerts
- Anomaly detection
- Multi-channel notifications
- Trend analysis

---

## Best Practices

1. **Use daily-range mode** for weekly preparation (more efficient)
2. **Keep PostgreSQL** for production workflows
3. **Use parquet output** for backups and testing
4. **Monitor anomalies** regularly
5. **Archive old weeks** to manage storage
6. **Document custom configurations** in `.env` file
7. **Test with dry-run** before sending reports
8. **Review file sizes** to optimize performance

---

## Future Enhancements

Potential areas for expansion:
- Real-time streaming support
- Advanced visualization dashboards
- Machine learning anomaly detection
- Automated scheduling (cron/systemd)
- API endpoints for data access
- Cloud storage integration (S3, GCS)
- Multi-region support

---

## Version History

### Version 1.3 (Current)
- ✅ Single file parquet format
- ✅ Parquet output for weekly mode
- ✅ Enhanced documentation

### Version 1.2
- ✅ Daily-range processing mode
- ✅ Automatic filename generation
- ✅ Week-specific folder organization

### Version 1.1
- ✅ Date standardization
- ✅ Passenger count anomaly detection
- ✅ Multi-channel reporting

### Version 1.0
- ✅ Initial release
- ✅ Daily and weekly processing
- ✅ PostgreSQL integration
- ✅ Basic reporting

---

## Support and Documentation

- Main Documentation: [POSTGRESQL_WORKFLOW.md](POSTGRESQL_WORKFLOW.md)
- Features Summary: This document
- Environment Setup: `.env.example`
- Troubleshooting: See POSTGRESQL_WORKFLOW.md

For questions or issues, refer to the documentation or contact the development team.
