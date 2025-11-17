# 🚀 Quick Start Guide - NYC Taxi Data Pipeline

## Overview
This guide will help you run the NYC Taxi Data Pipeline from start to finish.

## Prerequisites

### 1. System Requirements
- **Python**: 3.8 or higher (Python 3.12 recommended)
- **PostgreSQL**: 12 or higher
- **Storage**: At least 500MB free space
- **OS**: macOS, Linux, or Windows

### 2. Check Your Python Version
```bash
python3 --version
# Should show: Python 3.12.x or similar
```

## 📦 Step 1: Install Dependencies

### Option A: Automatic Installation (Recommended)
```bash
cd "/Users/buditriatmojo/Downloads/Capstone 1"
python3 install_requirements.py
```

This will automatically install:
- PyArrow (for fast Parquet processing)
- Pandas (for data manipulation)
- SQLAlchemy (for database connections)
- psycopg2-binary (PostgreSQL adapter)
- requests (for Discord/Gmail notifications)
- python-dotenv (for environment variables)

### Option B: Manual Installation
```bash
pip3 install -r requirements.txt
```

### Verify Installation
```bash
python3 -c "import pyarrow, pandas, sqlalchemy; print('✓ All packages installed!')"
```

## 🗄️ Step 2: Setup PostgreSQL Database

### Start PostgreSQL (if not running)
```bash
# macOS (via Homebrew)
brew services start postgresql

# Or manually
postgres -D /usr/local/var/postgres

# Check if running
psql --version
```

### Create Database
```bash
# Method 1: Using provided script
chmod +x setup_postgres.sh
./setup_postgres.sh

# Method 2: Manual creation
psql postgres
CREATE DATABASE nyc_taxi_db;
CREATE USER postgres WITH PASSWORD 'postgres';
GRANT ALL PRIVILEGES ON DATABASE nyc_taxi_db TO postgres;
\q
```

## ⚙️ Step 3: Configure Environment Variables

### Create .env file (Optional - has defaults)
```bash
cp .env.example .env
```

### Edit .env (if needed)
```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Discord Webhook (optional)
DISCORD_WEBHOOK_URL=your_webhook_url_here

# Gmail SMTP (optional)
GMAIL_SENDER_EMAIL=your_email@gmail.com
GMAIL_SENDER_PASSWORD=your_app_password_here
GMAIL_RECIPIENT_EMAILS=recipient1@email.com,recipient2@email.com
```

## 📊 Step 4: Run the Pipeline

### Pipeline Workflow
```
Raw Data (Parquet) → Data Extraction → PostgreSQL → Data Pipeline → CSV Reports → Send Reports
```

### 4.1 View Raw Data (Optional)
```bash
python3 view_parquet.py
```
This displays sample data from the raw Parquet files.

### 4.2 Extract Data for 7 Days
```bash
# Process 7 consecutive days (Sept 1-7, 2025)
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7
```

**What this does:**
- Reads Green and Yellow taxi Parquet files
- Filters data for each date
- Saves daily files to `data/processed/daily/`
- Takes ~30 seconds

### 4.3 Upload to PostgreSQL
```bash
python3 data_extraction.py --mode weekly
```

**What this does:**
- Combines all daily files
- Creates table `week_1_september_2025` in PostgreSQL
- Uploads ~900,000+ rows
- Takes ~1-2 minutes

### 4.4 Generate Aggregated Reports
```bash
python3 data_pipeline.py --table week_1_september_2025
```

**What this does:**
- Loads data from PostgreSQL
- Cleans invalid records
- Computes 5 aggregations:
  1. Trips per day
  2. Revenue per day
  3. Peak hour analysis
  4. Daily average metrics
  5. Anomaly monitoring
- Saves 5 CSV files to `output/week_1_september_2025/`
- Takes ~5 seconds

### 4.5 Send Reports (Optional)

#### Preview Report (Dry Run)
```bash
python3 send_weekly_report.py --week week_1_september_2025 --dry-run
```

#### Send to Discord Only
```bash
python3 send_weekly_report.py --week week_1_september_2025 --discord-only
```

#### Send to Gmail Only
```bash
python3 send_weekly_report.py --week week_1_september_2025 --gmail-only
```

#### Send to Both Discord & Gmail
```bash
python3 send_weekly_report.py --week week_1_september_2025
```

## 🎯 Complete End-to-End Example

```bash
# Navigate to project directory
cd "/Users/buditriatmojo/Downloads/Capstone 1"

# 1. Install dependencies (first time only)
python3 install_requirements.py

# 2. Process 7 days of data
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# 3. Upload to PostgreSQL
python3 data_extraction.py --mode weekly

# 4. Generate CSV reports
python3 data_pipeline.py --table week_1_september_2025

# 5. Preview report
python3 send_weekly_report.py --week week_1_september_2025 --dry-run

# 6. Send report (if configured)
python3 send_weekly_report.py --week week_1_september_2025
```

## 📁 Output Files

### After Extraction
```
data/processed/daily/
├── taxi_data_2025-09-01.parquet
├── taxi_data_2025-09-02.parquet
├── taxi_data_2025-09-03.parquet
├── taxi_data_2025-09-04.parquet
├── taxi_data_2025-09-05.parquet
├── taxi_data_2025-09-06.parquet
└── taxi_data_2025-09-07.parquet
```

### After Pipeline
```
output/week_1_september_2025/
├── 2025_09_week1_trips_per_day.csv
├── 2025_09_week1_revenue_per_day.csv
├── 2025_09_week1_peak_hour_per_day.csv
├── 2025_09_week1_daily_avg_metrics.csv
└── 2025_09_week1_anomaly_monitoring.csv
```

## 🔍 Verify Results

### Check PostgreSQL Table
```bash
psql nyc_taxi_db
\dt  # List tables (should show week_1_september_2025)
SELECT COUNT(*) FROM week_1_september_2025;  # Should show ~900,000+ rows
\q
```

### View CSV Files
```bash
# View trips per day
cat output/week_1_september_2025/2025_09_week1_trips_per_day.csv

# Or open in Excel/Numbers
open output/week_1_september_2025/2025_09_week1_trips_per_day.csv
```

## 🛠️ Common Commands

### Process Different Date Ranges
```bash
# Single day only
python3 data_extraction.py --mode daily --date 2025-09-01

# Different week (days 8-14)
python3 data_extraction.py --mode daily-range --date 2025-09-08 --days 7
python3 data_extraction.py --mode weekly  # Creates week_2_september_2025
python3 data_pipeline.py --table week_2_september_2025
```

### Save Weekly Data to Parquet (instead of PostgreSQL)
```bash
python3 data_extraction.py --mode weekly --output parquet
# Saves to: data/processed/weekly/week_1_september_2025.parquet
```

### Process Multiple Weeks
```bash
# Week 1 (Sept 1-7)
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7
python3 data_extraction.py --mode weekly
python3 data_pipeline.py --table week_1_september_2025

# Week 2 (Sept 8-14)
python3 data_extraction.py --mode daily-range --date 2025-09-08 --days 7
python3 data_extraction.py --mode weekly
python3 data_pipeline.py --table week_2_september_2025

# Week 3 (Sept 15-21)
python3 data_extraction.py --mode daily-range --date 2025-09-15 --days 7
python3 data_extraction.py --mode weekly
python3 data_pipeline.py --table week_3_september_2025
```

## ⏱️ Performance Benchmarks

| Task | Time | Output |
|------|------|--------|
| Install dependencies | 30-60s | Packages installed |
| Extract 7 days | 30-45s | 7 daily Parquet files (~17MB) |
| Upload to PostgreSQL | 1-2 min | 1 table (~900K rows) |
| Generate reports | 4-6s | 5 CSV files |
| Send Discord/Gmail | 2-5s | Notifications sent |
| **Total Pipeline** | **~3 minutes** | **Complete weekly analysis** |

## 📊 Expected Results

### Sample Output - Trips Per Day
```csv
date,taxi_type,total_trips
2025-09-01,Green,1228
2025-09-01,Yellow,65622
2025-09-02,Green,1474
2025-09-02,Yellow,90821
```

### Sample Output - Revenue Per Day
```csv
date,taxi_type,total_revenue_per_day
2025-09-01,Green,38533.91
2025-09-01,Yellow,2253843.65
2025-09-02,Green,44331.51
2025-09-02,Yellow,3181096.78
```

## 🐛 Troubleshooting

### Issue: "Table not found"
**Solution:** Run extraction first
```bash
python3 data_extraction.py --mode weekly
```

### Issue: "PostgreSQL connection failed"
**Solution:** Check if PostgreSQL is running
```bash
brew services list  # macOS
# Or
pg_isready
```

### Issue: "Import 'sqlalchemy' could not be resolved"
**Solution:** IDE configuration issue, scripts still work. Run:
```bash
python3 -c "import sqlalchemy; print('✓ Works fine')"
```

### Issue: "No daily files found"
**Solution:** Process daily data first
```bash
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7
```

### Issue: "Discord/Gmail sending failed"
**Solution:** Configure credentials in `.env` or use `--dry-run`
```bash
python3 send_weekly_report.py --week week_1_september_2025 --dry-run
```

## 📚 Next Steps

### Analyze Your Data
- Open CSV files in Excel, Google Sheets, or Tableau
- Use `anomaly_monitoring.csv` to identify unusual patterns
- Compare `peak_hour_per_day.csv` for fleet optimization

### Automate with Cron/systemd
See [AUTOMATION_SETUP.md](AUTOMATION_SETUP.md) for scheduling

### Customize Reports
Edit [send_weekly_report.py](send_weekly_report.py) to modify message format

## 📞 Support

- **Documentation**: Check `docs/` folder
- **Migration Guide**: See [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md)
- **Report Changes**: See [REPORT_IMPROVEMENTS.md](REPORT_IMPROVEMENTS.md)

## ✅ Quick Health Check

Run this to verify everything is working:
```bash
# Test all imports
python3 -c "import pyarrow, pandas, sqlalchemy, psycopg2, requests; print('✓ Dependencies OK')"

# Test PostgreSQL connection
psql -d nyc_taxi_db -c "SELECT 1" > /dev/null 2>&1 && echo "✓ PostgreSQL OK" || echo "✗ PostgreSQL not accessible"

# Test data files
ls -lh data/raw/*.parquet && echo "✓ Raw data OK" || echo "✗ Raw data not found"

# All good? Run the pipeline!
echo "🚀 Ready to run the pipeline!"
```

---

**That's it!** You're now ready to run the NYC Taxi Data Pipeline. Start with the "Complete End-to-End Example" above for the full workflow. 🎉
