# 🚀 Run Pipeline - Quick Reference

## One-Command Pipeline (Copy & Paste)

```bash
# Navigate to project
cd "/Users/buditriatmojo/Downloads/Capstone 1"

# Run complete pipeline (Week 1: Sept 1-7, 2025)
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7 && \
python3 data_extraction.py --mode weekly && \
python3 data_pipeline.py --table week_1_september_2025 && \
python3 send_weekly_report.py --week week_1_september_2025 --dry-run
```

## Step-by-Step Commands

### 1️⃣ Install (First Time Only)
```bash
python3 install_requirements.py
```

### 2️⃣ Extract Data (7 days)
```bash
python3 data_extraction.py --mode daily-range --date 2025-09-01 --days 7
```

### 3️⃣ Upload to PostgreSQL
```bash
python3 data_extraction.py --mode weekly
```

### 4️⃣ Generate CSV Reports
```bash
python3 data_pipeline.py --table week_1_september_2025
```

### 5️⃣ Send Report (Optional)
```bash
# Preview only
python3 send_weekly_report.py --week week_1_september_2025 --dry-run

# Send to Discord
python3 send_weekly_report.py --week week_1_september_2025 --discord-only

# Send to Gmail
python3 send_weekly_report.py --week week_1_september_2025 --gmail-only

# Send to both
python3 send_weekly_report.py --week week_1_september_2025
```

## Common Variations

### Different Date Range
```bash
# Week 2: Sept 8-14
python3 data_extraction.py --mode daily-range --date 2025-09-08 --days 7
python3 data_extraction.py --mode weekly
python3 data_pipeline.py --table week_2_september_2025
```

### Single Day Only
```bash
python3 data_extraction.py --mode daily --date 2025-09-01
```

### Save to Parquet (not PostgreSQL)
```bash
python3 data_extraction.py --mode weekly --output parquet
```

## Output Locations

```
📂 data/processed/daily/        # Daily Parquet files
📂 output/week_1_september_2025/ # CSV reports
📊 PostgreSQL table             # week_1_september_2025
```

## Quick Checks

```bash
# Check PostgreSQL
psql nyc_taxi_db -c "SELECT COUNT(*) FROM week_1_september_2025;"

# View output
ls -lh output/week_1_september_2025/

# View CSV
cat output/week_1_september_2025/2025_09_week1_trips_per_day.csv
```

## Timing
- Extract 7 days: ~30 seconds
- Upload PostgreSQL: ~2 minutes
- Generate reports: ~5 seconds
- **Total: ~3 minutes**

---
📖 **Need more details?** See [QUICK_START.md](QUICK_START.md) for full guide.
