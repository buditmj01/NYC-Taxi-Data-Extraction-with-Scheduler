# 🚕 NYC Taxi Data Pipeline

A comprehensive data pipeline for processing, analyzing, and reporting on NYC Taxi trip data (Green and Yellow cabs). 📊

## 🎯 Overview

This pipeline provides a complete solution for:
- **📥 Data Extraction**: Process daily/weekly taxi trip data
- **💾 Data Storage**: PostgreSQL database or Parquet files
- **📈 Data Analysis**: Automated aggregations and metrics
- **🔍 Anomaly Detection**: Statistical outlier identification
- **📧 Reporting**: Multi-channel notifications (Discord, Gmail)

## 🚀 Quick Start

### 📋 Prerequisites

1. **🐘 PostgreSQL** (optional, for database mode):
   ```bash
   ./setup_postgres.sh
   ```

2. **🐍 Python Dependencies**:
   ```bash
   pip install pyspark pandas requests
   ```

3. **⚙️ Environment Setup**:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

### 💻 Basic Usage

```bash
# Process 7 days in one command
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Upload to PostgreSQL
python data_extraction.py --mode weekly

# Generate aggregated reports
python data_pipeline.py --table week_1_september_2025

# Send weekly report
python send_weekly_report.py --week "week_1_september_2025"
```

## ✨ Features

### 🚀 Processing Modes

| Mode | Description | Command |
|------|-------------|---------|
| **📅 Daily** | Process one day | `--mode daily --date 2025-09-01` |
| **📆 Daily-Range** | Process 7+ days efficiently | `--mode daily-range --date 2025-09-01 --days 7` |
| **📊 Weekly** | Combine and store | `--mode weekly [--output postgres\|parquet]` |

### 📊 Automated Aggregations

- **🚖 Trips per day** - Track volume trends
- **💰 Revenue per day** - Monitor earnings
- **⏰ Peak hours** - Identify busy periods (24 hours)
- **📏 Daily averages** - Distance, fare, duration, passenger count
- **🔍 Anomaly detection** - Statistical outlier identification

### 🔍 Anomaly Detection

Automatically flags unusual patterns:
- ⚠️ Trip count deviations (±2σ from average)
- 📉 Revenue drops (>20% decrease)
- 👥 High passenger counts (>2 per trip)

### 📁 File Organization

**🏷️ Automatic filename generation**:
```
2025_09_week1_trips_per_day.csv
2025_09_week1_revenue_per_day.csv
2025_09_week1_anomaly_monitoring.csv
```

**📂 Week-specific folders**:
```
output/
├── week_1_september_2025/
│   ├── 2025_09_week1_trips_per_day.csv
│   └── ...
└── week_2_september_2025/
    └── ...
```

### 📧 Multi-Channel Reporting

Send reports via:
- **💬 Discord** webhooks
- **📮 Gmail** SMTP
- **🔄 Both** simultaneously

```bash
python send_weekly_report.py --week "week_1_september_2025" --dry-run
python send_weekly_report.py --week "week_1_september_2025" --discord-only
python send_weekly_report.py --week "week_1_september_2025" --gmail-only
python send_weekly_report.py --week "week_1_september_2025"  # Both
```

## 📚 Documentation

- **[📖 Complete Workflow Guide](documentation/POSTGRESQL_WORKFLOW.md)** - Step-by-step instructions
- **[✨ Features Summary](documentation/FEATURES_SUMMARY.md)** - Detailed feature documentation
- **[⚙️ Environment Setup](.env.example)** - Configuration template

## 📂 Project Structure

```
NYC Taxi Data Pipeline/
├── data/
│   ├── raw/                      # 📦 Source parquet files
│   └── processed/
│       ├── daily/                # 📅 Daily parquet files
│       └── weekly/               # 📊 Weekly parquet files
├── output/                       # 📄 CSV aggregations (by week)
├── jdbc/                         # 🔌 PostgreSQL JDBC driver
├── documentation/                # 📚 Documentation files
├── data_extraction.py            # 🔧 Main extraction pipeline
├── data_pipeline.py              # ⚙️ Aggregation engine
├── send_weekly_report.py         # 📧 Reporting system
├── view_parquet.py               # 👁️ Data viewer
└── setup_postgres.sh             # 🐘 Database setup
```

## 🎉 Key Features

### ✨ Version 1.00
- **📥 Automated data extraction** - Download and process data from NYC TLC
- **🔄 Flexible processing modes** - Daily, Daily-Range, and Weekly
- **💾 Dual storage options** - PostgreSQL and Parquet
- **📊 Automated aggregations** - Trips, revenue, peak hours, daily averages
- **🔍 Anomaly detection** - Automatically identify unusual patterns
- **📧 Multi-channel reporting** - Discord and Gmail
- **📁 Structured file organization** - Week-specific folders with consistent naming

## 🔄 Workflows

### Workflow 1: Daily-Range + PostgreSQL (Recommended) 🌟

```bash
# Step 1: Process 7 days
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Step 2: Upload to PostgreSQL
python data_extraction.py --mode weekly

# Step 3: Generate reports
python data_pipeline.py --table week_1_september_2025

# Step 4: Send notifications
python send_weekly_report.py --week "week_1_september_2025"
```

### Workflow 2: Parquet-Only (No Database) 💾

```bash
# Step 1: Process 7 days
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Step 2: Create weekly parquet
python data_extraction.py --mode weekly --output parquet

# Result: data/processed/weekly/week_1_september_2025.parquet
```

## ⚙️ Configuration

### 🔐 Environment Variables

Create a `.env` file with your credentials:

```bash
# Discord
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Gmail
GMAIL_SENDER_EMAIL=your-email@gmail.com
GMAIL_SENDER_PASSWORD=your-app-password
GMAIL_RECIPIENT_EMAILS=recipient1@gmail.com,recipient2@gmail.com

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## 📊 Output Examples

### 📄 CSV Aggregations

**Trips per day**:
```csv
date,taxi_type,total_trips
2025-09-01,Green,15234
2025-09-01,Yellow,48567
```

**Anomaly monitoring**:
```csv
date,taxi_type,daily_trips,total_revenue_per_day,avg_passenger_count,is_anomaly
2025-09-01,Green,15234,234567.89,1.45,False
2025-09-02,Green,12000,189000.00,1.38,True
2025-09-03,Green,14800,228000.00,2.35,True
```

## 🔧 Troubleshooting

### 🐘 PostgreSQL Connection Issues
```bash
# Check if PostgreSQL is running
docker ps

# Restart PostgreSQL
docker restart nyc-taxi-postgres
```

### 📄 Missing CSV Files
```bash
# Ensure data_pipeline.py was run first
python data_pipeline.py --table week_1_september_2025

# Check output directory
ls -la output/week_1_september_2025/
```

### 📧 Report Send Failures
```bash
# Test with dry-run first
python send_weekly_report.py --week "week_1_september_2025" --dry-run

# Verify environment variables
cat .env
```

## 💡 Use Cases

1. **📊 Production Monitoring** - Daily processing with automated reporting
2. **💾 Data Archival** - Weekly parquet backups
3. **🧪 Development/Testing** - Parquet-only mode without database
4. **📤 Data Sharing** - Single file parquet exports
5. **⚠️ Anomaly Alerts** - Automated outlier detection

## ⚡ Performance

- **🚀 Daily-range mode**: ~85% faster than individual daily processing
- **📦 Single file parquet**: Faster I/O and simpler management
- **⚙️ Efficient aggregations**: Spark DataFrame operations with parallel processing

## 📋 Requirements

- 🐍 Python 3.7+
- ⚡ PySpark 3.0+
- 🐼 Pandas 1.0+
- 🌐 Requests 2.0+
- 🐘 PostgreSQL 12+ (optional)
- 🐳 Docker (for PostgreSQL setup)

## 📜 License

This project is part of a capstone project for educational purposes. 🎓

## 💬 Support

For issues or questions:
1. 📖 Check the [Complete Workflow Guide](documentation/POSTGRESQL_WORKFLOW.md)
2. ✨ Review [Features Summary](documentation/FEATURES_SUMMARY.md)
3. ⚙️ Verify environment setup in `.env`

## 🤝 Contributing

This is a capstone project. For suggestions or improvements, please document them in the issues section.

---

**📌 Version**: 1.00
**📅 Last Updated**: November 2025
**🚦 Status**: Active Development
