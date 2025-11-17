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

1. **🐍 Python 3.8+** (Python 3.12 recommended)

2. **📦 Install Dependencies** (Automatic):
   ```bash
   python3 install_requirements.py
   ```
   Or manually:
   ```bash
   pip3 install -r requirements.txt
   ```

3. **🐘 PostgreSQL** (optional, for database mode):
   ```bash
   ./setup_postgres.sh
   ```

4. **⚙️ Environment Setup** (optional):
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

### 📋 Comprehensive Logging

All scripts now include detailed logging to track operations and troubleshoot issues:

```bash
logs/
├── data_extraction_YYYY-MM-DD.log    # Extraction process logs
├── data_pipeline_YYYY-MM-DD.log      # Aggregation process logs
└── weekly_report_YYYY-MM-DD.log      # Reporting process logs
```

**Features:**
- 📝 Timestamped log entries for all operations
- 🔍 Detailed error messages with stack traces
- 📊 Performance metrics (execution time, row counts)
- 🎯 Easy debugging with rotating log files

**View logs:**
```bash
# View latest extraction log
tail -f logs/data_extraction_$(date +%Y-%m-%d).log

# Search for errors
grep "ERROR" logs/*.log

# View all logs from today
ls -lh logs/*$(date +%Y-%m-%d).log
```

### ⏰ Automation with systemd

Automate daily pipeline runs and weekly reports using systemd timers (Linux/macOS):

**Services included:**
- 🔄 **Daily Pipeline** - Runs every day at 2:00 AM
- 📧 **Weekly Report** - Sends reports every Monday at 9:00 AM

**Setup automation:**
```bash
# See detailed instructions
cat documentation/AUTOMATION_SETUP.md

# Quick install (Linux)
sudo cp systemd/*.service /etc/systemd/system/
sudo cp systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nyc-taxi-daily-pipeline.timer
sudo systemctl enable nyc-taxi-weekly-report.timer
sudo systemctl start nyc-taxi-daily-pipeline.timer
sudo systemctl start nyc-taxi-weekly-report.timer
```

**Check automation status:**
```bash
# View timer status
systemctl list-timers nyc-taxi-*

# View service logs
journalctl -u nyc-taxi-daily-pipeline.service -f
```

## 📚 Documentation

### 🚀 Getting Started
- **[⚡ Quick Start Guide](QUICK_START.md)** - Complete setup and installation
- **[🏃 Run Pipeline](RUN_PIPELINE.md)** - Quick reference commands
- **[📖 Complete Workflow Guide](documentation/POSTGRESQL_WORKFLOW.md)** - Step-by-step instructions
- **[⚙️ Automation Setup](documentation/AUTOMATION_SETUP.md)** - systemd scheduling

### 📊 Project Documentation
- **[📝 Latar Belakang dan Tujuan](docs/01_Latar_Belakang_dan_Tujuan.md)** - Background and objectives
- **[📈 Hasil dan Pembahasan](docs/02_Hasil_dan_Pembahasan.md)** - Results and analysis
- **[🔄 Diagram dan Alur Project](docs/03_Diagram_dan_Alur_Project.md)** - Workflow diagrams
- **[💻 Penjelasan Kode](docs/04_Penjelasan_Kode.md)** - Code explanation
- **[📸 Output dan Screenshots](docs/05_Output_dan_Screenshots.md)** - Visual outputs
- **[🎯 Kesimpulan dan Saran](docs/06_Kesimpulan_dan_Saran.md)** - Conclusions and recommendations
- **[🔍 View Parquet Analysis](docs/07_View_Parquet_Analysis.md)** - Data exploration guide

### 🔧 Technical Documentation
- **[✨ Features Summary](documentation/FEATURES_SUMMARY.md)** - Detailed feature documentation
- **[📋 Weekly Table Examples](documentation/WEEKLY_TABLE_EXAMPLES.md)** - Database schema examples
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
├── logs/                         # 📋 Application logs
├── docs/                         # 📚 Project documentation (Indonesian)
├── documentation/                # 📖 Technical documentation (English)
├── systemd/                      # ⏰ Automation service files
│   ├── nyc-taxi-daily-pipeline.service
│   ├── nyc-taxi-daily-pipeline.timer
│   ├── nyc-taxi-weekly-report.service
│   └── nyc-taxi-weekly-report.timer
├── data_extraction.py            # 🔧 Main extraction pipeline
├── data_pipeline.py              # ⚙️ Aggregation engine
├── send_weekly_report.py         # 📧 Reporting system
├── view_parquet.py               # 👁️ Data viewer
├── install_requirements.py       # 📦 Dependency installer
├── requirements.txt              # 📋 Python dependencies
├── setup_postgres.sh             # 🐘 Database setup
├── QUICK_START.md                # ⚡ Quick start guide
└── RUN_PIPELINE.md               # 🏃 Command reference
```

## 🎉 Key Features

### ✨ Version 1.10
- **📥 Automated data extraction** - Download and process data from NYC TLC
- **🔄 Flexible processing modes** - Daily, Daily-Range, and Weekly
- **💾 Dual storage options** - PostgreSQL and Parquet
- **📊 Automated aggregations** - Trips, revenue, peak hours, daily averages
- **🔍 Anomaly detection** - Automatically identify unusual patterns
- **📧 Multi-channel reporting** - Discord and Gmail with CSV attachments
- **📁 Structured file organization** - Week-specific folders with consistent naming
- **📋 Comprehensive logging** - Detailed logs for all operations in `logs/` directory
- **⏰ Automation support** - systemd service files for scheduled execution
- **📦 Easy installation** - Automated dependency installer

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

- 🐍 Python 3.8+ (Python 3.12 recommended)
- 🏹 PyArrow (for Parquet file handling)
- 🐼 Pandas 1.0+
- 🗄️ SQLAlchemy + psycopg2-binary (for PostgreSQL)
- 🌐 Requests 2.0+
- 📧 python-dotenv (for environment variables)
- 🐘 PostgreSQL 12+ (optional, for database mode)

**Note:** All Python dependencies can be installed automatically:
```bash
python3 install_requirements.py
```

## 📜 License

This project is part of a capstone project for educational purposes. 🎓

## 💬 Support

For issues or questions:
1. ⚡ Start with [Quick Start Guide](QUICK_START.md)
2. 🏃 Check [Run Pipeline](RUN_PIPELINE.md) for command reference
3. 📖 Review [Complete Workflow Guide](documentation/POSTGRESQL_WORKFLOW.md)
4. ⏰ See [Automation Setup](documentation/AUTOMATION_SETUP.md) for scheduling
5. ✨ Browse [Features Summary](documentation/FEATURES_SUMMARY.md)
6. 📋 Check logs in `logs/` directory for detailed error messages

## 🤝 Contributing

This is a capstone project. For suggestions or improvements, please document them in the issues section.

## 🆕 Recent Updates

### Version 1.10 (November 2025)
- ✅ Added comprehensive logging system to all scripts
- ✅ Created systemd automation for daily pipeline and weekly reports
- ✅ Enhanced email reporting with CSV attachments
- ✅ Fixed Discord message length limit with auto-split
- ✅ Added detailed aggregation reports
- ✅ Organized documentation into `docs/` and `documentation/` folders
- ✅ Added `install_requirements.py` for easy dependency setup
- ✅ Added `requirements.txt` for package management
- ✅ Created `QUICK_START.md` and `RUN_PIPELINE.md` guides

### Version 1.00 (September 2025)
- Initial release with core pipeline functionality
- PostgreSQL and Parquet storage modes
- Multi-channel reporting (Discord, Gmail)
- Anomaly detection system

---

**📌 Version**: 1.10
**📅 Last Updated**: November 17, 2025
**🚦 Status**: Production Ready
**👨‍💻 Author**: Budi Triatmojo
**📧 Contact**: buditriatmojo01@gmail.com
