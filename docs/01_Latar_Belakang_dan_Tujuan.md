# Latar Belakang dan Tujuan Project
## NYC Taxi Data Pipeline - Capstone 1

---

## 1. Latar Belakang

### 1.1 Konteks Industri

Industri transportasi taksi di New York City merupakan salah satu ekosistem transportasi tersibuk di dunia, dengan jutaan perjalanan yang terjadi setiap bulannya. NYC Taxi and Limousine Commission (TLC) secara rutin mempublikasikan data trip records yang mencakup informasi detail mengenai setiap perjalanan taksi, termasuk waktu pickup/dropoff, jarak tempuh, tarif, dan jumlah penumpang.

Data ini memiliki potensi besar untuk:
- Memahami pola operasional taksi harian dan mingguan
- Mengidentifikasi jam-jam sibuk (peak hours)
- Mendeteksi anomali dalam operasional
- Mengoptimalkan strategi pricing dan fleet management
- Meningkatkan efisiensi operasional

### 1.2 Permasalahan

Namun, data mentah yang dipublikasikan oleh NYC TLC memiliki beberapa tantangan:

1. **Volume Data Besar**: Data dalam format Parquet dengan ukuran puluhan MB per bulan
2. **Dua Jenis Taksi Berbeda**: Green Taxi dan Yellow Taxi memiliki schema yang sedikit berbeda
3. **Kebutuhan Processing Harian**: Data perlu diproses secara kontinyu setiap hari
4. **Kompleksitas Analisis**: Memerlukan aggregasi multi-dimensi (waktu, tipe taksi, metrik)
5. **Distribusi Laporan**: Stakeholder membutuhkan laporan rutin yang mudah diakses
6. **Deteksi Anomali**: Perlu sistem untuk mendeteksi pola tidak normal dalam operasional

### 1.3 Solusi yang Diusulkan

Project ini mengembangkan **production-grade automated data pipeline** yang dapat:
- Mengekstrak dan memproses data harian secara otomatis
- Menyimpan data dalam database relational (PostgreSQL) untuk analisis lanjutan
- Menghasilkan aggregasi mingguan dengan berbagai metrik operasional
- Mendeteksi anomali menggunakan metode statistik
- Mendistribusikan laporan otomatis melalui Discord dan Email
- Berjalan 24/7 menggunakan systemd automation

---

## 2. Tujuan Project

### 2.1 Tujuan Umum

Membangun **sistem data pipeline end-to-end** yang dapat mengotomatisasi proses ekstraksi, transformasi, loading (ETL), analisis, dan pelaporan data NYC Taxi secara real-time dengan arsitektur yang scalable dan maintainable.

### 2.2 Tujuan Khusus

#### A. Data Engineering
1. **Ekstraksi Data Otomatis**
   - Memproses data harian dari raw Parquet files
   - Menormalisasi perbedaan schema antara Green dan Yellow Taxi
   - Mengombinasikan kedua tipe taksi menjadi unified dataset
   - Menyimpan hasil processing dalam format Parquet dan PostgreSQL

2. **Optimasi Performance**
   - Implementasi mode `daily-range` untuk processing batch yang lebih efisien
   - Minimalisasi I/O operations dengan single-read strategy
   - Menggunakan PySpark untuk distributed processing

3. **Data Quality**
   - Validasi data integrity
   - Cleaning data invalid (negative values, null critical fields)
   - Kalkulasi derived metrics (trip duration)

#### B. Data Analysis
1. **Aggregasi Multi-Dimensi**
   - **Trips per Day**: Total trips berdasarkan tanggal dan tipe taksi
   - **Revenue per Day**: Total pendapatan harian
   - **Peak Hours**: Identifikasi jam-jam sibuk (24-hour analysis)
   - **Daily Average Metrics**: Rata-rata jarak, tarif, durasi, passenger count
   - **Anomaly Detection**: Deteksi outlier statistik

2. **Time-Series Analysis**
   - Weekly aggregations
   - Daily trend monitoring
   - Hour-by-hour patterns

#### C. Automation & Operations
1. **Scheduled Automation**
   - Daily pipeline execution (02:00 AM)
   - Weekly report distribution (Sunday 18:00)
   - Log rotation dan cleanup otomatis

2. **Multi-Channel Reporting**
   - Discord notifications dengan user mentions
   - Email reports dengan CSV attachments
   - Auto-split untuk message length limits

3. **Monitoring & Logging**
   - Comprehensive logging system
   - Error tracking dan debugging
   - 30-day log retention

#### D. Software Engineering Best Practices
1. **Clean Architecture**
   - Object-Oriented Programming (OOP)
   - Design Patterns (Strategy, Builder)
   - SOLID principles
   - Dependency Injection

2. **Code Quality**
   - Modular design
   - Reusable components
   - Clear separation of concerns
   - Extensive documentation

3. **Production Readiness**
   - Error handling
   - Environment configuration via .env
   - Docker containerization untuk database
   - Systemd integration untuk automation

---

## 3. Sasaran (Target Outcomes)

### 3.1 Deliverables

1. **Python Scripts**
   - [data_extraction.py](../data_extraction.py): ETL script dengan 3 modes (daily, daily-range, weekly)
   - [data_pipeline.py](../data_pipeline.py): OOP-based aggregation engine
   - [send_weekly_report.py](../send_weekly_report.py): Multi-channel reporting system
   - [config.py](../config.py): Centralized configuration management

2. **Automation Scripts**
   - [scripts/daily_data_pipeline.sh](../scripts/daily_data_pipeline.sh): Daily execution wrapper
   - [scripts/weekly_report.sh](../scripts/weekly_report.sh): Weekly report sender
   - [scripts/setup_automation.sh](../scripts/setup_automation.sh): Systemd setup
   - [scripts/setup_postgres.sh](../scripts/setup_postgres.sh): Database initialization

3. **Data Outputs**
   - Daily Parquet files: `data/processed/daily/taxi_data_YYYY-MM-DD.parquet`
   - Weekly Parquet files: `data/processed/weekly/week_N_month_year.parquet`
   - PostgreSQL tables: Week-based naming scheme
   - CSV aggregations: 5 files per week in organized folders

4. **Reports**
   - Discord weekly summaries
   - Email reports dengan 5 CSV attachments
   - Comprehensive logs

### 3.2 Metrics Keberhasilan

1. **Performance**
   - Daily processing time: < 5 menit per day
   - Daily-range mode: ~85% faster dibanding sequential processing
   - Database upload: Successfully stores millions of records

2. **Reliability**
   - Automation uptime: 99%+
   - Error rate: < 1%
   - Successful report delivery: 100%

3. **Data Quality**
   - Data completeness: All 7 days processed per week
   - Validation pass rate: > 95%
   - Anomaly detection: Identifies statistical outliers (±2σ)

4. **Usability**
   - Reports delivered within 1 hour of scheduled time
   - Clear, actionable insights dalam laporan
   - Easy to configure dan maintain

---

## 4. Manfaat Project

### 4.1 Manfaat Teknis

1. **Automated Data Processing**: Menghilangkan manual work untuk data extraction dan transformation
2. **Scalable Architecture**: Dapat di-scale untuk data volume yang lebih besar
3. **Reusable Components**: Kode modular dapat digunakan untuk project serupa
4. **Production-Ready**: Siap deploy untuk real-world scenarios

### 4.2 Manfaat Bisnis (Use Cases)

1. **Operations Management**
   - Monitor daily trip counts dan revenue
   - Identify operational anomalies
   - Optimize fleet allocation

2. **Strategic Planning**
   - Understand peak hour patterns
   - Plan resource allocation
   - Forecast demand trends

3. **Quality Monitoring**
   - Track average trip metrics
   - Identify service quality issues
   - Monitor customer satisfaction indicators (passenger count)

4. **Stakeholder Communication**
   - Automated weekly reports
   - Data-driven insights
   - Transparent operations

### 4.3 Manfaat Pembelajaran

1. **Data Engineering**: Hands-on experience dengan production pipeline
2. **Big Data Tools**: PySpark, PostgreSQL, Docker
3. **Software Architecture**: OOP, Design Patterns, SOLID
4. **DevOps**: Automation, systemd, logging, monitoring
5. **Communication**: Multi-channel reporting, stakeholder management

---

## 5. Scope dan Batasan

### 5.1 In-Scope

- NYC Green dan Yellow Taxi data (September 2025)
- Daily dan weekly processing
- 5 jenis aggregasi spesifik
- PostgreSQL database storage
- Discord dan Gmail reporting
- Systemd automation
- Anomaly detection (statistical methods)

### 5.2 Out-of-Scope

- Real-time streaming data
- Machine learning predictions
- Interactive dashboards
- API endpoints
- Cloud deployment (AWS, GCP, Azure)
- For-Hire Vehicle (FHV) data
- Historical data (pre-2025)
- Advanced anomaly detection (ML-based)

### 5.3 Asumsi

- Data source: NYC TLC published Parquet files
- Processing environment: Unix-like OS dengan systemd support
- Database: PostgreSQL dalam Docker container
- Python version: 3.7+
- PySpark availability
- Stable internet untuk Discord/Email notifications

---

## 6. Stakeholders

### 6.1 Primary Stakeholder
- **Project Owner**: Budi Triatmojo (Capstone 1)
- **Target Audience**: Data Engineers, Analysts, Operations Managers

### 6.2 Report Recipients
- Discord: @samsudinde
- Gmail: Multiple configured email addresses

### 6.3 Technical Environment
- Development: macOS (Darwin 25.1.0)
- Database: PostgreSQL 12+ (Docker)
- Automation: systemd
- Version Control: Git (branch: budi_capstone1)

---

## 7. Timeline Overview

**Development Phase**: Completed
- Data extraction module: ✓
- Data pipeline module: ✓
- Reporting module: ✓
- Automation setup: ✓

**Current Phase**: Production Deployment
- Daily pipeline running at 02:00 AM
- Weekly reports sent every Sunday 18:00
- Monitoring dan maintenance ongoing

**Future Enhancements**: (Lihat [Kesimpulan & Saran](06_Kesimpulan_dan_Saran.md))
- ML-based anomaly detection
- Interactive dashboard
- Cloud migration
- Real-time processing

---

**Document Version**: 1.0
**Last Updated**: 2025-11-16
**Author**: Budi Triatmojo
**Project**: NYC Taxi Data Pipeline - Capstone 1
