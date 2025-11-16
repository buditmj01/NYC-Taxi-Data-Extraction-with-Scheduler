# NYC Taxi Data Pipeline

Pipeline data komprehensif untuk memproses, menganalisis, dan melaporkan data perjalanan Taksi NYC (Green dan Yellow cabs).

## Gambaran Umum

Pipeline ini menyediakan solusi lengkap untuk:
- **Ekstraksi Data**: Memproses data perjalanan taksi harian/mingguan
- **Penyimpanan Data**: Database PostgreSQL atau file Parquet
- **Analisis Data**: Agregasi dan metrik otomatis
- **Deteksi Anomali**: Identifikasi outlier statistik
- **Pelaporan**: Notifikasi multi-channel (Discord, Gmail)

## Memulai

### Prasyarat

1. **PostgreSQL** (opsional, untuk mode database):
   ```bash
   ./setup_postgres.sh
   ```

2. **Dependensi Python**:
   ```bash
   pip install pyspark pandas requests
   ```

3. **Pengaturan Environment**:
   ```bash
   cp .env.example .env
   # Edit .env dengan kredensial Anda
   ```

### Penggunaan Dasar

```bash
# Memproses 7 hari dalam satu perintah
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Upload ke PostgreSQL
python data_extraction.py --mode weekly

# Generate laporan agregasi
python data_pipeline.py --table week_1_september_2025

# Kirim laporan mingguan
python send_weekly_report.py --week "week_1_september_2025"
```

## Fitur

### 🚀 Mode Pemrosesan

| Mode | Deskripsi | Perintah |
|------|-----------|----------|
| **Daily** | Memproses satu hari | `--mode daily --date 2025-09-01` |
| **Daily-Range** | Memproses 7+ hari secara efisien | `--mode daily-range --date 2025-09-01 --days 7` |
| **Weekly** | Menggabungkan dan menyimpan | `--mode weekly [--output postgres\|parquet]` |

### 📊 Agregasi Otomatis

- **Trips per day** - Melacak tren volume
- **Revenue per day** - Memantau pendapatan
- **Peak hours** - Mengidentifikasi periode sibuk (24 jam)
- **Daily averages** - Jarak, tarif, durasi, jumlah penumpang
- **Anomaly detection** - Identifikasi outlier statistik

### 🔍 Deteksi Anomali

Menandai pola yang tidak biasa secara otomatis:
- Deviasi jumlah trip (±2σ dari rata-rata)
- Penurunan pendapatan (>20% penurunan)
- Jumlah penumpang tinggi (>2 per trip)

### 📁 Organisasi File

**Penamaan file otomatis**:
```
2025_09_week1_trips_per_day.csv
2025_09_week1_revenue_per_day.csv
2025_09_week1_anomaly_monitoring.csv
```

**Folder spesifik per minggu**:
```
output/
├── week_1_september_2025/
│   ├── 2025_09_week1_trips_per_day.csv
│   └── ...
└── week_2_september_2025/
    └── ...
```

### 📧 Pelaporan Multi-Channel

Kirim laporan melalui:
- **Discord** webhooks
- **Gmail** SMTP
- **Keduanya** secara bersamaan

```bash
python send_weekly_report.py --week "week_1_september_2025" --dry-run
python send_weekly_report.py --week "week_1_september_2025" --discord-only
python send_weekly_report.py --week "week_1_september_2025" --gmail-only
python send_weekly_report.py --week "week_1_september_2025"  # Keduanya
```

## Dokumentasi

- **[Panduan Workflow Lengkap](documentation/POSTGRESQL_WORKFLOW.md)** - Instruksi langkah demi langkah
- **[Ringkasan Fitur](documentation/FEATURES_SUMMARY.md)** - Dokumentasi fitur detail
- **[Pengaturan Environment](.env.example)** - Template konfigurasi

## Struktur Proyek

```
Capstone 1/
├── data/
│   ├── raw/                      # Source parquet files
│   └── processed/
│       ├── daily/                # Daily parquet files
│       └── weekly/               # Weekly parquet files
├── output/                       # CSV aggregations (by week)
├── jdbc/                         # PostgreSQL JDBC driver
├── documentation/                # Documentation files
├── data_extraction.py            # Main extraction pipeline
├── data_pipeline.py              # Aggregation engine
├── send_weekly_report.py         # Reporting system
├── view_parquet.py               # Data viewer
└── setup_postgres.sh             # Database setup
```

## Fitur Utama

### ✨ Versi 1.00
- **Ekstraksi data otomatis** - Download dan proses data dari NYC TLC
- **Mode pemrosesan fleksibel** - Daily, Daily-Range, dan Weekly
- **Penyimpanan data ganda** - PostgreSQL dan Parquet
- **Agregasi otomatis** - Trips, revenue, peak hours, daily averages
- **Deteksi anomali** - Identifikasi pola tidak biasa secara otomatis
- **Pelaporan multi-channel** - Discord dan Gmail
- **Organisasi file terstruktur** - Folder per minggu dengan penamaan konsisten

## Alur Kerja

### Workflow 1: Daily-Range + PostgreSQL (Direkomendasikan)

```bash
# Langkah 1: Proses 7 hari
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Langkah 2: Upload ke PostgreSQL
python data_extraction.py --mode weekly

# Langkah 3: Generate laporan
python data_pipeline.py --table week_1_september_2025

# Langkah 4: Kirim notifikasi
python send_weekly_report.py --week "week_1_september_2025"
```

### Workflow 2: Parquet-Only (Tanpa Database)

```bash
# Langkah 1: Proses 7 hari
python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

# Langkah 2: Buat weekly parquet
python data_extraction.py --mode weekly --output parquet

# Hasil: data/processed/weekly/week_1_september_2025.parquet
```

## Konfigurasi

### Variabel Environment

Buat file `.env` dengan kredensial Anda:

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

## Contoh Output

### Agregasi CSV

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

## Troubleshooting

### Masalah Koneksi PostgreSQL
```bash
# Cek apakah PostgreSQL berjalan
docker ps

# Restart PostgreSQL
docker restart nyc-taxi-postgres
```

### File CSV Hilang
```bash
# Pastikan data_pipeline.py sudah dijalankan terlebih dahulu
python data_pipeline.py --table week_1_september_2025

# Cek direktori output
ls -la output/week_1_september_2025/
```

### Gagal Mengirim Laporan
```bash
# Test dengan dry-run terlebih dahulu
python send_weekly_report.py --week "week_1_september_2025" --dry-run

# Verifikasi variabel environment
cat .env
```

## Kasus Penggunaan

1. **Production Monitoring** - Pemrosesan harian dengan pelaporan otomatis
2. **Arsip Data** - Backup parquet mingguan
3. **Development/Testing** - Mode parquet-only tanpa database
4. **Berbagi Data** - Ekspor file parquet tunggal
5. **Alert Anomali** - Deteksi outlier otomatis

## Performa

- **Mode daily-range**: ~85% lebih cepat dari pemrosesan harian individual
- **Single file parquet**: I/O lebih cepat dan manajemen lebih sederhana
- **Agregasi efisien**: Operasi Spark DataFrame dengan pemrosesan paralel

## Persyaratan

- Python 3.7+
- PySpark 3.0+
- Pandas 1.0+
- Requests 2.0+
- PostgreSQL 12+ (opsional)
- Docker (untuk setup PostgreSQL)

## Lisensi

Proyek ini adalah bagian dari proyek capstone untuk tujuan edukasi.

## Dukungan

Untuk masalah atau pertanyaan:
1. Cek [Panduan Workflow Lengkap](documentation/POSTGRESQL_WORKFLOW.md)
2. Review [Ringkasan Fitur](documentation/FEATURES_SUMMARY.md)
3. Verifikasi pengaturan environment di `.env`

## Kontribusi

Ini adalah proyek capstone. Untuk saran atau perbaikan, silakan dokumentasikan di bagian issues.

---

**Versi**: 1.00
**Terakhir Diperbarui**: November 2025
**Status**: Pengembangan Aktif
