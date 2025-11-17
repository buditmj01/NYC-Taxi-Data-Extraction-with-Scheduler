# 🤖 NYC Taxi Pipeline - Automation Setup

Panduan lengkap untuk mengatur automasi pipeline data menggunakan **systemd** (Linux).

## 📋 Daftar Isi

- [Fitur Automasi](#-fitur-automasi)
- [Persyaratan](#-persyaratan)
- [Instalasi](#-instalasi)
- [Struktur File](#-struktur-file)
- [Konfigurasi](#-konfigurasi)
- [Penggunaan](#-penggunaan)
- [Monitoring](#-monitoring)
- [Troubleshooting](#-troubleshooting)

## ✨ Fitur Automasi

### 1. **Daily Data Pipeline** 📊
- **Jadwal**: Setiap hari pukul 02:00 AM
- **Fungsi**:
  - Extract data NYC Taxi untuk tanggal saat ini
  - Process dan simpan ke database
  - Setiap hari Minggu: Jalankan agregasi mingguan otomatis
- **Service**: `nyc-taxi-daily-pipeline.service`
- **Timer**: `nyc-taxi-daily-pipeline.timer`

### 2. **Weekly Report** 📧
- **Jadwal**: Setiap hari Minggu pukul 18:00 (6 PM)
- **Fungsi**:
  - Kirim laporan mingguan ke Discord (@samsudinde)
  - Kirim email dengan 5 file CSV attachment
  - Hitung week name otomatis
- **Service**: `nyc-taxi-weekly-report.service`
- **Timer**: `nyc-taxi-weekly-report.timer`

## 📦 Persyaratan

- **OS**: Linux dengan systemd (Ubuntu 16.04+, Debian 8+, CentOS 7+, Fedora, dll)
- **Python**: 3.7+
- **Privileges**: Root/sudo access untuk instalasi
- **Dependencies**: Virtual environment sudah ter-setup di `.venv/`

## 🚀 Instalasi

### Langkah 1: Clone Repository

```bash
cd /path/to/project
```

### Langkah 2: Setup Python Environment

```bash
# Create virtual environment (jika belum ada)
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install pyspark pandas requests
```

### Langkah 3: Configure Environment Variables

```bash
# Copy example env file
cp .env.example .env

# Edit dengan kredensial Anda
nano .env
```

Pastikan isi `.env`:
```bash
# Discord
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

# Gmail
GMAIL_SENDER_EMAIL=your-email@gmail.com
GMAIL_SENDER_PASSWORD=your-app-password
GMAIL_RECIPIENT_EMAILS=email1@gmail.com,email2@gmail.com

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourpassword
```

### Langkah 4: Jalankan Setup Script

```bash
# Make setup script executable
chmod +x scripts/setup_automation.sh

# Run setup (requires sudo)
sudo ./scripts/setup_automation.sh
```

Script akan:
1. ✅ Membuat direktori logs
2. ✅ Membuat shell scripts executable
3. ✅ Install systemd service files
4. ✅ Install systemd timer files
5. ✅ Enable dan start timers
6. ✅ Menampilkan status

## 📁 Struktur File

```
nyc-taxi-pipeline/
├── scripts/
│   ├── daily_data_pipeline.sh      # Script untuk daily pipeline
│   ├── weekly_report.sh            # Script untuk weekly report
│   └── setup_automation.sh         # Setup installation script
├── systemd/
│   ├── nyc-taxi-daily-pipeline.service
│   ├── nyc-taxi-daily-pipeline.timer
│   ├── nyc-taxi-weekly-report.service
│   └── nyc-taxi-weekly-report.timer
├── logs/
│   ├── daily_pipeline_*.log        # Daily pipeline logs
│   └── weekly_report_*.log         # Weekly report logs
└── AUTOMATION_SETUP.md             # This file
```

## ⚙️ Konfigurasi

### Mengubah Jadwal

Edit file timer di `/etc/systemd/system/`:

**Daily Pipeline** (default: 2:00 AM daily):
```bash
sudo nano /etc/systemd/system/nyc-taxi-daily-pipeline.timer
```

```ini
[Timer]
OnCalendar=*-*-* 03:00:00  # Ubah ke 3:00 AM
```

**Weekly Report** (default: Sunday 6:00 PM):
```bash
sudo nano /etc/systemd/system/nyc-taxi-weekly-report.timer
```

```ini
[Timer]
OnCalendar=Sun *-*-* 20:00:00  # Ubah ke 8:00 PM
```

Setelah edit, reload:
```bash
sudo systemctl daemon-reload
sudo systemctl restart nyc-taxi-daily-pipeline.timer
sudo systemctl restart nyc-taxi-weekly-report.timer
```

### Format OnCalendar

```
Format: DayOfWeek Year-Month-Day Hour:Minute:Second

Contoh:
  *-*-* 02:00:00           # Setiap hari jam 2:00 AM
  Mon *-*-* 09:00:00       # Setiap Senin jam 9:00 AM
  Mon,Fri *-*-* 14:00:00   # Senin & Jumat jam 2:00 PM
  *-*-01 00:00:00          # Tanggal 1 setiap bulan
```

## 💻 Penggunaan

### Lihat Status Timer

```bash
# Lihat semua timer
sudo systemctl list-timers

# Lihat timer NYC Taxi saja
sudo systemctl list-timers | grep nyc-taxi
```

Output contoh:
```
NEXT                         LEFT          LAST                         PASSED       UNIT
Sun 2025-11-17 18:00:00 WIB  1 day left    Sun 2025-11-10 18:00:12 WIB  6 days ago   nyc-taxi-weekly-report.timer
Mon 2025-11-11 02:00:00 WIB  14h left      Sun 2025-11-10 02:00:08 WIB  9h ago       nyc-taxi-daily-pipeline.timer
```

### Lihat Status Service

```bash
# Daily pipeline
sudo systemctl status nyc-taxi-daily-pipeline.service

# Weekly report
sudo systemctl status nyc-taxi-weekly-report.service
```

### Jalankan Manual

```bash
# Run daily pipeline sekarang
sudo systemctl start nyc-taxi-daily-pipeline.service

# Run weekly report sekarang
sudo systemctl start nyc-taxi-weekly-report.service

# Run dengan week name spesifik
sudo -u $USER bash /path/to/scripts/weekly_report.sh --week week_1_november_2025
```

### Stop/Start Automation

```bash
# Stop automation
sudo systemctl stop nyc-taxi-daily-pipeline.timer
sudo systemctl stop nyc-taxi-weekly-report.timer

# Start automation
sudo systemctl start nyc-taxi-daily-pipeline.timer
sudo systemctl start nyc-taxi-weekly-report.timer

# Disable (won't start on boot)
sudo systemctl disable nyc-taxi-daily-pipeline.timer
sudo systemctl disable nyc-taxi-weekly-report.timer

# Enable (start on boot)
sudo systemctl enable nyc-taxi-daily-pipeline.timer
sudo systemctl enable nyc-taxi-weekly-report.timer
```

## 📊 Monitoring

### View Logs Real-time

```bash
# Daily pipeline logs (follow mode)
sudo journalctl -u nyc-taxi-daily-pipeline.service -f

# Weekly report logs (follow mode)
sudo journalctl -u nyc-taxi-weekly-report.service -f

# View last 100 lines
sudo journalctl -u nyc-taxi-daily-pipeline.service -n 100

# View logs for specific date
sudo journalctl -u nyc-taxi-daily-pipeline.service --since "2025-11-10" --until "2025-11-11"
```

### View Script Logs

```bash
# List all logs
ls -lh logs/

# View latest daily pipeline log
tail -f logs/daily_pipeline_*.log | tail -n 1

# View latest weekly report log
tail -f logs/weekly_report_*.log | tail -n 1

# Search for errors
grep -i error logs/daily_pipeline_*.log
```

### Check Next Run Time

```bash
# Show when services will run next
systemctl list-timers nyc-taxi-*
```

## 🔧 Troubleshooting

### Service Fails to Start

**Check service status**:
```bash
sudo systemctl status nyc-taxi-daily-pipeline.service
```

**View detailed logs**:
```bash
sudo journalctl -xe -u nyc-taxi-daily-pipeline.service
```

**Common issues**:
- Virtual environment not found → Check `.venv/` exists
- Permission denied → Check file ownership and permissions
- Python modules not found → Activate venv and install dependencies

### Timer Not Running

**Check if timer is active**:
```bash
sudo systemctl is-active nyc-taxi-daily-pipeline.timer
```

**Check timer configuration**:
```bash
sudo systemctl cat nyc-taxi-daily-pipeline.timer
```

**Reload and restart**:
```bash
sudo systemctl daemon-reload
sudo systemctl restart nyc-taxi-daily-pipeline.timer
```

### Missing Logs

**Check logs directory permissions**:
```bash
ls -ld logs/
# Should show: drwxr-xr-x ... your_user your_group ... logs/
```

**Fix permissions**:
```bash
sudo chown -R $USER:$USER logs/
chmod 755 logs/
```

### Email/Discord Not Sending

**Test manually**:
```bash
cd /path/to/project
source .venv/bin/activate
python send_weekly_report.py --week "week_1_november_2025" --dry-run
```

**Check environment variables**:
```bash
cat .env
# Verify all credentials are correct
```

## 📞 Support

Untuk issue atau pertanyaan:
1. Check [README.md](README.md) untuk dokumentasi lengkap
2. Review logs: `logs/` dan `journalctl`
3. Verify environment: `.env` file
4. Check systemd status: `systemctl status`

## 🎯 Best Practices

1. **Monitor Logs**: Check logs regularly untuk deteksi masalah early
2. **Backup Data**: Backup database dan CSV files secara berkala
3. **Test Manual**: Test services manually sebelum rely on automation
4. **Update Dependencies**: Keep Python packages up to date
5. **Log Rotation**: Script sudah include log cleanup (30/60 hari)

---

**Version**: 1.00
**Last Updated**: November 2025
**Systemd**: Recommended automation solution for Linux
