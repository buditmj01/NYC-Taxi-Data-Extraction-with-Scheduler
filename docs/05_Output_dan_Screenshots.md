# Output dan Screenshots
## NYC Taxi Data Pipeline - Capstone 1

---

## 1. Overview Output Project

Project ini menghasilkan berbagai jenis output yang terstruktur dan terorganisir dengan baik:

**Output Categories:**
1. **Processed Data Files** (Parquet)
2. **Aggregation Results** (CSV)
3. **Reports** (Discord & Email)
4. **Logs** (Execution logs)

---

## 2. Processed Data Files

### 2.1 Daily Parquet Files

**Location:** `data/processed/daily/`

**File Naming Convention:** `taxi_data_YYYY-MM-DD.parquet`

**Example Files:**
```
data/processed/daily/
├── taxi_data_2025-09-01.parquet  (2.1 MB)
├── taxi_data_2025-09-02.parquet  (2.1 MB)
├── taxi_data_2025-09-03.parquet  (2.1 MB)
├── taxi_data_2025-09-04.parquet  (2.1 MB)
├── taxi_data_2025-09-05.parquet  (2.1 MB)
├── taxi_data_2025-09-06.parquet  (2.1 MB)
└── taxi_data_2025-09-07.parquet  (2.1 MB)
```

**File Content:**

| Column Name | Type | Example | Description |
|-------------|------|---------|-------------|
| VendorID | int | 1 | 1=Creative, 2=VeriFone |
| pickup_datetime | timestamp | 2025-09-01 17:30:00 | Normalized pickup time |
| dropoff_datetime | timestamp | 2025-09-01 17:45:00 | Normalized dropoff time |
| passenger_count | int | 1 | Number of passengers |
| trip_distance | double | 3.45 | Distance in miles |
| fare_amount | double | 12.50 | Base fare |
| total_amount | double | 15.75 | Total including tips/tolls |
| taxi_type | string | yellow | green or yellow |

**Key Features:**
- ✓ Single file per day (not folder with multiple parts)
- ✓ Combined Green + Yellow data
- ✓ Normalized schema
- ✓ Ready for analysis

**Sample Data Viewing:**
```bash
$ python view_parquet.py data/processed/daily/taxi_data_2025-09-01.parquet
```

**Output:**
```
=== Parquet File: taxi_data_2025-09-01.parquet ===
Total Records: 50,234

Schema:
 |-- VendorID: integer (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- taxi_type: string (nullable = false)

Sample Rows (first 5):
+--------+-------------------+-------------------+---------------+-------------+-----------+------------+----------+
|VendorID|   pickup_datetime |  dropoff_datetime |passenger_count|trip_distance|fare_amount|total_amount| taxi_type|
+--------+-------------------+-------------------+---------------+-------------+-----------+------------+----------+
|       2|2025-09-01 00:00:12|2025-09-01 00:12:34|            1.0|         2.34|      10.50|       13.80|    yellow|
|       1|2025-09-01 00:01:23|2025-09-01 00:15:45|            2.0|         4.12|      15.00|       18.50|    yellow|
|       2|2025-09-01 00:02:34|2025-09-01 00:08:56|            1.0|         1.20|       7.00|        9.30|     green|
|       1|2025-09-01 00:03:45|2025-09-01 00:25:12|            1.0|         5.67|      18.50|       22.75|    yellow|
|       2|2025-09-01 00:04:56|2025-09-01 00:18:23|            3.0|         3.45|      12.00|       15.60|     green|
+--------+-------------------+-------------------+---------------+-------------+-----------+------------+----------+
```

---

### 2.2 Weekly Parquet Files

**Location:** `data/processed/weekly/`

**File Naming Convention:** `week_N_month_year.parquet`

**Example Files:**
```
data/processed/weekly/
├── week_1_september_2025.parquet  (14.7 MB)
├── week_2_september_2025.parquet  (15.2 MB)
└── week_3_september_2025.parquet  (14.9 MB)
```

**File Content:**
- Combined data from 7 daily files
- Schema identical to daily files
- Used for weekly analysis

**Record Count:**
```
week_1_september_2025.parquet: ~351,500 records
```

---

### 2.3 PostgreSQL Database Tables

**Connection Details:**
```
Host: localhost
Port: 5432
Database: nyc_taxi_db
Container: nyc-taxi-postgres
```

**Table Naming Convention:** `week_N_month_year`

**Example Tables:**
```sql
nyc_taxi_db=# \dt
                  List of relations
 Schema |          Name            | Type  | Owner
--------+--------------------------+-------+----------
 public | week_1_september_2025    | table | postgres
 public | week_2_september_2025    | table | postgres
 public | week_3_september_2025    | table | postgres
```

**Table Structure:**
```sql
nyc_taxi_db=# \d week_1_september_2025
                          Table "public.week_1_september_2025"
      Column       |            Type             | Collation | Nullable | Default
-------------------+-----------------------------+-----------+----------+---------
 VendorID          | integer                     |           |          |
 pickup_datetime   | timestamp without time zone |           |          |
 dropoff_datetime  | timestamp without time zone |           |          |
 passenger_count   | double precision            |           |          |
 trip_distance     | double precision            |           |          |
 fare_amount       | double precision            |           |          |
 total_amount      | double precision            |           |          |
 taxi_type         | text                        |           |          |
```

**Sample Queries:**
```sql
-- Total records
SELECT COUNT(*) FROM week_1_september_2025;
-- Result: 351,500

-- Trips by taxi type
SELECT taxi_type, COUNT(*) as trips
FROM week_1_september_2025
GROUP BY taxi_type;

-- Result:
  taxi_type | trips
 -----------+--------
  yellow    | 342,567
  green     |   8,933
```

---

## 3. CSV Aggregation Files

### 3.1 File Organization

**Directory Structure:**
```
output/
├── week_1_september_2025/
│   ├── 2025_09_week1_trips_per_day.csv
│   ├── 2025_09_week1_revenue_per_day.csv
│   ├── 2025_09_week1_peak_hour_per_day.csv
│   ├── 2025_09_week1_daily_avg_metrics.csv
│   └── 2025_09_week1_anomaly_monitoring.csv
│
├── week_2_september_2025/
│   ├── 2025_09_week2_trips_per_day.csv
│   ├── 2025_09_week2_revenue_per_day.csv
│   ├── 2025_09_week2_peak_hour_per_day.csv
│   ├── 2025_09_week2_daily_avg_metrics.csv
│   └── 2025_09_week2_anomaly_monitoring.csv
```

**Benefits of Organization:**
- ✓ Easy to locate specific week's data
- ✓ Prevents file name conflicts
- ✓ Clean separation of results
- ✓ Scalable for multiple weeks

---

### 3.2 CSV File 1: Trips Per Day

**Filename:** `2025_09_week1_trips_per_day.csv`

**Columns:** `date`, `taxi_type`, `total_trips`

**Sample Data:**
```csv
date,taxi_type,total_trips
2025-09-01,green,1234
2025-09-01,yellow,48234
2025-09-02,green,1156
2025-09-02,yellow,39012
2025-09-03,green,987
2025-09-03,yellow,28456
2025-09-04,green,1345
2025-09-04,yellow,52123
2025-09-05,green,1423
2025-09-05,yellow,53456
2025-09-06,green,1567
2025-09-06,yellow,55678
2025-09-07,green,1512
2025-09-07,yellow,54234
```

**Total Rows:** 14 (7 days × 2 taxi types)

**Insights from Data:**
1. **Yellow Taxi Dominance**: 30-40x more trips than Green
2. **Weekday vs Weekend**:
   - Weekdays (Mon-Fri): 48k-54k trips
   - Weekends (Sat-Sun): 39k-55k trips (more variable)
3. **Anomaly Detection**: Sept 2-3 show significant drop

**Visualization (Conceptual):**
```
Daily Trips Comparison

Yellow Taxi  ████████████████████████████████████████ 48,234
Green Taxi   █                                         1,234

Legend: █ = 1,000 trips
```

---

### 3.3 CSV File 2: Revenue Per Day

**Filename:** `2025_09_week1_revenue_per_day.csv`

**Columns:** `date`, `taxi_type`, `total_revenue_per_day`

**Sample Data:**
```csv
date,taxi_type,total_revenue_per_day
2025-09-01,green,18234.56
2025-09-01,yellow,712345.12
2025-09-02,green,17012.34
2025-09-02,yellow,567890.45
2025-09-03,green,14567.89
2025-09-03,yellow,421234.67
2025-09-04,green,19845.23
2025-09-04,yellow,768901.23
2025-09-05,green,21012.45
2025-09-05,yellow,789012.34
2025-09-06,green,23145.67
2025-09-06,yellow,821456.78
2025-09-07,green,22345.12
2025-09-07,yellow,801234.56
```

**Insights:**
1. **Revenue Correlation**: Follows trip count patterns
2. **Average Revenue Per Trip**:
   ```
   Yellow: $712,345 / 48,234 trips = $14.76/trip
   Green:  $18,234 / 1,234 trips = $14.78/trip
   ```
   Very similar pricing!

3. **Weekly Total Revenue**:
   ```
   Yellow: $4,881,974.15
   Green:  $136,163.26
   Total:  $5,018,137.41
   ```

---

### 3.4 CSV File 3: Peak Hour Per Day

**Filename:** `2025_09_week1_peak_hour_per_day.csv`

**Columns:** `date`, `taxi_type`, `pickup_hour`, `trips_per_hour`

**Sample Data (2025-09-01, Yellow Taxi):**
```csv
date,taxi_type,pickup_hour,trips_per_hour
2025-09-01,yellow,0,1234
2025-09-01,yellow,1,876
2025-09-01,yellow,2,654
2025-09-01,yellow,3,432
2025-09-01,yellow,4,567
2025-09-01,yellow,5,987
2025-09-01,yellow,6,1654
2025-09-01,yellow,7,3456
2025-09-01,yellow,8,4567
2025-09-01,yellow,9,3987
2025-09-01,yellow,10,2876
2025-09-01,yellow,11,2654
2025-09-01,yellow,12,3123
2025-09-01,yellow,13,3456
2025-09-01,yellow,14,3987
2025-09-01,yellow,15,4234
2025-09-01,yellow,16,4876
2025-09-01,yellow,17,5678
2025-09-01,yellow,18,5432
2025-09-01,yellow,19,4987
2025-09-01,yellow,20,3876
2025-09-01,yellow,21,3234
2025-09-01,yellow,22,2654
2025-09-01,yellow,23,2123
```

**Total Rows:** 336 (7 days × 2 taxi types × 24 hours)

**Peak Hour Analysis:**
```
Top 5 Busiest Hours (Yellow Taxi, Sept 1):
1. 17:00 (5 PM) - 5,678 trips  ← PEAK
2. 18:00 (6 PM) - 5,432 trips
3. 16:00 (4 PM) - 4,876 trips
4. 19:00 (7 PM) - 4,987 trips
5. 15:00 (3 PM) - 4,234 trips

Lowest Hours:
1. 03:00 (3 AM) - 432 trips
2. 02:00 (2 AM) - 654 trips
3. 04:00 (4 AM) - 567 trips
```

**Hourly Distribution (Conceptual Chart):**
```
Trips Per Hour (Sept 1, Yellow Taxi)

6K |                 ▄
   |               ▄ █ ▄
5K |             ▄ █ █ █
   |           ▄ █ █ █ █ ▄
4K |         ▄ █ █ █ █ █ █
   |       ▄ █ █ █ █ █ █ █ ▄
3K |     ▄ █ █ █ █ █ █ █ █ █ ▄
   |   ▄ █ █ █ █ █ █ █ █ █ █ █ ▄
2K | ▄ █ █ █ █ █ █ █ █ █ █ █ █ █ ▄
   |▄█ █ █ █ █ █ █ █ █ █ █ █ █ █ █▄
1K |█ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █
   +─────────────────────────────────
   0 2 4 6 8 10 12 14 16 18 20 22 24
        Hour of Day

Peak: Evening Rush (17:00-19:00)
Low:  Early Morning (2:00-4:00)
```

---

### 3.5 CSV File 4: Daily Average Metrics

**Filename:** `2025_09_week1_daily_avg_metrics.csv`

**Columns:** `date`, `taxi_type`, `avg_trip_distance`, `avg_fare_amount`, `avg_total_amount`, `avg_passenger_count`, `avg_trip_duration_minutes`

**Sample Data:**
```csv
date,taxi_type,avg_trip_distance,avg_fare_amount,avg_total_amount,avg_passenger_count,avg_trip_duration_minutes
2025-09-01,green,4.12,13.89,16.34,1.38,16.8
2025-09-01,yellow,3.45,12.34,15.67,1.42,14.2
2025-09-02,green,4.05,13.56,15.98,1.35,16.5
2025-09-02,yellow,3.38,12.12,15.45,1.40,14.0
2025-09-03,green,3.98,13.21,15.67,1.33,16.2
2025-09-03,yellow,3.29,11.89,15.23,1.38,13.8
2025-09-04,green,4.18,14.02,16.56,1.40,17.1
2025-09-04,yellow,3.52,12.56,15.89,1.45,14.5
```

**Key Metrics Analysis:**

**1. Trip Distance:**
```
Green Taxi:  Avg 4.05 miles (longer trips, outer boroughs)
Yellow Taxi: Avg 3.41 miles (shorter trips, Manhattan core)
```

**2. Fare Structure:**
```
Green:  $13.67 avg fare → $3.38/mile
Yellow: $12.26 avg fare → $3.60/mile

Yellow has slightly higher per-mile rate (Manhattan premium)
```

**3. Total Amount (includes tips, tolls, surcharges):**
```
Green:  $16.13 avg total → 18% markup over fare
Yellow: $15.58 avg total → 27% markup over fare

Yellow passengers tip more!
```

**4. Passenger Count:**
```
Green:  1.36 avg passengers
Yellow: 1.41 avg passengers

Both around 1.4, indicating mostly solo riders with some pairs
```

**5. Trip Duration:**
```
Green:  16.6 minutes avg
Yellow: 14.1 minutes avg

Speed calculation:
Green:  (4.05 miles / 16.6 min) × 60 = 14.6 mph
Yellow: (3.41 miles / 14.1 min) × 60 = 14.5 mph

Nearly identical speeds! NYC traffic affects both equally.
```

---

### 3.6 CSV File 5: Anomaly Monitoring

**Filename:** `2025_09_week1_anomaly_monitoring.csv`

**Columns:** `date`, `taxi_type`, `daily_trips`, `total_revenue_per_day`, `avg_passenger_count`, `avg_trips`, `stddev_trips`, `pct_change_trips`, `is_anomaly`

**Sample Data:**
```csv
date,taxi_type,daily_trips,total_revenue_per_day,avg_passenger_count,avg_trips,stddev_trips,pct_change_trips,is_anomaly
2025-09-01,green,1234,18234.56,1.38,1323,145.2,,False
2025-09-01,yellow,48234,712345.12,1.42,47123,3456.7,,False
2025-09-02,green,1156,17012.34,1.35,1323,145.2,-6.3,False
2025-09-02,yellow,39012,567890.45,1.40,47123,3456.7,-19.1,True
2025-09-03,green,987,14567.89,1.33,1323,145.2,-14.6,True
2025-09-03,yellow,28456,421234.67,1.38,47123,3456.7,-27.1,True
2025-09-04,green,1345,19845.23,1.40,1323,145.2,36.3,False
2025-09-04,yellow,52123,768901.23,1.45,47123,3456.7,83.2,True
2025-09-05,green,1423,21012.45,1.42,1323,145.2,5.8,False
2025-09-05,yellow,53456,789012.34,1.48,47123,3456.7,2.6,False
```

**Anomaly Detection Results:**

**1. Statistical Outliers (±2σ):**
```
Yellow Taxi:
  Mean (μ):     47,123 trips
  Std Dev (σ):  3,456.7 trips

  Lower bound:  47,123 - 2(3,456.7) = 40,209
  Upper bound:  47,123 + 2(3,456.7) = 54,037

Anomalies detected:
  Sept 3: 28,456 trips < 40,209 ✗ ANOMALY
  Sept 4: 52,123 trips > 54,037 ✗ ANOMALY (borderline)
```

**2. Revenue Drop (>20%):**
```
Sept 2 → Sept 3:
  Revenue: $567,890 → $421,234
  Drop: -25.8% ✗ ANOMALY
```

**3. Trip Count Drop (>20%):**
```
Sept 1 → Sept 2:
  Trips: 48,234 → 39,012
  Drop: -19.1% ✓ OK (below 20% threshold)

Sept 2 → Sept 3:
  Trips: 39,012 → 28,456
  Drop: -27.1% ✗ ANOMALY
```

**4. Passenger Count Spike:**
```
All days: 1.33 - 1.48 passengers
None exceed 2.0 threshold ✓ OK
```

**Anomaly Summary Table:**
```
┌────────────┬───────────┬───────────────┬────────────────┬───────────┐
│ Date       │ Taxi Type │ Daily Trips   │ % Change       │ Anomaly?  │
├────────────┼───────────┼───────────────┼────────────────┼───────────┤
│ 2025-09-01 │ Yellow    │ 48,234        │ -              │ No        │
│ 2025-09-02 │ Yellow    │ 39,012        │ -19.1% ⚠️      │ **YES**   │
│ 2025-09-03 │ Yellow    │ 28,456        │ -27.1% ⚠️⚠️    │ **YES**   │
│ 2025-09-04 │ Yellow    │ 52,123        │ +83.2% 📈      │ **YES**   │
│ 2025-09-05 │ Yellow    │ 53,456        │ +2.6%          │ No        │
│ 2025-09-06 │ Yellow    │ 55,678        │ +4.2%          │ No        │
│ 2025-09-07 │ Yellow    │ 54,234        │ -2.6%          │ No        │
└────────────┴───────────┴───────────────┴────────────────┴───────────┘

Anomalies: 3 out of 7 days (42.9%)
```

**Root Cause Analysis (Hypothetical):**
- **Sept 2 (Mon)**: Labor Day holiday → fewer commuters
- **Sept 3 (Tue)**: Post-holiday recovery, slow return
- **Sept 4 (Wed)**: Rebound effect, pent-up demand

---

## 4. Discord Reports

### 4.1 Discord Channel Setup

**Webhook Configuration:**
```
Channel: #nyc-taxi-reports
Webhook URL: https://discord.com/api/webhooks/[ID]/[TOKEN]
Mention: @samsudinde
```

### 4.2 Sample Discord Message

**Message 1/2:**
```
@samsudinde

=== LAPORAN MINGGUAN NYC TAXI DATA PIPELINE ===
Periode: September 2025, Minggu ke-1 (2025-09-01 to 2025-09-07)

--- RINGKASAN EKSEKUSI ---
✓ Trips per Day: 14 rows processed
✓ Revenue per Day: 14 rows processed
✓ Peak Hours: 336 rows processed
✓ Daily Avg Metrics: 14 rows processed
✓ Anomaly Monitoring: 14 rows processed

--- TOTAL TRIPS & REVENUE ---
Yellow Taxi:
  Total Trips: 342,567
  Total Revenue: $4,881,974.15
  Avg Revenue/Trip: $14.25

Green Taxi:
  Total Trips: 8,934
  Total Revenue: $136,163.26
  Avg Revenue/Trip: $15.24

Combined:
  Total Trips: 351,501
  Total Revenue: $5,018,137.41

--- PEAK HOURS (Top 5) ---
1. 17:00 (5 PM) - 45,678 trips
2. 18:00 (6 PM) - 43,234 trips
3. 19:00 (7 PM) - 41,567 trips
4. 16:00 (4 PM) - 38,901 trips
5. 15:00 (3 PM) - 36,234 trips

Peak window: 15:00-19:00 (3 PM - 7 PM)
```

**Message 2/2:**
```
--- ⚠️ ANOMALI TERDETEKSI ---
Total Anomalies: 3

1. Sept 2, 2025 (yellow):
   Trips: 39,012 (-19.1% vs prev day)
   Revenue: $567,890 (-20.3%)
   Cause: Labor Day holiday effect

2. Sept 3, 2025 (yellow):
   Trips: 28,456 (-27.1% vs prev day)
   Revenue: $421,234 (-25.8%)
   Status: CRITICAL - below 2σ threshold

3. Sept 4, 2025 (yellow):
   Trips: 52,123 (+83.2% vs prev day)
   Revenue: $768,901 (+82.5%)
   Status: Rebound surge

--- RECOMMENDATIONS ---
• Investigate Sept 2-3 drop - verify data completeness
• Consider dynamic pricing during 17:00-19:00 peak
• Monitor post-holiday patterns for future Labor Days
• Yellow taxi dominates 97.5% of trips - optimize fleet allocation
• Green taxi has higher avg revenue/trip - explore niche markets

--- METADATA ---
Report generated: 2025-09-08 18:05:23
Pipeline version: 1.0
Data quality: 99.07% (3,266 invalid records removed)
Processing time: 4 min 32 sec

Questions? Contact Data Engineering Team.
```

**Discord Rendering Features:**
- ✓ Markdown formatting preserved
- ✓ User @mention triggers notification
- ✓ Emoji indicators (⚠️, ✓) for visual scanning
- ✓ Auto-split at 2000 chars maintains readability
- ✓ Timestamped delivery

---

## 5. Email Reports

### 5.1 Email Configuration

**SMTP Details:**
```
Server: smtp.gmail.com
Port: 587 (STARTTLS)
Authentication: App Password
From: nyc-taxi-pipeline@gmail.com
To: [Multiple recipients]
```

### 5.2 Email Subject

```
[NYC Taxi Pipeline] Laporan Agregasi Data Mingguan - September 2025, Minggu ke-1
```

### 5.3 Email HTML Body (Sample)

```html
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; }
        h2 { color: #2c3e50; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #3498db; color: white; }
        .anomaly { background-color: #ffe6e6; }
        .success { color: #27ae60; }
        .warning { color: #e74c3c; }
    </style>
</head>
<body>
    <h2>Laporan Agregasi Data NYC Taxi - Mingguan</h2>

    <p><strong>Periode:</strong> September 2025, Minggu ke-1 (2025-09-01 to 2025-09-07)</p>
    <p><strong>Tanggal Generate:</strong> 2025-09-08 18:05:23</p>

    <h3>Executive Summary</h3>
    <table>
        <tr>
            <th>Metric</th>
            <th>Yellow Taxi</th>
            <th>Green Taxi</th>
            <th>Total</th>
        </tr>
        <tr>
            <td>Total Trips</td>
            <td>342,567</td>
            <td>8,934</td>
            <td>351,501</td>
        </tr>
        <tr>
            <td>Total Revenue</td>
            <td>$4,881,974</td>
            <td>$136,163</td>
            <td>$5,018,137</td>
        </tr>
        <tr>
            <td>Avg Trip Distance</td>
            <td>3.41 miles</td>
            <td>4.05 miles</td>
            <td>3.47 miles</td>
        </tr>
        <tr>
            <td>Avg Fare</td>
            <td>$12.26</td>
            <td>$13.67</td>
            <td>$12.36</td>
        </tr>
    </table>

    <h3>Peak Hours</h3>
    <table>
        <tr>
            <th>Rank</th>
            <th>Hour</th>
            <th>Total Trips</th>
        </tr>
        <tr>
            <td>1</td>
            <td>17:00 (5 PM)</td>
            <td>45,678</td>
        </tr>
        <tr>
            <td>2</td>
            <td>18:00 (6 PM)</td>
            <td>43,234</td>
        </tr>
        <tr>
            <td>3</td>
            <td>19:00 (7 PM)</td>
            <td>41,567</td>
        </tr>
    </table>

    <h3 class="warning">Anomalies Detected</h3>
    <table>
        <tr>
            <th>Date</th>
            <th>Taxi Type</th>
            <th>Trips</th>
            <th>% Change</th>
            <th>Status</th>
        </tr>
        <tr class="anomaly">
            <td>2025-09-02</td>
            <td>Yellow</td>
            <td>39,012</td>
            <td>-19.1%</td>
            <td>Labor Day</td>
        </tr>
        <tr class="anomaly">
            <td>2025-09-03</td>
            <td>Yellow</td>
            <td>28,456</td>
            <td>-27.1%</td>
            <td>CRITICAL</td>
        </tr>
        <tr class="anomaly">
            <td>2025-09-04</td>
            <td>Yellow</td>
            <td>52,123</td>
            <td>+83.2%</td>
            <td>Rebound</td>
        </tr>
    </table>

    <h3>File Terlampir</h3>
    <ul>
        <li>2025_09_week1_trips_per_day.csv</li>
        <li>2025_09_week1_revenue_per_day.csv</li>
        <li>2025_09_week1_peak_hour_per_day.csv</li>
        <li>2025_09_week1_daily_avg_metrics.csv</li>
        <li>2025_09_week1_anomaly_monitoring.csv</li>
    </ul>

    <p class="success">✓ Semua aggregasi berhasil diproses</p>
    <p>Silakan review file CSV terlampir untuk analisis detail.</p>

    <hr>
    <p style="color: #7f8c8d; font-size: 12px;">
        NYC Taxi Data Pipeline v1.0 | Automated Report<br>
        Generated by PySpark + PostgreSQL + Python
    </p>
</body>
</html>
```

**Email Rendering:**
- ✓ Professional HTML formatting
- ✓ Color-coded anomalies (red background)
- ✓ Responsive tables
- ✓ Clear section headers
- ✓ Mobile-friendly design

### 5.4 Email Attachments

**Attached Files (5):**
1. `2025_09_week1_trips_per_day.csv` (1.2 KB)
2. `2025_09_week1_revenue_per_day.csv` (1.3 KB)
3. `2025_09_week1_peak_hour_per_day.csv` (12.5 KB)
4. `2025_09_week1_daily_avg_metrics.csv` (2.1 KB)
5. `2025_09_week1_anomaly_monitoring.csv` (2.5 KB)

**Total Attachment Size:** ~20 KB (well below Gmail 25 MB limit)

**Attachment Benefits:**
- ✓ Raw data for further analysis
- ✓ Easy to import into Excel/Tableau
- ✓ Archivable for compliance
- ✓ Shareable with stakeholders

---

## 6. Log Files

### 6.1 Daily Pipeline Logs

**Location:** `logs/daily_pipeline_YYYYMMDD_HHMMSS.log`

**Sample Log (logs/daily_pipeline_20250908_020000.log):**
```
=== NYC Taxi Daily Pipeline ===
Started: Sun Sep  8 02:00:00 EDT 2025

Activating virtual environment: .venv
Processing date: 2025-09-08

2025-09-08 02:00:15 - data_extraction - INFO - Initializing Spark session
2025-09-08 02:00:18 - data_extraction - INFO - Loading raw data files
2025-09-08 02:00:25 - data_extraction - INFO - Green taxi records: 4,234
2025-09-08 02:00:28 - data_extraction - INFO - Yellow taxi records: 48,901
2025-09-08 02:00:30 - data_extraction - INFO - Normalizing schemas
2025-09-08 02:00:32 - data_extraction - INFO - Combined DataFrame: 53,135 records
2025-09-08 02:00:35 - data_extraction - INFO - Filtering for date: 2025-09-08
2025-09-08 02:00:37 - data_extraction - INFO - Filtered records: 53,135
2025-09-08 02:00:40 - data_extraction - INFO - Saving to: data/processed/daily/taxi_data_2025-09-08.parquet
2025-09-08 02:00:45 - data_extraction - INFO - ✓ Successfully saved taxi_data_2025-09-08.parquet

Today is Sunday - Running weekly aggregation

2025-09-08 02:00:46 - data_extraction - INFO - Loading all daily files for weekly processing
2025-09-08 02:00:52 - data_extraction - INFO - Loaded 7 daily files
2025-09-08 02:00:55 - data_extraction - INFO - Total records: 351,500
2025-09-08 02:00:58 - data_extraction - INFO - Calculating week name
2025-09-08 02:01:00 - data_extraction - INFO - Week name: week_1_september_2025
2025-09-08 02:01:05 - data_extraction - INFO - Downloading PostgreSQL JDBC driver
2025-09-08 02:01:12 - data_extraction - INFO - ✓ JDBC driver ready
2025-09-08 02:01:15 - data_extraction - INFO - Uploading to PostgreSQL table: week_1_september_2025
2025-09-08 02:03:10 - data_extraction - INFO - ✓ Successfully uploaded 351,500 records to PostgreSQL
2025-09-08 02:03:12 - data_extraction - INFO - Stopping Spark session

Week name: week_1_september_2025

2025-09-08 02:03:15 - data_pipeline - INFO - Initializing Spark session
2025-09-08 02:03:18 - data_pipeline - INFO - Loading data from PostgreSQL table: week_1_september_2025
2025-09-08 02:03:45 - data_pipeline - INFO - Loaded 351,500 records
2025-09-08 02:03:48 - data_pipeline - INFO - Adding trip_duration column
2025-09-08 02:03:52 - data_pipeline - INFO - Cleaning data
2025-09-08 02:04:05 - data_pipeline - INFO - Removed 3,266 invalid records (0.93%)
2025-09-08 02:04:08 - data_pipeline - INFO - Valid records: 348,234
2025-09-08 02:04:10 - data_pipeline - INFO - Executing TripsPerDayStrategy
2025-09-08 02:04:25 - data_pipeline - INFO - ✓ TripsPerDayStrategy completed
2025-09-08 02:04:28 - data_pipeline - INFO - Executing RevenuePerDayStrategy
2025-09-08 02:04:40 - data_pipeline - INFO - ✓ RevenuePerDayStrategy completed
2025-09-08 02:04:42 - data_pipeline - INFO - Executing PeakHourStrategy
2025-09-08 02:05:10 - data_pipeline - INFO - ✓ PeakHourStrategy completed
2025-09-08 02:05:12 - data_pipeline - INFO - Executing DailyAvgMetricsStrategy
2025-09-08 02:05:28 - data_pipeline - INFO - ✓ DailyAvgMetricsStrategy completed
2025-09-08 02:05:30 - data_pipeline - INFO - Executing AnomalyMonitoringStrategy
2025-09-08 02:05:55 - data_pipeline - INFO - ✓ AnomalyMonitoringStrategy completed
2025-09-08 02:05:58 - data_pipeline - INFO - Saving results to output/week_1_september_2025
2025-09-08 02:06:02 - data_pipeline - INFO - Saved: 2025_09_week1_trips_per_day.csv (14 rows)
2025-09-08 02:06:05 - data_pipeline - INFO - Saved: 2025_09_week1_revenue_per_day.csv (14 rows)
2025-09-08 02:06:10 - data_pipeline - INFO - Saved: 2025_09_week1_peak_hour_per_day.csv (336 rows)
2025-09-08 02:06:13 - data_pipeline - INFO - Saved: 2025_09_week1_daily_avg_metrics.csv (14 rows)
2025-09-08 02:06:16 - data_pipeline - INFO - Saved: 2025_09_week1_anomaly_monitoring.csv (14 rows)
2025-09-08 02:06:18 - data_pipeline - INFO - ✓ All results saved to output/week_1_september_2025
2025-09-08 02:06:20 - data_pipeline - INFO - Stopping Spark session
2025-09-08 02:06:22 - data_pipeline - INFO - Pipeline complete!

Cleaning up old logs (30+ days)
Completed: Sun Sep  8 02:06:25 EDT 2025

Total execution time: 6 minutes 25 seconds
```

**Log Analysis:**
- **Start Time**: 02:00:00
- **End Time**: 02:06:25
- **Total Duration**: 6 min 25 sec
- **Records Processed**: 351,500
- **Invalid Records Removed**: 3,266 (0.93%)
- **CSV Files Generated**: 5
- **Status**: ✓ Success

---

### 6.2 Weekly Report Logs

**Location:** `logs/weekly_report_YYYYMMDD_HHMMSS.log`

**Sample Log (logs/weekly_report_20250908_180000.log):**
```
=== NYC Taxi Weekly Report Sender ===
Started: Sun Sep  8 18:00:00 EDT 2025

Calculating week name...
Week name: week_1_september_2025

2025-09-08 18:00:05 - send_weekly_report - INFO - Loading CSV files from output/week_1_september_2025
2025-09-08 18:00:06 - send_weekly_report - INFO - ✓ Loaded trips_per_day.csv
2025-09-08 18:00:07 - send_weekly_report - INFO - ✓ Loaded revenue_per_day.csv
2025-09-08 18:00:08 - send_weekly_report - INFO - ✓ Loaded peak_hour_per_day.csv
2025-09-08 18:00:09 - send_weekly_report - INFO - ✓ Loaded daily_avg_metrics.csv
2025-09-08 18:00:10 - send_weekly_report - INFO - ✓ Loaded anomaly_monitoring.csv
2025-09-08 18:00:12 - send_weekly_report - INFO - Building summary report
2025-09-08 18:00:15 - send_weekly_report - INFO - Summary complete
2025-09-08 18:00:16 - send_weekly_report - INFO - Sending to Discord
2025-09-08 18:00:18 - send_weekly_report - INFO - Message length: 2,456 chars
2025-09-08 18:00:19 - send_weekly_report - INFO - Splitting into 2 messages
2025-09-08 18:00:21 - send_weekly_report - INFO - ✓ Sent Discord chunk 1/2
2025-09-08 18:00:23 - send_weekly_report - INFO - ✓ Sent Discord chunk 2/2
2025-09-08 18:00:24 - send_weekly_report - INFO - Sending to Gmail
2025-09-08 18:00:26 - send_weekly_report - INFO - Building HTML email
2025-09-08 18:00:28 - send_weekly_report - INFO - Attaching CSV files
2025-09-08 18:00:32 - send_weekly_report - INFO - Connecting to SMTP server
2025-09-08 18:00:34 - send_weekly_report - INFO - Authenticating
2025-09-08 18:00:36 - send_weekly_report - INFO - Sending email
2025-09-08 18:00:42 - send_weekly_report - INFO - ✓ Email sent to 5 recipients
2025-09-08 18:00:43 - send_weekly_report - INFO - ✓ Report distribution complete

Cleaning up old logs (60+ days)
Completed: Sun Sep  8 18:00:45 EDT 2025

Total execution time: 45 seconds
```

**Log Analysis:**
- **Start Time**: 18:00:00
- **End Time**: 18:00:45
- **Total Duration**: 45 seconds
- **Discord Messages**: 2 (auto-split)
- **Email Recipients**: 5
- **Status**: ✓ Success

---

## 7. Command-Line Output Examples

### 7.1 Daily Extraction Output

```bash
$ python data_extraction.py --mode daily --date 2025-09-01

Initializing Spark session...
Loading raw data files...
Green taxi records: 4,123
Yellow taxi records: 48,234
Normalizing schemas...
Combined DataFrame: 52,357 records
Filtering for date: 2025-09-01
Filtered records: 52,357
Saving to: data/processed/daily/taxi_data_2025-09-01.parquet
✓ Successfully saved taxi_data_2025-09-01.parquet
Stopping Spark session
```

### 7.2 Daily-Range Extraction Output

```bash
$ python data_extraction.py --mode daily-range --date 2025-09-01 --days 7

Initializing Spark session...
Loading raw data files...
Green taxi records: 28,934
Yellow taxi records: 342,567
Normalizing schemas...
Combined DataFrame: 371,501 records
Processing 7 days starting from 2025-09-01

Processing date: 2025-09-01
Filtered records: 52,357
✓ Saved taxi_data_2025-09-01.parquet

Processing date: 2025-09-02
Filtered records: 40,168
✓ Saved taxi_data_2025-09-02.parquet

Processing date: 2025-09-03
Filtered records: 29,443
✓ Saved taxi_data_2025-09-03.parquet

Processing date: 2025-09-04
Filtered records: 53,468
✓ Saved taxi_data_2025-09-04.parquet

Processing date: 2025-09-05
Filtered records: 54,879
✓ Saved taxi_data_2025-09-05.parquet

Processing date: 2025-09-06
Filtered records: 57,245
✓ Saved taxi_data_2025-09-06.parquet

Processing date: 2025-09-07
Filtered records: 55,746
✓ Saved taxi_data_2025-09-07.parquet

✓ All 7 days processed successfully
Total time: 1m 32s
Stopping Spark session
```

### 7.3 Data Pipeline Output

```bash
$ python data_pipeline.py --table week_1_september_2025

Initializing Spark session...
Loading data from PostgreSQL table: week_1_september_2025
Loaded 351,500 records
Adding trip_duration column...
Cleaning data...
Removed 3,266 invalid records (0.93%)
Valid records: 348,234

Executing aggregation strategies...

[1/5] Executing TripsPerDayStrategy...
✓ TripsPerDayStrategy completed (14 rows)

[2/5] Executing RevenuePerDayStrategy...
✓ RevenuePerDayStrategy completed (14 rows)

[3/5] Executing PeakHourStrategy...
✓ PeakHourStrategy completed (336 rows)

[4/5] Executing DailyAvgMetricsStrategy...
✓ DailyAvgMetricsStrategy completed (14 rows)

[5/5] Executing AnomalyMonitoringStrategy...
✓ AnomalyMonitoringStrategy completed (14 rows)

Saving results to output/week_1_september_2025...
✓ Saved 2025_09_week1_trips_per_day.csv
✓ Saved 2025_09_week1_revenue_per_day.csv
✓ Saved 2025_09_week1_peak_hour_per_day.csv
✓ Saved 2025_09_week1_daily_avg_metrics.csv
✓ Saved 2025_09_week1_anomaly_monitoring.csv

✓ Pipeline complete!
Total time: 3m 45s
Stopping Spark session
```

---

## 8. Systemd Status Output

```bash
$ systemctl --user status nyc-taxi-daily-pipeline.timer
● nyc-taxi-daily-pipeline.timer - Run NYC Taxi Pipeline Daily at 2 AM
     Loaded: loaded (/home/user/.config/systemd/user/nyc-taxi-daily-pipeline.timer; enabled; vendor preset: enabled)
     Active: active (waiting) since Sun 2025-09-08 00:00:01 EDT; 18h ago
    Trigger: Mon 2025-09-09 02:00:00 EDT; 7h left
   Triggers: ● nyc-taxi-daily-pipeline.service

Sep 08 00:00:01 hostname systemd[1234]: Started Run NYC Taxi Pipeline Daily at 2 AM.

$ systemctl --user status nyc-taxi-weekly-report.timer
● nyc-taxi-weekly-report.timer - Run NYC Taxi Weekly Report on Sundays
     Loaded: loaded (/home/user/.config/systemd/user/nyc-taxi-weekly-report.timer; enabled; vendor preset: enabled)
     Active: active (waiting) since Sun 2025-09-08 00:00:01 EDT; 18h ago
    Trigger: Sun 2025-09-15 18:00:00 EDT; 6 days left
   Triggers: ● nyc-taxi-weekly-report.service

Sep 08 00:00:01 hostname systemd[1234]: Started Run NYC Taxi Weekly Report on Sundays.
```

---

## 9. Summary of All Outputs

| Output Type | Location | Format | Frequency | Retention |
|-------------|----------|--------|-----------|-----------|
| Daily Parquet | `data/processed/daily/` | Parquet | Daily | Permanent |
| Weekly Parquet | `data/processed/weekly/` | Parquet | Weekly | Permanent |
| PostgreSQL Tables | `nyc_taxi_db` | SQL | Weekly | Permanent |
| Aggregation CSVs | `output/{week_name}/` | CSV | Weekly | Permanent |
| Discord Reports | Discord channel | Text | Weekly | Discord history |
| Email Reports | Gmail inboxes | HTML+CSV | Weekly | Email archives |
| Daily Logs | `logs/daily_pipeline_*.log` | Text | Daily | 30 days |
| Weekly Logs | `logs/weekly_report_*.log` | Text | Weekly | 60 days |
| Consolidated Log | `report.log` | Text | Continuous | Permanent |

**Total Storage (per week):**
- Daily Parquet files: ~15 MB
- Weekly Parquet: ~15 MB
- PostgreSQL: ~200 MB (compressed)
- CSV files: ~20 KB
- Logs: ~500 KB
- **Total**: ~230 MB per week

---

**Document Version**: 1.0
**Last Updated**: 2025-11-16
**Author**: Budi Triatmojo
**Project**: NYC Taxi Data Pipeline - Capstone 1

**Note:** Screenshots would be captured from actual execution. The above provides detailed textual representations of all outputs with sample data and formatting examples.
