# Diagram dan Alur Project
## NYC Taxi Data Pipeline - Capstone 1

---

## 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    NYC TAXI DATA PIPELINE                            │
│                         Capstone 1                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│  DATA SOURCES   │
├─────────────────┤
│ • Green Taxi    │──┐
│   Parquet       │  │
│   (1.2 MB)      │  │
│                 │  │
│ • Yellow Taxi   │  │
│   Parquet       │  │
│   (72.4 MB)     │  │
└─────────────────┘  │
                     │
                     ▼
        ┌────────────────────────┐
        │  DATA EXTRACTION       │
        │  (data_extraction.py)  │
        ├────────────────────────┤
        │ • Schema Normalization │
        │ • Date Filtering       │
        │ • Combine Green+Yellow │
        │ • Parquet Writer       │
        └────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
┌──────────────┐          ┌──────────────┐
│ DAILY FILES  │          │ WEEKLY DATA  │
│ (Parquet)    │          │ (PostgreSQL) │
├──────────────┤          ├──────────────┤
│ taxi_data_   │          │ week_N_      │
│ 2025-09-01   │          │ month_year   │
│ ...          │          │ table        │
│ taxi_data_   │          └──────────────┘
│ 2025-09-07   │                 │
└──────────────┘                 │
        │                        │
        │                        ▼
        │            ┌────────────────────────┐
        │            │  DATA PIPELINE         │
        │            │  (data_pipeline.py)    │
        │            ├────────────────────────┤
        │            │ • DataLoader           │
        │            │ • DataCleaner          │
        │            │ • AggregationEngine    │
        │            │ • OutputManager        │
        │            └────────────────────────┘
        │                        │
        │                        ▼
        │            ┌────────────────────────┐
        │            │  5 CSV AGGREGATIONS    │
        │            ├────────────────────────┤
        │            │ 1. Trips per Day       │
        │            │ 2. Revenue per Day     │
        │            │ 3. Peak Hours          │
        │            │ 4. Daily Avg Metrics   │
        │            │ 5. Anomaly Monitoring  │
        │            └────────────────────────┘
        │                        │
        │                        ▼
        │            ┌────────────────────────┐
        │            │  REPORTING             │
        │            │ (send_weekly_report.py)│
        │            ├────────────────────────┤
        │            │ • Discord Webhook      │
        │            │ • Gmail SMTP           │
        │            └────────────────────────┘
        │                        │
        │            ┌───────────┴────────────┐
        │            │                        │
        │            ▼                        ▼
        │     ┌─────────────┐        ┌──────────────┐
        │     │  DISCORD    │        │    EMAIL     │
        │     │  @mention   │        │  + 5 CSVs    │
        │     │  Summary    │        │  Attachments │
        │     └─────────────┘        └──────────────┘
        │
        ▼
┌──────────────────────────────────────┐
│       AUTOMATION LAYER               │
├──────────────────────────────────────┤
│  Systemd Timers:                     │
│  • Daily Pipeline (02:00 AM)         │
│  • Weekly Report (Sunday 18:00)      │
│                                      │
│  Bash Scripts:                       │
│  • daily_data_pipeline.sh            │
│  • weekly_report.sh                  │
│  • Log rotation (30/60 days)         │
└──────────────────────────────────────┘
```

---

## 2. Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA FLOW PIPELINE                            │
└─────────────────────────────────────────────────────────────────────┘

START
  │
  ├─► [1] RAW DATA
  │   ┌──────────────────────────────────────┐
  │   │ green_tripdata_2025-09.parquet       │
  │   │ yellow_tripdata_2025-09.parquet      │
  │   └──────────────────────────────────────┘
  │
  ├─► [2] SPARK DATAFRAME LOAD
  │   ┌──────────────────────────────────────┐
  │   │ spark.read.parquet()                 │
  │   │ • Green DF: 50+ columns              │
  │   │ • Yellow DF: 50+ columns             │
  │   └──────────────────────────────────────┘
  │
  ├─► [3] SCHEMA NORMALIZATION
  │   ┌──────────────────────────────────────┐
  │   │ Green:                               │
  │   │   lpep_pickup_datetime               │
  │   │     → pickup_datetime                │
  │   │   lpep_dropoff_datetime              │
  │   │     → dropoff_datetime               │
  │   │                                      │
  │   │ Yellow:                              │
  │   │   tpep_pickup_datetime               │
  │   │     → pickup_datetime                │
  │   │   tpep_dropoff_datetime              │
  │   │     → dropoff_datetime               │
  │   └──────────────────────────────────────┘
  │
  ├─► [4] ADD TAXI TYPE COLUMN
  │   ┌──────────────────────────────────────┐
  │   │ green_df = green_df.withColumn(      │
  │   │   "taxi_type", lit("green")          │
  │   │ )                                    │
  │   │                                      │
  │   │ yellow_df = yellow_df.withColumn(    │
  │   │   "taxi_type", lit("yellow")         │
  │   │ )                                    │
  │   └──────────────────────────────────────┘
  │
  ├─► [5] UNION DATAFRAMES
  │   ┌──────────────────────────────────────┐
  │   │ combined_df = green_df.union(        │
  │   │   yellow_df                          │
  │   │ )                                    │
  │   │                                      │
  │   │ Result: Unified schema with          │
  │   │ taxi_type differentiator             │
  │   └──────────────────────────────────────┘
  │
  ├─► [6] DATE FILTERING
  │   ┌──────────────────────────────────────┐
  │   │ Daily Mode:                          │
  │   │   filter(date == "2025-09-01")       │
  │   │                                      │
  │   │ Daily-Range Mode:                    │
  │   │   filter(date >= "2025-09-01" AND    │
  │   │          date <= "2025-09-07")       │
  │   └──────────────────────────────────────┘
  │
  ├─► [7] SAVE DAILY FILES
  │   ┌──────────────────────────────────────┐
  │   │ Daily Mode:                          │
  │   │   df.coalesce(1)                     │
  │   │     .write.parquet()                 │
  │   │   → taxi_data_2025-09-01.parquet     │
  │   │                                      │
  │   │ Daily-Range Mode:                    │
  │   │   for date in date_range:            │
  │   │     df_day.coalesce(1)               │
  │   │            .write.parquet()          │
  │   │   → 7 separate daily files           │
  │   └──────────────────────────────────────┘
  │
  ├─► [8] WEEKLY AGGREGATION (Sunday only)
  │   ┌──────────────────────────────────────┐
  │   │ Load all daily files:                │
  │   │   week_df = spark.read.parquet(      │
  │   │     "daily/taxi_data_*.parquet"      │
  │   │   )                                  │
  │   │                                      │
  │   │ Calculate week name:                 │
  │   │   min_date = week_df.agg(min("date"))│
  │   │   max_date = week_df.agg(max("date"))│
  │   │   week_num = calculate_week_number() │
  │   │   week_name = f"week_{num}_          │
  │   │                {month}_{year}"       │
  │   └──────────────────────────────────────┘
  │
  ├─► [9A] SAVE TO POSTGRESQL (default)
  │   ┌──────────────────────────────────────┐
  │   │ week_df.write                        │
  │   │   .format("jdbc")                    │
  │   │   .option("url", pg_url)             │
  │   │   .option("dbtable", week_name)      │
  │   │   .mode("overwrite")                 │
  │   │   .save()                            │
  │   │                                      │
  │   │ Result: Table "week_1_september_2025"│
  │   └──────────────────────────────────────┘
  │
  ├─► [9B] OR SAVE TO PARQUET (optional)
  │   ┌──────────────────────────────────────┐
  │   │ week_df.coalesce(1)                  │
  │   │   .write.parquet(                    │
  │   │     f"weekly/{week_name}.parquet"    │
  │   │   )                                  │
  │   └──────────────────────────────────────┘
  │
  ├─► [10] DATA PIPELINE AGGREGATIONS
  │   ┌──────────────────────────────────────┐
  │   │ Load from PostgreSQL:                │
  │   │   df = spark.read.jdbc(table_name)   │
  │   │                                      │
  │   │ Add trip_duration column:            │
  │   │   df = df.withColumn(                │
  │   │     "trip_duration_minutes",         │
  │   │     (unix_timestamp(dropoff) -       │
  │   │      unix_timestamp(pickup)) / 60    │
  │   │   )                                  │
  │   │                                      │
  │   │ Clean data:                          │
  │   │   df = df.filter(                    │
  │   │     trip_distance > 0 AND            │
  │   │     fare_amount > 0 AND              │
  │   │     ...                              │
  │   │   )                                  │
  │   │                                      │
  │   │ Execute 5 aggregation strategies:    │
  │   │   1. TripsPerDayStrategy             │
  │   │   2. RevenuePerDayStrategy           │
  │   │   3. PeakHourStrategy                │
  │   │   4. DailyAvgMetricsStrategy         │
  │   │   5. AnomalyMonitoringStrategy       │
  │   │                                      │
  │   │ Save to CSV:                         │
  │   │   output/{week_name}/                │
  │   │     YYYY_MM_weekN_*.csv              │
  │   └──────────────────────────────────────┘
  │
  ├─► [11] REPORT GENERATION
  │   ┌──────────────────────────────────────┐
  │   │ Load all 5 CSVs:                     │
  │   │   trips_df = pd.read_csv()           │
  │   │   revenue_df = pd.read_csv()         │
  │   │   ...                                │
  │   │                                      │
  │   │ Build summary:                       │
  │   │   - Total trips (Green/Yellow)       │
  │   │   - Total revenue                    │
  │   │   - Peak hours                       │
  │   │   - Anomalies detected               │
  │   │   - Recommendations                  │
  │   └──────────────────────────────────────┘
  │
  ├─► [12] MULTI-CHANNEL DISTRIBUTION
  │   ┌──────────────────────────────────────┐
  │   │ Discord:                             │
  │   │   POST to webhook URL                │
  │   │   - Auto-split if > 2000 chars       │
  │   │   - Mention @samsudinde              │
  │   │                                      │
  │   │ Gmail:                               │
  │   │   SMTP send via gmail                │
  │   │   - HTML body                        │
  │   │   - 5 CSV attachments                │
  │   │   - Multiple recipients              │
  │   └──────────────────────────────────────┘
  │
  └─► END
```

---

## 3. Process Flow - Daily Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│              DAILY AUTOMATED PIPELINE FLOW                           │
│                   (Runs at 02:00 AM)                                 │
└─────────────────────────────────────────────────────────────────────┘

02:00:00  │  Systemd Timer triggers
          │  nyc-taxi-daily-pipeline.service
          ▼
      ┌────────────────────────────────┐
      │ daily_data_pipeline.sh         │
      └────────────────────────────────┘
          │
          ├─► Activate Python venv
          │   source .venv/bin/activate
          │
          ├─► Get current date
          │   CURRENT_DATE=$(date +%Y-%m-%d)
          │   Example: "2025-09-01"
          │
          ├─► Run daily extraction
          │   python data_extraction.py \
          │     --mode daily \
          │     --date $CURRENT_DATE
          │
          │   Result:
          │   ✓ taxi_data_2025-09-01.parquet
          │   ✓ Log: Processing successful
          │
          ├─► Check day of week
          │   DAY_OF_WEEK=$(date +%u)
          │   # 1=Monday, ..., 7=Sunday
          │
          ▼
     Is Sunday?
          │
    ┌─────┴─────┐
    │           │
   YES         NO
    │           │
    │           └──► End (Daily only)
    │
    ├─► Run weekly aggregation
    │   python data_extraction.py \
    │     --mode weekly
    │
    │   Actions:
    │   1. Load all daily files
    │      (taxi_data_2025-09-01.parquet
    │       ... to ...
    │       taxi_data_2025-09-07.parquet)
    │
    │   2. Calculate week name
    │      week_1_september_2025
    │
    │   3. Upload to PostgreSQL
    │      CREATE TABLE week_1_september_2025
    │      INSERT all records
    │
    │   Result:
    │   ✓ PostgreSQL table created
    │   ✓ ~350,000 records inserted
    │
    ├─► Extract week name
    │   WEEK_NAME=$(python -c "...")
    │
    ├─► Run data pipeline
    │   python data_pipeline.py \
    │     --table $WEEK_NAME
    │
    │   Actions:
    │   1. Load from PostgreSQL
    │   2. Add trip_duration column
    │   3. Clean data (remove invalid)
    │   4. Run 5 aggregations:
    │      - Trips per day
    │      - Revenue per day
    │      - Peak hours
    │      - Daily avg metrics
    │      - Anomaly monitoring
    │   5. Save 5 CSVs to output/
    │
    │   Result:
    │   ✓ 5 CSV files generated
    │   ✓ Saved in output/week_1_september_2025/
    │
    └─► End (Daily + Weekly)

02:05:00  │  Pipeline complete
          │  Total time: ~5 minutes
          │
          ├─► Cleanup old logs
          │   find logs/ -mtime +30 -delete
          │
          └─► Exit
```

---

## 4. Process Flow - Weekly Report

```
┌─────────────────────────────────────────────────────────────────────┐
│               WEEKLY REPORT DISTRIBUTION FLOW                        │
│                 (Runs Sunday at 18:00)                               │
└─────────────────────────────────────────────────────────────────────┘

18:00:00  │  Systemd Timer triggers
          │  nyc-taxi-weekly-report.service
          ▼
      ┌────────────────────────────────┐
      │ weekly_report.sh               │
      └────────────────────────────────┘
          │
          ├─► Activate Python venv
          │
          ├─► Calculate week name
          │   WEEK_NAME=$(python -c "
          │     from data_extraction import get_week_name
          │     print(get_week_name())
          │   ")
          │
          │   Result: "week_1_september_2025"
          │
          ├─► Run report sender
          │   python send_weekly_report.py \
          │     --week $WEEK_NAME
          │
          ▼
      ┌────────────────────────────────┐
      │ send_weekly_report.py          │
      └────────────────────────────────┘
          │
          ├─► Validate CSV files exist
          │   output/week_1_september_2025/
          │     ├─ 2025_09_week1_trips_per_day.csv
          │     ├─ 2025_09_week1_revenue_per_day.csv
          │     ├─ 2025_09_week1_peak_hour_per_day.csv
          │     ├─ 2025_09_week1_daily_avg_metrics.csv
          │     └─ 2025_09_week1_anomaly_monitoring.csv
          │
          │   If missing:
          │   ✗ Error: Files not found
          │   Exit
          │
          ├─► Load and analyze CSVs
          │   trips_df = pd.read_csv(...)
          │   revenue_df = pd.read_csv(...)
          │   peak_df = pd.read_csv(...)
          │   avg_df = pd.read_csv(...)
          │   anomaly_df = pd.read_csv(...)
          │
          ├─► Build summary report
          │   ┌─────────────────────────────┐
          │   │ SUMMARY CALCULATIONS        │
          │   ├─────────────────────────────┤
          │   │ • Total Yellow trips        │
          │   │ • Total Green trips         │
          │   │ • Total revenue (both)      │
          │   │ • Peak hours ranking        │
          │   │ • Anomalies count           │
          │   │ • Recommendations           │
          │   └─────────────────────────────┘
          │
          ├─► Format for Discord
          │   report_text = build_discord_message()
          │
          │   Check length:
          │   if len(report_text) > 2000:
          │     chunks = split_message(report_text)
          │   else:
          │     chunks = [report_text]
          │
          ├─► Send to Discord
          │   for chunk in chunks:
          │     POST webhook_url
          │       Content-Type: application/json
          │       Body: {
          │         "content": "@samsudinde\n" + chunk
          │       }
          │
          │   Result:
          │   ✓ Discord messages sent
          │   ✓ User mentioned
          │
          ├─► Format for Email
          │   html_body = build_html_email()
          │
          │   HTML structure:
          │   <h2>Laporan Agregasi NYC Taxi</h2>
          │   <table>Summary stats</table>
          │   <ul>5 attached files</ul>
          │
          ├─► Attach CSVs
          │   for csv_file in all_csv_files:
          │     part = MIMEBase('text', 'csv')
          │     part.set_payload(csv_content)
          │     msg.attach(part)
          │
          ├─► Send via Gmail SMTP
          │   smtp = smtplib.SMTP('smtp.gmail.com', 587)
          │   smtp.starttls()
          │   smtp.login(email, app_password)
          │   smtp.send_message(msg)
          │
          │   Result:
          │   ✓ Email sent to all recipients
          │   ✓ 5 CSVs attached
          │
          └─► Log completion
              ✓ Discord: Success
              ✓ Gmail: Success
              ✓ Report sent at 18:05:23

18:05:23  │  Report distribution complete
          │  Total time: ~5-10 seconds
          │
          ├─► Cleanup old logs
          │   find logs/ -mtime +60 -delete
          │
          └─► Exit
```

---

## 5. Class Diagram - Data Pipeline Module

```
┌─────────────────────────────────────────────────────────────────────┐
│                   data_pipeline.py OOP STRUCTURE                     │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐
│   «abstract»                 │
│   AggregationStrategy        │
├──────────────────────────────┤
│ + execute(df): DataFrame     │
│ + get_output_filename(): str │
└──────────────────────────────┘
            ▲
            │ implements
            │
  ┌─────────┼─────────┬─────────┬─────────┬──────────┐
  │         │         │         │         │          │
┌─┴──────┐ │ ┌───────┴─┐ ┌─────┴──┐ ┌────┴───┐ ┌───┴────────┐
│ Trips  │ │ │Revenue  │ │Peak    │ │Daily   │ │Anomaly     │
│ PerDay │ │ │PerDay   │ │Hour    │ │Avg     │ │Monitoring  │
│Strategy│ │ │Strategy │ │Strategy│ │Metrics │ │Strategy    │
└────────┘ │ └─────────┘ └────────┘ │Strategy│ └────────────┘
           │                        └────────┘
           │
           │
┌──────────────────────────────┐
│   DataLoader                 │
├──────────────────────────────┤
│ - spark: SparkSession        │
│ - db_url: str                │
│ - table_name: str            │
├──────────────────────────────┤
│ + load(): DataFrame          │
└──────────────────────────────┘
           │
           │ uses
           ▼
┌──────────────────────────────┐
│   DataCleaner                │
├──────────────────────────────┤
│ + add_trip_duration(df): DF  │
│ + clean_data(df): DF         │
│ + validate(df): bool         │
└──────────────────────────────┘
           │
           │ uses
           ▼
┌──────────────────────────────┐
│   AggregationEngine          │
├──────────────────────────────┤
│ - strategies: List[Strategy] │
├──────────────────────────────┤
│ + add_strategy(s): void      │
│ + execute_all(df): Dict      │
└──────────────────────────────┘
           │
           │ uses
           ▼
┌──────────────────────────────┐
│   OutputManager              │
├──────────────────────────────┤
│ - output_dir: str            │
│ - week_name: str             │
├──────────────────────────────┤
│ + save_to_csv(df, name): void│
│ + create_output_dir(): void  │
└──────────────────────────────┘

ORCHESTRATION:
┌──────────────────────────────┐
│   main()                     │
├──────────────────────────────┤
│ 1. loader = DataLoader()     │
│ 2. df = loader.load()        │
│ 3. cleaner = DataCleaner()   │
│ 4. df = cleaner.clean(df)    │
│ 5. engine = AggregationEngine│
│ 6. engine.add_strategy(...)  │
│ 7. results = engine.execute()│
│ 8. output = OutputManager()  │
│ 9. output.save_all(results)  │
└──────────────────────────────┘
```

**Design Pattern Benefits:**
- **Strategy Pattern**: Each aggregation is independent, testable
- **Dependency Injection**: Classes receive configs, not hardcoded
- **Single Responsibility**: Each class has one job
- **Open/Closed**: Easy to add new strategies without modifying existing

---

## 6. Sequence Diagram - End-to-End Weekly Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│           COMPLETE WEEKLY PROCESSING SEQUENCE                        │
└─────────────────────────────────────────────────────────────────────┘

Actor: Systemd Timer
Components: DailyScript, DataExtraction, PostgreSQL, DataPipeline,
            ReportSender, Discord, Gmail

Sunday
02:00 AM
  │
  │  Timer fires
  ├──────────────────────────────────────────►  DailyScript
  │                                                  │
  │                                                  │ run daily mode
  │                                                  ├────────►  DataExtraction
  │                                                  │               │
  │                                                  │               │ process date
  │                                                  │               ├───► Load raw
  │                                                  │               ├───► Normalize
  │                                                  │               ├───► Filter
  │                                                  │               ├───► Save parquet
  │                                                  │               │
  │                                                  │  ✓ Daily file │
  │                                                  │ ◄─────────────┤
  │                                                  │
  │                                                  │ check if Sunday
  │                                                  │ → YES
  │                                                  │
  │                                                  │ run weekly mode
  │                                                  ├────────►  DataExtraction
  │                                                  │               │
  │                                                  │               │ load all daily files
  │                                                  │               ├───► Read 7 parquets
  │                                                  │               ├───► Union all
  │                                                  │               ├───► Calculate week name
  │                                                  │               │
  │                                                  │               │ upload to database
  │                                                  │               ├───────────►  PostgreSQL
  │                                                  │               │                  │
  │                                                  │               │                  │ CREATE TABLE
  │                                                  │               │                  │ INSERT records
  │                                                  │               │       ✓ Success  │
  │                                                  │               │ ◄────────────────┤
  │                                                  │  ✓ Week table │
  │                                                  │ ◄─────────────┤
  │                                                  │
  │                                                  │ get week name
  │                                                  │ week_1_september_2025
  │                                                  │
  │                                                  │ run pipeline
  │                                                  ├────────►  DataPipeline
  │                                                  │               │
  │                                                  │               │ load data
  │                                                  │               ├───────────►  PostgreSQL
  │                                                  │               │                  │
  │                                                  │               │                  │ SELECT *
  │                                                  │               │       DataFrame  │
  │                                                  │               │ ◄────────────────┤
  │                                                  │               │
  │                                                  │               │ clean data
  │                                                  │               ├───► Add trip_duration
  │                                                  │               ├───► Filter invalid
  │                                                  │               │
  │                                                  │               │ run aggregations
  │                                                  │               ├───► TripsPerDayStrategy
  │                                                  │               ├───► RevenuePerDayStrategy
  │                                                  │               ├───► PeakHourStrategy
  │                                                  │               ├───► DailyAvgMetricsStrategy
  │                                                  │               ├───► AnomalyMonitoringStrategy
  │                                                  │               │
  │                                                  │               │ save CSVs
  │                                                  │               ├───► Write 5 CSV files
  │                                                  │               │
  │                                                  │  ✓ 5 CSVs    │
  │                                                  │ ◄─────────────┤
  │                                                  │
  │  ✓ Pipeline complete                           │
  ├──────────────────────────────────────────────◄─┤
  │
  │
Sunday
18:00 PM
  │
  │  Timer fires (weekly report)
  ├──────────────────────────────────────────►  ReportSender
  │                                                  │
  │                                                  │ calculate week name
  │                                                  │ week_1_september_2025
  │                                                  │
  │                                                  │ load CSVs
  │                                                  ├───► Read 5 CSV files
  │                                                  ├───► Analyze data
  │                                                  ├───► Build summary
  │                                                  │
  │                                                  │ send to Discord
  │                                                  ├────────►  Discord Webhook
  │                                                  │               │
  │                                                  │               │ POST message
  │                                                  │               │ @mention user
  │                                                  │       ✓ 200 OK │
  │                                                  │ ◄─────────────┤
  │                                                  │
  │                                                  │ send to Gmail
  │                                                  ├────────►  Gmail SMTP
  │                                                  │               │
  │                                                  │               │ SMTP auth
  │                                                  │               │ Send HTML + 5 CSVs
  │                                                  │       ✓ Success │
  │                                                  │ ◄─────────────┤
  │                                                  │
  │  ✓ Reports sent                                │
  ├──────────────────────────────────────────────◄─┤
  │
END
```

**Timeline:**
- **02:00-02:05**: Daily + Weekly processing (5 min)
- **18:00-18:05**: Report distribution (5 sec)

**Total Automation**: No manual intervention required

---

## 7. File Structure Diagram

```
/Users/budi.triatmojo/Documents/Capstone 1/
│
├── data/                                    # Data storage
│   ├── raw/                                 # Raw source files
│   │   ├── green_tripdata_2025-09.parquet   (1.2 MB)
│   │   └── yellow_tripdata_2025-09.parquet  (72.4 MB)
│   │
│   ├── processed/
│   │   ├── daily/                           # Daily processed files
│   │   │   ├── taxi_data_2025-09-01.parquet (2.1 MB)
│   │   │   ├── taxi_data_2025-09-02.parquet (2.1 MB)
│   │   │   ├── ...
│   │   │   └── taxi_data_2025-09-07.parquet (2.1 MB)
│   │   │
│   │   └── weekly/                          # Weekly combined files
│   │       └── week_1_september_2025.parquet (15 MB)
│   │
│   └── data_dictionary/                     # Schema documentation
│       ├── data_dictionary_trip_records_green.pdf
│       └── data_dictionary_trip_records_yellow.pdf
│
├── output/                                  # Aggregation results
│   └── week_1_september_2025/               # Week-specific folder
│       ├── 2025_09_week1_trips_per_day.csv
│       ├── 2025_09_week1_revenue_per_day.csv
│       ├── 2025_09_week1_peak_hour_per_day.csv
│       ├── 2025_09_week1_daily_avg_metrics.csv
│       └── 2025_09_week1_anomaly_monitoring.csv
│
├── logs/                                    # Execution logs
│   ├── daily_pipeline_20250901_020000.log
│   ├── daily_pipeline_20250908_020000.log
│   ├── weekly_report_20250908_180000.log
│   └── report.log                           # Consolidated log
│
├── scripts/                                 # Automation scripts
│   ├── daily_data_pipeline.sh               # Daily execution wrapper
│   ├── weekly_report.sh                     # Weekly report sender
│   ├── setup_automation.sh                  # Systemd installer
│   └── setup_postgres.sh                    # Database setup
│
├── systemd/                                 # Service definitions
│   ├── nyc-taxi-daily-pipeline.service
│   ├── nyc-taxi-daily-pipeline.timer
│   ├── nyc-taxi-weekly-report.service
│   └── nyc-taxi-weekly-report.timer
│
├── docs/                                    # Documentation
│   ├── 01_Latar_Belakang_dan_Tujuan.md
│   ├── 02_Hasil_dan_Pembahasan.md
│   ├── 03_Diagram_dan_Alur_Project.md      # This file
│   ├── 04_Penjelasan_Kode.md
│   ├── 05_Output_dan_Screenshots.md
│   └── 06_Kesimpulan_dan_Saran.md
│
├── Python Scripts (Main)
│   ├── data_extraction.py                   (688 LOC)
│   ├── data_pipeline.py                     (954 LOC)
│   ├── send_weekly_report.py                (841 LOC)
│   ├── config.py                            (40 LOC)
│   └── view_parquet.py                      (96 LOC)
│
├── Configuration
│   ├── .env                                 # Environment variables (gitignored)
│   ├── .env.example                         # Template for .env
│   └── .gitignore
│
├── Database
│   └── Docker container: nyc-taxi-postgres
│       └── PostgreSQL 12+
│           └── Database: nyc_taxi_db
│               └── Tables: week_1_september_2025, week_2_..., etc.
│
├── Dependencies
│   ├── .venv/                               # Python virtual environment
│   ├── requirements.txt                     # Python dependencies
│   └── postgresql-42.7.4.jar                # JDBC driver (auto-downloaded)
│
└── Git
    └── .git/
        └── branch: budi_capstone1
```

**Total Files:**
- **Python**: 5 main scripts (2,619 LOC)
- **Bash**: 4 automation scripts
- **Systemd**: 4 service/timer files
- **Documentation**: 6 markdown files
- **Config**: 3 files (.env, .env.example, .gitignore)

**Data Volume:**
- Raw: ~74 MB
- Processed: ~30 MB (weekly)
- Database: ~200 MB
- Logs: ~50 MB (30-60 day retention)
- Total: ~350 MB

---

## 8. Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                   DEPLOYMENT ENVIRONMENT                             │
└─────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│  macOS Host (Darwin 25.1.0)                                        │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  systemd (User Service Manager)                              │ │
│  │  ┌────────────────────┐  ┌────────────────────┐              │ │
│  │  │ daily-pipeline     │  │ weekly-report      │              │ │
│  │  │ .timer             │  │ .timer             │              │ │
│  │  │                    │  │                    │              │ │
│  │  │ OnCalendar:        │  │ OnCalendar:        │              │ │
│  │  │ *-*-* 02:00:00     │  │ Sun *-*-* 18:00:00 │              │ │
│  │  └────────────────────┘  └────────────────────┘              │ │
│  │           │                       │                           │ │
│  │           │ triggers              │ triggers                  │ │
│  │           ▼                       ▼                           │ │
│  │  ┌────────────────────┐  ┌────────────────────┐              │ │
│  │  │ daily-pipeline     │  │ weekly-report      │              │ │
│  │  │ .service           │  │ .service           │              │ │
│  │  │                    │  │                    │              │ │
│  │  │ ExecStart:         │  │ ExecStart:         │              │ │
│  │  │ daily_*.sh         │  │ weekly_*.sh        │              │ │
│  │  └────────────────────┘  └────────────────────┘              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Python Virtual Environment (.venv)                          │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐             │ │
│  │  │ PySpark    │  │ Pandas     │  │ Requests   │             │ │
│  │  │ 3.x        │  │ 2.x        │  │ 2.x        │             │ │
│  │  └────────────┘  └────────────┘  └────────────┘             │ │
│  │  ┌────────────┐  ┌────────────┐                              │ │
│  │  │ smtplib    │  │ pathlib    │                              │ │
│  │  │ (built-in) │  │ (built-in) │                              │ │
│  │  └────────────┘  └────────────┘                              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Docker Container: nyc-taxi-postgres                         │ │
│  │  ┌────────────────────────────────────────────────────────┐  │ │
│  │  │  PostgreSQL 12+                                        │  │ │
│  │  │  ┌──────────────────────────────────────────────────┐  │  │ │
│  │  │  │  Database: nyc_taxi_db                           │  │  │ │
│  │  │  │  ┌────────────────────────────────────────────┐  │  │  │ │
│  │  │  │  │  Tables:                                   │  │  │  │ │
│  │  │  │  │  • week_1_september_2025                   │  │  │  │ │
│  │  │  │  │  • week_2_september_2025                   │  │  │  │ │
│  │  │  │  │  • ...                                     │  │  │  │ │
│  │  │  │  └────────────────────────────────────────────┘  │  │  │ │
│  │  │  └──────────────────────────────────────────────────┘  │  │ │
│  │  │  Port: 5432 (mapped to host)                           │  │ │
│  │  │  Volume: postgres_data (persistent)                    │  │ │
│  │  └────────────────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  File System:                                                      │
│  /Users/budi.triatmojo/Documents/Capstone 1/                       │
│  ├── data/ (raw, processed)                                        │
│  ├── output/ (CSVs)                                                │
│  ├── logs/ (execution logs)                                        │
│  └── scripts/ (bash automation)                                    │
└────────────────────────────────────────────────────────────────────┘
         │                                                │
         │ HTTPS                                          │ SMTP/TLS
         ▼                                                ▼
┌─────────────────────┐                      ┌───────────────────────┐
│  Discord Webhook    │                      │  Gmail SMTP           │
│  (External API)     │                      │  smtp.gmail.com:587   │
│                     │                      │                       │
│  Receives:          │                      │  Receives:            │
│  • Weekly summaries │                      │  • HTML emails        │
│  • @mentions        │                      │  • 5 CSV attachments  │
│  • Auto-split msgs  │                      │  • Multiple recipients│
└─────────────────────┘                      └───────────────────────┘
```

**Network Connectivity:**
- **Local**: Python ↔ PostgreSQL (JDBC over localhost:5432)
- **Internet**: Python → Discord (HTTPS webhook)
- **Internet**: Python → Gmail (SMTP TLS port 587)

**Security:**
- Environment variables in .env (gitignored)
- Gmail App Password (not account password)
- Discord webhook URL (secret)
- PostgreSQL credentials (local only)

---

## 9. Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ERROR HANDLING STRATEGY                          │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│  Operation Start    │
└─────────────────────┘
          │
          ▼
    ┌─────────────┐
    │ Try Block   │
    └─────────────┘
          │
          ├───► File exists?
          │       │
          │       ├── NO ──► FileNotFoundError
          │       │              │
          │       │              ├─► Log error
          │       │              ├─► Return gracefully
          │       │              └─► Continue to next task
          │       │
          │       └── YES ──► Continue
          │
          ├───► Load data
          │       │
          │       ├── FAIL ──► DataLoadError
          │       │              │
          │       │              ├─► Log error with details
          │       │              ├─► Retry (max 3 attempts)
          │       │              └─► Exit if all retries fail
          │       │
          │       └── SUCCESS ──► Continue
          │
          ├───► Process data
          │       │
          │       ├── INVALID ──► ValidationError
          │       │              │
          │       │              ├─► Log validation failures
          │       │              ├─► Remove invalid records
          │       │              └─► Continue with valid data
          │       │
          │       └── VALID ──► Continue
          │
          ├───► Save output
          │       │
          │       ├── DISK FULL ──► IOError
          │       │              │
          │       │              ├─► Log error
          │       │              ├─► Alert via Discord
          │       │              └─► Exit
          │       │
          │       └── SUCCESS ──► Continue
          │
          ├───► Send report
          │       │
          │       ├── Discord fails ──► RequestException
          │       │              │
          │       │              ├─► Log error
          │       │              └─► Continue to Email
          │       │
          │       ├── Email fails ──► SMTPException
          │       │              │
          │       │              ├─► Log error
          │       │              └─► Exit (at least tried both)
          │       │
          │       └── SUCCESS ──► Complete
          │
          ▼
    ┌─────────────┐
    │ Finally     │
    │ Block       │
    ├─────────────┤
    │ • Close     │
    │   connections│
    │ • Flush logs│
    │ • Cleanup   │
    │   temp files│
    └─────────────┘
          │
          ▼
    ┌─────────────┐
    │  Exit       │
    └─────────────┘
```

**Error Categories:**
1. **File Errors**: Missing files, permission denied
2. **Data Errors**: Corrupt parquet, invalid schema
3. **Database Errors**: Connection failed, query timeout
4. **Network Errors**: Discord webhook down, SMTP auth fail
5. **System Errors**: Out of memory, disk full

**Handling Strategy:**
- **Retry**: Database connections (3 attempts with backoff)
- **Skip**: Invalid data records (log and continue)
- **Fallback**: Discord fails → still try Email
- **Abort**: Critical errors (disk full, database unreachable)

---

## 10. State Machine - Weekly Processing

```
┌─────────────────────────────────────────────────────────────────────┐
│                    WEEKLY PROCESSING STATE MACHINE                   │
└─────────────────────────────────────────────────────────────────────┘

    START
      │
      ▼
  ┌─────────┐
  │  IDLE   │◄────────────────────────────┐
  └─────────┘                             │
      │                                   │
      │ Daily Timer (02:00 AM)            │
      ▼                                   │
  ┌─────────────────┐                    │
  │ DAILY_EXTRACT   │                    │
  └─────────────────┘                    │
      │                                   │
      │ Save daily parquet                │
      ▼                                   │
  ┌─────────┐                            │
  │  CHECK  │                            │
  │  DAY    │                            │
  └─────────┘                            │
      │                                   │
  ┌───┴───┐                              │
  │       │                              │
 YES     NO ─────────────────────────────┘
  │       (Not Sunday, back to IDLE)
  │
  │ (Sunday)
  ▼
┌─────────────────┐
│ WEEKLY_COMBINE  │
└─────────────────┘
  │
  │ Load 7 daily files
  ▼
┌─────────────────┐
│ UPLOAD_TO_DB    │
└─────────────────┘
  │
  │ PostgreSQL INSERT
  ▼
┌─────────────────┐
│ RUN_PIPELINE    │
└─────────────────┘
  │
  │ Execute 5 aggregations
  ▼
┌─────────────────┐
│ SAVE_CSV        │
└─────────────────┘
  │
  │ 5 CSV files written
  ▼
┌─────────────────┐
│ WAIT_FOR_REPORT │
└─────────────────┘
  │
  │ Wait until 18:00
  ▼
┌─────────────────┐
│ SEND_REPORTS    │
└─────────────────┘
  │
  │ Discord + Email
  ▼
┌─────────────────┐
│ CLEANUP         │
└─────────────────┘
  │
  │ Delete old logs
  ▼
┌─────────┐
│ COMPLETE│
└─────────┘
  │
  │ Back to IDLE for next week
  └──────────────────────────────────────────┐
                                             │
                                             ▼
                                         ┌─────────┐
                                         │  IDLE   │
                                         └─────────┘
```

**State Transitions:**
- **IDLE → DAILY_EXTRACT**: Triggered daily at 02:00
- **DAILY_EXTRACT → IDLE**: Monday-Saturday (after saving daily file)
- **DAILY_EXTRACT → WEEKLY_COMBINE**: Sunday only
- **WEEKLY_COMBINE → UPLOAD_TO_DB**: After combining 7 files
- **UPLOAD_TO_DB → RUN_PIPELINE**: After successful DB insert
- **RUN_PIPELINE → SAVE_CSV**: After all aggregations complete
- **SAVE_CSV → WAIT_FOR_REPORT**: CSV files ready
- **WAIT_FOR_REPORT → SEND_REPORTS**: At 18:00 Sunday
- **SEND_REPORTS → CLEANUP**: After distribution
- **CLEANUP → COMPLETE**: After log rotation
- **COMPLETE → IDLE**: Ready for next week

**State Persistence:**
- Files in `data/processed/daily/` indicate which days processed
- PostgreSQL tables indicate which weeks completed
- CSV files in `output/` indicate which weeks analyzed
- Logs in `logs/` provide complete audit trail

---

This diagram documentation provides a complete visual understanding of the NYC Taxi Data Pipeline architecture, workflows, and implementation details.

**Document Version**: 1.0
**Last Updated**: 2025-11-16
**Author**: Budi Triatmojo
**Project**: NYC Taxi Data Pipeline - Capstone 1
