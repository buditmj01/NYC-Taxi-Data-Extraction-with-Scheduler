# Weekly PostgreSQL Table Examples

## Table Naming Convention

Tables are automatically created based on the week number (1-5) within the month.

**Format:** `week_[N]_[month]_[year]`

---

## September 2025 Examples

### Week 1 (September 1-7, 2025)
**Dates:** Monday Sept 1 - Sunday Sept 7

```bash
# Process daily
python data_extraction.py --mode daily --date 2025-09-01
python data_extraction.py --mode daily --date 2025-09-02
python data_extraction.py --mode daily --date 2025-09-03
python data_extraction.py --mode daily --date 2025-09-04
python data_extraction.py --mode daily --date 2025-09-05
python data_extraction.py --mode daily --date 2025-09-06
python data_extraction.py --mode daily --date 2025-09-07

# Send to PostgreSQL
python data_extraction.py --mode weekly
```

**Creates table:** `week_1_september_2025`

---

### Week 2 (September 8-14, 2025)
**Dates:** Monday Sept 8 - Sunday Sept 14

```bash
# Process daily
python data_extraction.py --mode daily --date 2025-09-08
python data_extraction.py --mode daily --date 2025-09-09
python data_extraction.py --mode daily --date 2025-09-10
python data_extraction.py --mode daily --date 2025-09-11
python data_extraction.py --mode daily --date 2025-09-12
python data_extraction.py --mode daily --date 2025-09-13
python data_extraction.py --mode daily --date 2025-09-14

# Send to PostgreSQL
python data_extraction.py --mode weekly
```

**Creates table:** `week_2_september_2025`

---

### Week 3 (September 15-21, 2025)
**Dates:** Monday Sept 15 - Sunday Sept 21

**Creates table:** `week_3_september_2025`

---

### Week 4 (September 22-28, 2025)
**Dates:** Monday Sept 22 - Sunday Sept 28

**Creates table:** `week_4_september_2025`

---

### Week 5 (September 29-30, 2025)
**Dates:** Monday Sept 29 - Tuesday Sept 30

**Creates table:** `week_5_september_2025`

---

## Database Queries

### List all weekly tables
```sql
\dt
```

### Query Week 1 data
```sql
-- Count total records
SELECT COUNT(*) FROM week_1_september_2025;

-- Count by taxi type
SELECT taxi_type, COUNT(*) as count
FROM week_1_september_2025
GROUP BY taxi_type;

-- Get specific day
SELECT *
FROM week_1_september_2025
WHERE pickup_datetime::date = '2025-09-01'
LIMIT 10;

-- Daily trip counts for the week
SELECT 
    pickup_datetime::date as date,
    taxi_type,
    COUNT(*) as trips
FROM week_1_september_2025
GROUP BY pickup_datetime::date, taxi_type
ORDER BY date, taxi_type;
```

### Query across multiple weeks
```sql
-- Compare Week 1 vs Week 2
SELECT 'Week 1' as week, COUNT(*) as total_trips
FROM week_1_september_2025
UNION ALL
SELECT 'Week 2' as week, COUNT(*) as total_trips
FROM week_2_september_2025;
```

---

## Important Notes

1. **Automatic Table Creation**: Tables are created automatically when you run `--mode weekly`
2. **Overwrite Mode**: Each weekly run overwrites the table for that week (not append)
3. **Week Calculation**: Based on day of month (Days 1-7 = Week 1, Days 8-14 = Week 2, etc.)
4. **Clean Workflow**: Clear out daily files after weekly processing to start fresh for next week

---

## Workflow Best Practice

```bash
# Week 1 (Sept 1-7)
python data_extraction.py --mode daily --date 2025-09-01
python data_extraction.py --mode daily --date 2025-09-02
# ... continue for all 7 days
python data_extraction.py --mode weekly  # Creates week_1_september_2025

# Optional: Clean up daily files
rm -rf data/processed/daily/*

# Week 2 (Sept 8-14)
python data_extraction.py --mode daily --date 2025-09-08
python data_extraction.py --mode daily --date 2025-09-09
# ... continue for all 7 days
python data_extraction.py --mode weekly  # Creates week_2_september_2025

# And so on...
```

This keeps your daily directory clean and prevents accidentally mixing weeks!
