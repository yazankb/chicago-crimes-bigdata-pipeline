# Stage II — Hive Optimizations & EDA
## Chicago Crimes Analysis Pipeline (Team 2)

**Authors:** Yazan Kbaili, Ali Salloum  
**Course:** Big Data — IU Hadoop Cluster Project  
**Date:** May 2026

---

## 1. Hive Optimizations

### Database Setup
The Hive database `team2_projectdb` was configured with its warehouse at `/user/team2/project/hive/warehouse`. External tables were created on the Sqoop-imported Parquet data from Stage I.

### Partitioned + Bucketed Table
A managed table `crimes_optimized` was created with:
- **Partitioning**: By `year` (26 partitions: 2001–2026) — enables time-range pruning for queries
- **Bucketing**: By `district` into 22 buckets — enables efficient join/sampling by district
- **Storage**: Parquet + Snappy compression (same as Sqoop import)

| Property | Value |
|----------|-------|
| Rows | 8,549,662 |
| Partitions | 26 |
| Buckets | 22 (by district) |
| Files | 370 |
| Insert method | Dynamic partitioning (nonstrict mode) |

The original unpartitioned `crimes` table was dropped after migration — all EDA uses the optimized table.

---

## 2. EDA — 7 Insights

Exploratory Data Analysis was performed via PySpark (reading from the optimized Parquet path). Results were stored in both CSV files (`output/q1.csv`..`q7.csv`) and PostgreSQL tables (`q1_results`..`q7_results`) for Superset consumption.

| # | Insight | Key Finding |
|---|---------|-------------|
| q1 | Arrest rate by district | District 11 highest (40.89%), District 16 lowest (18.19%) |
| q2 | Top 15 crime types | THEFT (1.82M), BATTERY (1.56M), CRIMINAL DAMAGE (0.97M) |
| q3 | Crimes by hour | Peaks at 12:00 (noon) and 18:00 (evening) |
| q4 | Arrest rate trend (2001–2026) | Declining from 29.21% (2001) to ~12-16% (2020s) |
| q5 | Crimes by month | Seasonal variation with summer peaks |
| q6 | Arrest rate by community area | Wide variation across Chicago's 77 community areas |
| q7 | Domestic vs non-domestic | Non-domestic (26.32%) higher arrest rate than domestic (19.22%) |

---

## 3. Superset Dashboard

A dashboard **"Chicago Crimes Analysis"** was built in Apache Superset containing 7 charts:

| Chart | Type |
|-------|------|
| Arrest Rate by District | Bar |
| Top 15 Crime Types | Horizontal Bar |
| Crimes by Hour of Day | Bar |
| Arrest Rate Trend (2001-2026) | Line |
| Crimes by Month | Bar |
| Arrest Rate by Community Area | Bar |
| Domestic vs Non-Domestic | Bar |

**Dashboard exported as**: `output/dashboard.jpg`

---

## 4. Files Created

| File | Purpose |
|------|---------|
| `sql/db.hql` | Hive DDL (partitioned+bucketed table, views) |
| `scripts/eda_pyspark.py` | 7 EDA queries → CSVs + PostgreSQL |
| `scripts/stage2.sh` | Stage II orchestrator |
| `docs/superset_instructions.md` | Superset setup guide |
| `output/q1.csv`..`q7.csv` | EDA query results |
| `output/q1.jpg`..`q7.jpg` | Superset chart exports |
| `output/dashboard.jpg` | Dashboard export |
| `CHECKLIST.md` | Full task tracker |

---

## 5. Architecture

```
Stage I:
  Socrata API → CSV → PostgreSQL → Sqoop (Parquet+Snappy) → HDFS

Stage II:
  Hive (partitioned+bucketed) → PySpark EDA → CSVs + PostgreSQL → Superset Dashboard
```

---

*Stage II complete. Ready for Stage III.*
