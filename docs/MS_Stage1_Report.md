# Stage I — Data Collection and Ingestion
## Chicago Crimes Analysis Pipeline (Team 2)

**Authors:** Yazan Kbaili, Ali Salloum  
**Course:** Big Data — IU Hadoop Cluster Project  
**Date:** May 2026

---

## 1. Business Problem

City agencies need data-driven prioritization of patrol and response resources. Manual analysis of 8.5+ million crime records is infeasible. The goal is to identify high-risk spatiotemporal patterns and predict arrest likelihood to improve resource allocation.

## 2. Dataset Overview

| Attribute | Value |
|-----------|-------|
| Source | [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) |
| Records | 8,523,527 |
| Columns | 22 |
| Uncompressed Size | ~2.3 GB (CSV) |
| Formats | CSV, JSON |
| Primary Key | `id` |
| Target Variable | `arrest` (Boolean) |
| Geospatial Fields | `latitude`, `longitude` |
| Temporal Fields | `date`, `updated_on`, `year` |

## 3. Pipeline Architecture

```
Socrata API / CSV ──▶ PostgreSQL (Staging) ──▶ Sqoop ──▶ HDFS (Parquet+Snappy) ──▶ Hive
                                                                                          │
                                                                                          ▼
                                                                              PySpark ML Pipeline
                                                                              (Stage II)
```

## 4. Tools & Technologies

| Component | Tool | Purpose |
|-----------|------|---------|
| Data Source | Socrata SODA API | Bulk CSV download |
| RDBMS | PostgreSQL 14 | Staging & schema validation |
| Ingestion | Apache Sqoop | RDBMS → HDFS bridge |
| Storage | HDFS | Distributed file storage |
| Format | Apache Parquet | Columnar storage with Snappy compression |
| Catalog | Apache Hive | Schema-on-read with external tables |
| Compute | Apache Spark 3 | EDA and ML pipeline |
| Visualization | Apache Superset | Dashboard creation |

## 5. Execution Results

| Metric | Value |
|--------|-------|
| Total records | 8,549,662 |
| CSV size | 1.9 GB |
| PostgreSQL rows | 8,549,662 |
| HDFS format | Parquet + Snappy |
| HDFS size (Parquet+Snappy) | 509 MB |
| Sqoop import time (Parquet+Snappy) | 54s (4 mappers) |
| Class balance (TRUE) | 25.09% |
| Class balance (FALSE) | 74.91% |
| Year range | 2001–2026 |

### Sqoop Compression Benchmark

| Format | Codec | Size | Import Time |
|--------|-------|------|-------------|
| Parquet | Snappy | 509 MB | 54s |
| AVRO | Snappy | 845 MB | 84s |
| Parquet | Gzip | 360 MB | 93s |

**Recommendation:** Parquet + Snappy — best balance of storage efficiency and import speed.

### Hive Tables Created
| Table | Type | Rows |
|-------|------|------|
| `team2_projectdb.crimes` | External (Parquet) | 8,549,662 |
| `team2_projectdb.crimes_optimized`| Managed (partitioned+bucketed) | 8,549,662 |
| `team2_projectdb.crimes_features` | View | 8,453,109 |
| `team2_projectdb.crimes_sample` | Managed (Parquet) | 850,000 |
| `team2_projectdb.arrest_balance` | View | — |

## 6. Prerequisites

- IU Hadoop Cluster access (`hadoop-01.uni.innopolis.ru`)
- Credentials: user `team2`, password `V2P1hy6zjPqWoXMm`
- PostgreSQL client tools (`psycopg2`)
- Sqoop client installed on cluster (`/usr/bin/sqoop`)
- PostgreSQL JDBC driver: `/shared/postgresql-42.6.1.jar`
- HiveServer2: `hadoop-03.uni.innopolis.ru:10001`
- Python 3.6 with PySpark and psycopg2

## 6. Execution Steps

### Step 1: Data Collection
```bash
bash scripts/data_collection.sh
```
- Downloads full dataset from Socrata API
- Output: `data/chicago_crimes_raw.csv` (~2.3 GB)
- Includes MD5 checksum verification
- Fallback: paginated API in batches of 500k rows

### Step 2: PostgreSQL Database Build
```bash
python3 scripts/build_db.py
```
- Creates `crimes` table with 22 columns + quality stats function
- Bulk loads via `COPY FROM STDIN` (fastest method)
- Indexes on `arrest`, `year`, `primary_type`, `date`, `district`, `community_area`
- Idempotent: safe to rerun

### Step 3: Sqoop Import to HDFS
```bash
bash scripts/sqoop_import.sh
```
- Tests 3 format/codec combinations:
  - Parquet + Snappy (★ recommended)
  - AVRO + Snappy
  - Parquet + Gzip
- Benchmarks: import time, file size, query performance
- Output: `/user/team2/project/warehouse/`

### Step 4: Hive Table Creation
```bash
beeline -n team2 -p "V2P1hy6zjPqWoXMm" \
  -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/" \
  -f sql/hive_create.sql
```
- External table `crimes` partitioned by year
- View `crimes_features` with datetime cyclical encoding
- Stratified sample table `crimes_sample` (~850k records, ~10%)
- View `arrest_balance` for class distribution

### Step 5: Verification
```bash
bash scripts/verify.sh
```
- File-level checks (size, row count, column count)
- Header validation
- Null/empty value analysis
- Year range verification (2001–present)
- Duplicate ID detection
- MD5 checksum

## 8. Schema Design

### PostgreSQL `crimes` Table
| Column | Type | Notes |
|--------|------|-------|
| id | BIGINT PK | Unique record identifier |
| case_number | VARCHAR(20) | Case reference |
| date | TIMESTAMP | Incident timestamp |
| block | VARCHAR(200) | Address block |
| iucr | VARCHAR(10) | Illinois Uniform Crime Reporting code |
| primary_type | VARCHAR(50) | Crime category (NOT NULL) |
| description | VARCHAR(300) | Detailed description |
| location_description | VARCHAR(100) | Location context |
| arrest | BOOLEAN | **Target variable** |
| domestic | BOOLEAN | Domestic incident flag |
| beat | VARCHAR(10) | Police beat area |
| district | SMALLINT | Police district (1-25) |
| ward | SMALLINT | City ward |
| community_area | SMALLINT | Chicago community area (1-77) |
| fbi_code | VARCHAR(10) | FBI UCR code |
| x_coordinate | INTEGER | X projection coordinate |
| y_coordinate | INTEGER | Y projection coordinate |
| year | SMALLINT | Incident year |
| updated_on | TIMESTAMP | Last update timestamp |
| latitude | DOUBLE PRECISION | GPS latitude |
| longitude | DOUBLE PRECISION | GPS longitude |
| location | VARCHAR(300) | Raw location string |

### Hive Partitioning Strategy
- **Partition key:** `year` (enables efficient time-range queries)
- **File format:** Parquet (columnar, splittable)
- **Compression:** Snappy (fast decompression for Spark)
- **Storage location:** `/user/team2/project/warehouse/`

## 9. Benchmarking Results

| Combination | Size | Import Time | Notes |
|-------------|------|-------------|-------|
| Parquet + Snappy | 509 MB | 54s | ★ Recommended |
| AVRO + Snappy | 845 MB | 84s | Row-oriented |
| Parquet + Gzip | 360 MB | 93s | Best compression, slowest |

## 10. Data Quality Considerations

- **Null handling:** Socrata exports empty strings for nulls → use `NULL ''` in COPY
- **Geospatial validation:** Chicago bounding box: lat [41.6, 42.1], lon [-87.9, -87.5]
- **Class imbalance:** Expected ~20-25% arrest rate → handle in Stage II with SMOTE/class weights
- **Year coverage:** 2001–present, with newer records potentially incomplete

## 11. Stage II Handoff

For Stage II:
- Feature-ready Hive table: `crimes_features` (with cyclical datetime encoding)
- Stratified sample: `crimes_sample` (~850k records)
- Class balance analysis available via `arrest_balance` view
- Community area polygons can be joined for geospatial features
- Datetime features pre-computed: `hour_sin`, `hour_cos`, `day_sin`, `day_cos`, `month_sin`, `month_cos`

## 12. Reproducibility

All scripts are idempotent and can be rerun safely:
- Drop IF EXISTS before CREATE
- Clear HDFS before import
- Checksums for data integrity
- Shell scripts executable via `bash scripts/xxx.sh`
- Python scripts standalone via `python3 scripts/xxx.py`

---

*Stage I complete. Ready for Stage II: ML Pipeline.*