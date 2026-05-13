#!/bin/bash
# ============================================================
# RUN THIS SCRIPT ON THE IU HADOOP CLUSTER
# Paste into JupyterLab Terminal or SSH session on hadoop-01
# DO NOT run from your local PC
# ============================================================
set -e

echo "============================================================="
echo "  Stage I: Chicago Crimes Pipeline - Full Execution"
echo "  Team 2 - Big Data Project"
echo "============================================================="
echo ""

# --- CONFIG ---
PROJECT_DIR="$HOME/project/chicago-crimes-pipeline"
mkdir -p "$PROJECT_DIR"/{scripts,sql,data,secrets,notebooks,docs/logs}
cd "$PROJECT_DIR"

# --- STEP 0: Password ---
echo "[STEP 0] Setting up database credentials..."
read -s -p "Enter PostgreSQL password (from readme.txt): " PW
echo ""
echo "$PW" > secrets/.psql.pass
chmod 600 secrets/.psql.pass
export PGPASSWORD="$PW"
echo "  Done."
echo ""

# --- STEP 1: Download Data ---
echo "[STEP 1/5] Downloading Chicago Crimes Dataset..."
echo "  Source: Socrata API (~2.3 GB, ~8.5M rows)"
echo "  Expected time: 15-30 minutes"

if [ -f "data/chicago_crimes_raw.csv" ]; then
    echo "  File already exists. Checking..."
    LINES=$(wc -l < data/chicago_crimes_raw.csv)
    echo "  Found $LINES lines. Skipping download."
else
    echo "  Downloading..."
    URL="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"
    if command -v wget &>/dev/null; then
        wget --no-check-certificate --progress=bar:force -O data/chicago_crimes_raw.csv "$URL" 2>&1
    else
        curl -L --progress-bar -o data/chicago_crimes_raw.csv "$URL"
    fi
    LINES=$(wc -l < data/chicago_crimes_raw.csv)
    SIZE=$(du -sh data/chicago_crimes_raw.csv 2>/dev/null | cut -f1 || echo "?")
    echo "  Downloaded! Lines: $LINES, Size: $SIZE"
    md5sum data/chicago_crimes_raw.csv > data/chicago_crimes_raw.csv.md5
fi
echo ""

# --- STEP 2: Build PostgreSQL ---
echo "[STEP 2/5] Building PostgreSQL database..."
echo "  Dropping existing tables..."
psql -h hadoop-04.uni.innopolis.ru -d team2_projectdb -c "DROP TABLE IF EXISTS crimes CASCADE;" 2>&1

echo "  Creating tables..."
psql -h hadoop-04.uni.innopolis.ru -d team2_projectdb -f sql/create_tables.sql 2>&1

echo "  Importing CSV data (this may take 5-10 minutes)..."
START_LOAD=$(date +%s)
psql -h hadoop-04.uni.innopolis.ru -d team2_projectdb \
    -c "\COPY crimes FROM '$(pwd)/data/chicago_crimes_raw.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',', NULL '', ENCODING 'UTF8', FORCE_NULL(date, updated_on, latitude, longitude, x_coordinate, y_coordinate))" \
    2>&1

END_LOAD=$(date +%s)
LOAD_TIME=$((END_LOAD - START_LOAD))

ROW_COUNT=$(psql -h hadoop-04.uni.innopolis.ru -t -d team2_projectdb -c "SELECT COUNT(*) FROM crimes;" | tr -d ' ')
echo "  Loaded $ROW_COUNT rows in ${LOAD_TIME}s"

echo "  Running quality stats..."
psql -h hadoop-04.uni.innopolis.ru -d team2_projectdb -c "SELECT update_data_quality_stats(); SELECT * FROM data_quality_stats;" 2>&1
echo ""
echo "  Arrest distribution:"
psql -h hadoop-04.uni.innopolis.ru -d team2_projectdb -c "SELECT arrest, COUNT(*) AS cnt, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 2) AS pct FROM crimes GROUP BY arrest;" 2>&1
echo ""
echo "  Year range:"
psql -h hadoop-04.uni.innopolis.ru -t -d team2_projectdb -c "SELECT MIN(year), MAX(year), COUNT(DISTINCT year) FROM crimes;" 2>&1
echo "  ✓ PostgreSQL ready!"
echo ""

# --- STEP 3: Sqoop Import ---
echo "[STEP 3/5] Sqoop Import to HDFS (Parquet + Snappy)"
echo "  Running benchmark of 3 formats..."
echo "  Expected time: 20-40 minutes"

echo "  3a. Creating HDFS directories..."
hdfs dfs -rm -r -skipTrash /user/team2/project/warehouse_parquet 2>/dev/null || true
hdfs dfs -rm -r -skipTrash /user/team2/project/warehouse_avro   2>/dev/null || true
hdfs dfs -rm -r -skipTrash /user/team2/project/warehouse_gzip   2>/dev/null || true

echo "  3b. Parquet + Snappy (recommended)..."
START_A=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru:5432/team2_projectdb" \
    --username team2 --password "$PW" \
    --as-parquetfile --compression-codec snappy \
    --warehouse-dir /user/team2/project/warehouse_parquet \
    --num-mappers 4 -z 2>&1
END_A=$(date +%s); TIME_A=$((END_A - START_A))
echo "  ✓ Parquet+Snappy: ${TIME_A}s"

echo "  3c. AVRO + Snappy..."
START_B=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru:5432/team2_projectdb" \
    --username team2 --password "$PW" \
    --as-avrodatafile --compression-codec snappy \
    --warehouse-dir /user/team2/project/warehouse_avro \
    --num-mappers 4 -z 2>&1
END_B=$(date +%s); TIME_B=$((END_B - START_B))
echo "  ✓ AVRO+Snappy: ${TIME_B}s"

echo "  3d. Parquet + Gzip..."
START_C=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://hadoop-04.uni.innopolis.ru:5432/team2_projectdb" \
    --username team2 --password "$PW" \
    --as-parquetfile --compression-codec gzip \
    --warehouse-dir /user/team2/project/warehouse_gzip \
    --num-mappers 4 -z 2>&1
END_C=$(date +%s); TIME_C=$((END_C - START_C))
echo "  ✓ Parquet+Gzip: ${TIME_C}s"

echo ""
echo "  BENCHMARK RESULTS:"
echo "  Format             Size(bytes)      Time(s)"
echo "  ------------------ ---------------- --------"
echo "  Parquet + Snappy   (see below)      $TIME_A"
echo "  AVRO + Snappy      (see below)      $TIME_B"
echo "  Parquet + Gzip     (see below)      $TIME_C"

# Save benchmark CSV
mkdir -p docs/benchmark
echo "format,codec,size_bytes,import_seconds" > docs/benchmark/benchmark_results.csv
echo "parquet,snappy,,${TIME_A}" >> docs/benchmark/benchmark_results.csv
echo "avro,snappy,,${TIME_B}" >> docs/benchmark/benchmark_results.csv
echo "parquet,gzip,,${TIME_C}" >> docs/benchmark/benchmark_results.csv

# Use Parquet+Snappy as primary (recommended)
echo "  Setting Parquet+Snappy as primary warehouse..."
hdfs dfs -cp /user/team2/project/warehouse_parquet/* /user/team2/project/warehouse/ 2>/dev/null || \
    hdfs dfs -mkdir -p /user/team2/project/warehouse && \
    hdfs dfs -cp /user/team2/project/warehouse_parquet /user/team2/project/warehouse 2>&1
echo "  ✓ Primary warehouse: /user/team2/project/warehouse (Parquet+Snappy)"
echo ""

# --- STEP 4: Hive ---
echo "[STEP 4/5] Creating Hive Tables & Views..."
echo "  - External table 'crimes' (partitioned by year)"
echo "  - View 'crimes_features' (with cyclical datetime encoding)"
echo "  - Table 'crimes_sample' (~850k stratified sample)"
echo "  - View 'arrest_balance'"

beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -f sql/hive_create.sql 2>&1

echo "  Verifying Hive table..."
beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -e \
    "SELECT COUNT(*) AS total FROM team2.crimes; SELECT * FROM team2.arrest_balance;" 2>&1

echo "  ✓ Hive tables created and verified!"
echo ""

# --- STEP 5: Final Verification ---
echo "[STEP 5/5] Final Verification"
echo "  Checking HDFS..."
hdfs dfs -du -s /user/team2/project/warehouse 2>&1
echo ""
echo "  Checking Hive sample table..."
beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -e \
    "SELECT COUNT(*) AS sample_count FROM team2.crimes_sample; SELECT * FROM team2.crimes_features LIMIT 3;" 2>&1
echo ""
echo "  Checking geospatial bounds (Chicago area)..."
beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -e \
    "SELECT MIN(latitude) min_lat, MAX(latitude) max_lat, MIN(longitude) min_lon, MAX(longitude) max_lon FROM team2.crimes WHERE latitude IS NOT NULL;" 2>&1
echo ""

echo "============================================================="
echo "  STAGE I COMPLETE 🎉"
echo "============================================================="
echo ""
echo "  Delivered:"
echo "    ├── PostgreSQL:    team2_projectdb.crimes ($ROW_COUNT rows)"
echo "    ├── HDFS:          /user/team2/project/warehouse/ (Parquet+Snappy)"
echo "    ├── Hive tables:   team2.crimes (external, partitioned by year)"
echo "    ├── Hive view:     team2.crimes_features (with datetime features)"
echo "    ├── Hive sample:   team2.crimes_sample (~850k records)"
echo "    ├── Hive view:     team2.arrest_balance"
echo "    ├── Benchmark:     docs/benchmark/benchmark_results.csv"
echo "    └── Raw data:      data/chicago_crimes_raw.csv"
echo ""
echo "  NEXT: Stage II — PySpark ML Pipeline"
echo "    → notebooks/ml_pipeline.ipynb"
echo "    → Logistic Regression + Random Forest + GBT"
echo "    → Feature engineering with cyclical datetime encoding"
echo "============================================================="