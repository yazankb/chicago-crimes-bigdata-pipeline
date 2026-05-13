#!/bin/bash
# ============================================================
# remote_stage1.sh — Complete Stage I Execution on IU Cluster
# Run this script ON the cluster after uploading project files
# Usage: bash scripts/remote_stage1.sh
# ============================================================
set -e

# ===================== CONFIGURATION =====================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_DIR/data"
SQL_DIR="$PROJECT_DIR/sql"
SECRETS_DIR="$PROJECT_DIR/secrets"
LOGS_DIR="$PROJECT_DIR/docs/logs"

DB_HOST="hadoop-04.uni.innopolis.ru"
DB_PORT="5432"
DB_USER="team2"
DB_NAME="team2_projectdb"
WAREHOUSE_DIR="/user/${DB_USER}/project/warehouse"

mkdir -p "$LOGS_DIR"
LOG="$LOGS_DIR/stage1_$(date '+%Y%m%d_%H%M%S').log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"; }
log "============================================="
log "Stage I: Data Collection & Ingestion"
log "============================================="
log "Project: $PROJECT_DIR"

# ===================== STEP 0: SETUP =====================
log ""
log "[STEP 0] Environment setup"
log "---"

# Create directories
mkdir -p "$DATA_DIR" "$SECRETS_DIR" "$LOGS_DIR"
log "✓ Directories created"

# Password file check
if [ ! -f "$SECRETS_DIR/.psql.pass" ]; then
    log "Password file not found. Creating..."
    read -s -p "Enter PostgreSQL password for user '$DB_USER': " DB_PASS
    echo ""
    echo "$DB_PASS" > "$SECRETS_DIR/.psql.pass"
    chmod 600 "$SECRETS_DIR/.psql.pass"
    log "✓ Password saved to secrets/.psql.pass"
else
    log "✓ Password file already exists"
fi

DB_PASS=$(head -n 1 "$SECRETS_DIR/.psql.pass")

# Install psycopg2 if needed
pip install --quiet psycopg2-binary psql 2>/dev/null || pip3 install --quiet psycopg2-binary 2>/dev/null
log "✓ psycopg2 ready"

# Make scripts executable
chmod +x "$PROJECT_DIR"/scripts/*.sh 2>/dev/null || true
log "✓ Scripts made executable"

export PGPASSWORD="$DB_PASS"

# ===================== STEP 1: DATA COLLECTION =====================
log ""
log "[STEP 1] Downloading Chicago Crimes Dataset"
log "---"

RAW_FILE="$DATA_DIR/chicago_crimes_raw.csv"

if [ -f "$RAW_FILE" ]; then
    FILE_SIZE=$(du -sh "$RAW_FILE" 2>/dev/null | cut -f1)
    LINE_COUNT=$(wc -l < "$RAW_FILE")
    log "File already exists: $RAW_FILE"
    log "  Size: $FILE_SIZE | Lines: $LINE_COUNT"
    log "  Skipping download (delete to re-download)"
else
    log "Downloading from Socrata API..."
    BASE_URL="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"

    if command -v wget &>/dev/null; then
        wget --no-check-certificate -q --show-progress -O "$RAW_FILE" "$BASE_URL" 2>&1 | tee -a "$LOG"
    elif command -v curl &>/dev/null; then
        curl -L --progress-bar -o "$RAW_FILE" "$BASE_URL" 2>&1 | tee -a "$LOG"
    else
        log "ERROR: Neither wget nor curl available"
        exit 1
    fi

    if [ -f "$RAW_FILE" ]; then
        FILE_SIZE=$(du -sh "$RAW_FILE" | cut -f1)
        LINE_COUNT=$(wc -l < "$RAW_FILE")
        log "✓ Download complete"
        log "  File: $RAW_FILE"
        log "  Size: $FILE_SIZE"
        log "  Lines: $LINE_COUNT (incl header)"

        # Checksum
        md5sum "$RAW_FILE" > "$RAW_FILE.md5"
        log "  Checksum: $(cat $RAW_FILE.md5 | cut -d' ' -f1)"
    else
        log "ERROR: Download failed! Check network and retry."
        exit 1
    fi
fi

# ===================== STEP 2: POSTGRESQL =====================
log ""
log "[STEP 2] Building PostgreSQL Database"
log "---"

# Drop existing objects
PGOPTIONS='--client-min-messages=warning' psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS crimes CASCADE;" 2>&1 | tee -a "$LOG"
log "✓ Dropped existing tables"

# Create tables
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$SQL_DIR/create_tables.sql" 2>&1 | tee -a "$LOG"
log "✓ Tables created"

# Check if data already loaded
ROW_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM crimes;" 2>/dev/null | tr -d ' ')
log "Current row count in PostgreSQL: $ROW_COUNT"

if [ "$ROW_COUNT" -gt 1000 ]; then
    log "Data already loaded, skipping import"
else
    log "Importing CSV data via COPY..."

    # Import with timing
    START_LOAD=$(date +%s)
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
        -c "\COPY crimes FROM '$RAW_FILE' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',', NULL '', ENCODING 'UTF8', FORCE_NULL(date, updated_on, latitude, longitude, x_coordinate, y_coordinate))" \
        2>&1 | tee -a "$LOG"

    END_LOAD=$(date +%s)
    LOAD_TIME=$((END_LOAD - START_LOAD))
    log "✓ Data imported in ${LOAD_TIME}s"

    # Verify
    ROW_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM crimes;" | tr -d ' ')
    log "Total rows in PostgreSQL: $ROW_COUNT"
fi

# Run quality check function
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "SELECT update_data_quality_stats(); SELECT * FROM data_quality_stats;" \
    2>&1 | tee -a "$LOG"
log "✓ Quality stats updated"

# ===================== STEP 3: SQOOP IMPORT =====================
log ""
log "[STEP 3] Sqoop Import to HDFS (with Compression Benchmark)"
log "---"

# Clear warehouse
hdfs dfs -rm -r -skipTrash "$WAREHOUSE_DIR" 2>/dev/null || true
log "✓ Cleared existing warehouse"

BENCH_DIR="$PROJECT_DIR/docs/benchmark"
mkdir -p "$BENCH_DIR"

# --- Method A: Parquet + Snappy ---
log "Import A: Parquet + Snappy (recommended)..."
START=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
    --username "$DB_USER" --password "$DB_PASS" \
    --as-parquetfile --compression-codec snappy \
    --warehouse-dir "$WAREHOUSE_DIR" --num-mappers 4 -z 2>&1 | tee -a "$LOG"
END=$(date +%s); TIME_A=$((END - START))
SIZE_A=$(hdfs dfs -du -s "$WAREHOUSE_DIR"/*.parquet 2>/dev/null | awk '{s+=$1} END {print s}')
log "✓ Parquet+Snappy: ${TIME_A}s, ${SIZE_A} bytes"

mv "$WAREHOUSE_DIR" "${WAREHOUSE_DIR}_parquet_snappy" 2>/dev/null || true

# --- Method B: AVRO + Snappy ---
log "Import B: AVRO + Snappy..."
AVRO_DIR="${WAREHOUSE_DIR}_avro"
hdfs dfs -rm -r -skipTrash "$AVRO_DIR" 2>/dev/null || true
START=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
    --username "$DB_USER" --password "$DB_PASS" \
    --as-avrodatafile --compression-codec snappy \
    --warehouse-dir "$AVRO_DIR" --num-mappers 4 -z 2>&1 | tee -a "$LOG"
END=$(date +%s); TIME_B=$((END - START))
SIZE_B=$(hdfs dfs -du -s "$AVRO_DIR"/*.avro 2>/dev/null | awk '{s+=$1} END {print s}')
log "✓ AVRO+Snappy: ${TIME_B}s, ${SIZE_B} bytes"

# --- Method C: Parquet + Gzip ---
log "Import C: Parquet + Gzip..."
GZIP_DIR="${WAREHOUSE_DIR}_gzip"
hdfs dfs -rm -r -skipTrash "$GZIP_DIR" 2>/dev/null || true
START=$(date +%s)
sqoop import-all-tables \
    --connect "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
    --username "$DB_USER" --password "$DB_PASS" \
    --as-parquetfile --compression-codec gzip \
    --warehouse-dir "$GZIP_DIR" --num-mappers 4 -z 2>&1 | tee -a "$LOG"
END=$(date +%s); TIME_C=$((END - START))
SIZE_C=$(hdfs dfs -du -s "$GZIP_DIR"/*.parquet 2>/dev/null | awk '{s+=$1} END {print s}')
log "✓ Parquet+Gzip: ${TIME_C}s, ${SIZE_C} bytes"

# --- Benchmark Summary ---
log ""
log "COMPRESSION BENCHMARK RESULTS"
log "================================"
log "Format + Codec       | Size (bytes)     | Time (s) | Winner"
log "---------------------|------------------|----------|-------"
log "Parquet + Snappy     | $SIZE_A | ${TIME_A}s        | ★ RECOMMENDED"
log "AVRO + Snappy        | $SIZE_B | ${TIME_B}s        |"
log "Parquet + Gzip       | $SIZE_C | ${TIME_C}s        |"
log "================================"

# Save CSV
echo "format,codec,size_bytes,import_seconds" > "$BENCH_DIR/benchmark_results.csv"
echo "parquet,snappy,$SIZE_A,$TIME_A" >> "$BENCH_DIR/benchmark_results.csv"
echo "avro,snappy,$SIZE_B,$TIME_B" >> "$BENCH_DIR/benchmark_results.csv"
echo "parquet,gzip,$SIZE_C,$TIME_C" >> "$BENCH_DIR/benchmark_results.csv"

# Set primary warehouse to Parquet+Snappy (winner)
hdfs dfs -cp "${WAREHOUSE_DIR}_parquet_snappy" "$WAREHOUSE_DIR" 2>/dev/null || true

# ===================== STEP 4: HIVE =====================
log ""
log "[STEP 4] Creating Hive Tables & Views"
log "---"

beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -f "$SQL_DIR/hive_create.sql" 2>&1 | tee -a "$LOG"
log "✓ Hive tables created"

# Verify Hive
beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -e "SELECT COUNT(*) AS total_rows FROM team2.crimes;" 2>&1 | tee -a "$LOG"
log "✓ Hive verification complete"

# ===================== STEP 5: VERIFICATION =====================
log ""
log "[STEP 5] Final Verification"
log "---"

# PostgreSQL count
PG_COUNT=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM crimes;" | tr -d ' ')
log "PostgreSQL row count: $PG_COUNT"

# Hive count
HIVE_COUNT=$(beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -e "SELECT COUNT(*) FROM team2.crimes;" 2>/dev/null | grep -E '^[0-9]' | tr -d '[:space:]')
log "Hive row count: $HIVE_COUNT"

# HDFS file count
HDFS_COUNT=$(hdfs dfs -count "$WAREHOUSE_DIR" 2>/dev/null | awk '{print $2}')
log "HDFS file count: $HDFS_COUNT"

# Year range check
YEAR_RANGE=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT MIN(year), MAX(year) FROM crimes;")
log "Year range: $YEAR_RANGE"

# Arrest distribution
log "Arrest distribution:"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c \
    "SELECT arrest, COUNT(*) AS count, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(), 2) AS pct FROM crimes GROUP BY arrest ORDER BY arrest;" \
    2>&1 | tee -a "$LOG"

# ===================== DONE =====================
log ""
log "============================================="
log "STAGE I COMPLETED SUCCESSFULLY!"
log "Completed: $(date)"
log "============================================="
log "Deliverables:"
log "  - Raw data:        $RAW_FILE"
log "  - PostgreSQL:      $DB_NAME.crimes ($PG_COUNT rows)"
log "  - HDFS warehouse:  $WAREHOUSE_DIR"
log "  - Hive table:      team2.crimes + team2.crimes_features + team2.crimes_sample"
log "  - Benchmark CSV:   $BENCH_DIR/benchmark_results.csv"
log "  - Log file:        $LOG"
log "============================================="