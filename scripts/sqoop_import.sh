#!/bin/bash
# sqoop_import.sh - Import PostgreSQL to HDFS via Sqoop with benchmarks
set -e

DB_HOST="hadoop-04.uni.innopolis.ru"
DB_PORT="5432"
DB_USER="team2"
DB_NAME="team2_projectdb"
WAREHOUSE_DIR="/user/${DB_USER}/project/warehouse"
PASSWORD=$(head -n 1 "secrets/.psql.pass")
JDBC_JAR="/shared/postgresql-42.6.1.jar"

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BENCH_DIR="$PROJECT_DIR/docs/benchmark"
mkdir -p "$BENCH_DIR"

export HADOOP_CLASSPATH=$JDBC_JAR

run_import() {
  local TARGET=$1
  local FORMAT=$2
  local CODEC=$3
  local LABEL=$4
  echo "=== $LABEL ==="
  hdfs dfs -rm -r -skipTrash "$TARGET" 2>/dev/null || true
  START=$(date +%s)
  if [ "$FORMAT" = "avro" ]; then
    sqoop import --connect "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
      --username "$DB_USER" --password "$PASSWORD" \
      --table crimes --split-by id -m 4 \
      --as-avrodatafile --compression-codec $CODEC --compress \
      --warehouse-dir "$TARGET" 2>&1
  else
    sqoop import --connect "jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}" \
      --username "$DB_USER" --password "$PASSWORD" \
      --table crimes --split-by id -m 4 \
      --as-parquetfile --compression-codec $CODEC --compress \
      --warehouse-dir "$TARGET" 2>&1
  fi
  END=$(date +%s)
  TIME=$((END - START))
  SIZE=$(hdfs dfs -du -s "$TARGET/crimes/" 2>/dev/null | awk '{print $1}')
  echo "$LABEL: ${TIME}s, ${SIZE} bytes"
  echo "${FORMAT},${CODEC},${SIZE},${TIME}" >> "$BENCH_DIR/benchmark_results.csv"
}

echo "Clearing warehouse..."
hdfs dfs -rm -r -skipTrash "$WAREHOUSE_DIR" 2>/dev/null || true

echo "format,codec,size_bytes,import_seconds" > "$BENCH_DIR/benchmark_results.csv"

run_import "$WAREHOUSE_DIR" parquet snappy "Parquet+Snappy"
run_import "${WAREHOUSE_DIR}_avro" avro snappy "AVRO+Snappy"
run_import "${WAREHOUSE_DIR}_gzip" parquet gzip "Parquet+Gzip"

hdfs dfs -rm -r -skipTrash "${WAREHOUSE_DIR}_avro" 2>/dev/null || true
hdfs dfs -rm -r -skipTrash "${WAREHOUSE_DIR}_gzip" 2>/dev/null || true

echo ""
echo "Benchmark saved to: $BENCH_DIR/benchmark_results.csv"
cat "$BENCH_DIR/benchmark_results.csv"
echo "Done."
