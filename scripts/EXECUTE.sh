#!/bin/bash
# ============================================================
# EXECUTE.sh — Stage I: Chicago Crimes Pipeline
# Run on IU Hadoop Cluster (hadoop-01.uni.innopolis.ru)
#   bash scripts/EXECUTE.sh
# ============================================================
set -e

cd "$(dirname "$0")/.."

echo "============================================================="
echo "  STAGE I: Chicago Crimes Pipeline" 
echo "============================================================="

# --- Step 0: Setup ---
echo ""; echo "[0/5] Setting up..."
mkdir -p scripts sql data secrets notebooks docs docs/logs docs/benchmark output
echo "V2P1hy6zjPqWoXMm" > secrets/.psql.pass
chmod 600 secrets/.psql.pass
chmod +x scripts/*.sh

# --- Step 1: Data Collection ---
echo ""; echo "[1/5] Downloading Chicago Crimes dataset..." 
bash scripts/data_collection.sh

# --- Step 2: PostgreSQL Database Build ---
echo ""; echo "[2/5] Building PostgreSQL database..."
bash scripts/data_storage.sh

# --- Step 3: Sqoop Import to HDFS ---
echo ""; echo "[3/5] Sqoop import to HDFS..."
bash scripts/sqoop_import.sh

# --- Step 4: Hive Tables & Views ---
echo ""; echo "[4/5] Creating Hive tables and views..."
export HADOOP_CLASSPATH=/shared/postgresql-42.6.1.jar
beeline -n team2 -p "V2P1hy6zjPqWoXMm" \
  -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/" \
  -f sql/hive_create.sql

# --- Step 5: Verify ---
echo ""; echo "[5/5] Verification..."
echo "  PostgreSQL row count:"
echo "V2P1hy6zjPqWoXMm" | psql -h hadoop-04 -U team2 -d team2_projectdb -c "SELECT COUNT(*) FROM crimes;" 2>/dev/null || \
  python3 -c "import psycopg2; c=psycopg2.connect(host='hadoop-04', dbname='team2_projectdb', user='team2', password='V2P1hy6zjPqWoXMm'); cur=c.cursor(); cur.execute('SELECT COUNT(*) FROM crimes'); print(f'  Rows: {cur.fetchone()[0]}'); c.close()"

echo "  Hive row count:"
beeline -n team2 -p "V2P1hy6zjPqWoXMm" \
  -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/" \
  --outputformat=tsv2 -e "SELECT COUNT(*) FROM team2_projectdb.crimes;" 2>/dev/null

echo "  Arrest balance:"
beeline -n team2 -p "V2P1hy6zjPqWoXMm" \
  -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/" \
  --outputformat=tsv2 -e "SELECT * FROM team2_projectdb.arrest_balance;" 2>/dev/null

echo "  HDFS size:"
hdfs dfs -du -s /user/team2/project/warehouse

echo ""; echo "============================================================="
echo "  STAGE I COMPLETE"
echo "============================================================="
echo "  PostgreSQL: team2_projectdb.crimes"
echo "  HDFS:       /user/team2/project/warehouse/"
echo "  Hive:       team2_projectdb.crimes"
echo "  Next:       Stage II — ML Pipeline (notebooks/ml_pipeline.ipynb)"
echo "============================================================="
