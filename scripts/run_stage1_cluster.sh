#!/bin/bash
# ============================================================
# run_stage1_cluster.sh — Execute entire Stage I on IU Hadoop Cluster
# Run from local machine via: ssh team2@hadoop-01.uni.innopolis.ru
# ============================================================
set -e

PROJECT_DIR="$HOME/project"
echo "============================================="
echo "Stage I: Data Collection & Ingestion"
echo "Project dir: $PROJECT_DIR"
echo "============================================="
cd "$PROJECT_DIR"

# Step 0: Setup password
echo ""
echo "[0/6] Setting up database password..."
read -s -p "Enter PostgreSQL password: " DB_PASS
echo "$DB_PASS" > secrets/.psql.pass
chmod 600 secrets/.psql.pass
echo "  ✓ Password saved (permissions 600)"

# Step 1: Download data
echo ""
echo "[1/6] Downloading Chicago Crimes dataset..."
bash scripts/data_collection.sh

# Step 2: Build PostgreSQL database
echo ""
echo "[2/6] Building PostgreSQL database..."
python3 scripts/build_db.py

# Step 3: Sqoop import with benchmarking
echo ""
echo "[3/6] Importing to HDFS via Sqoop..."
bash scripts/sqoop_import.sh

# Step 4: Create Hive tables
echo ""
echo "[4/6] Creating Hive tables and views..."
beeline -u "jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2" -f sql/hive_create.sql

# Step 5: Verify
echo ""
echo "[5/6] Running verification..."
bash scripts/verify.sh

# Step 6: Summary
echo ""
echo "============================================="
echo "STAGE I COMPLETE!"
echo "============================================="
echo "Checkpoints:"
echo "  - Raw data:    $PROJECT_DIR/data/chicago_crimes_raw.csv"
echo "  - PostgreSQL:  team2_projectdb.crimes"
echo "  - HDFS:        /user/team2/project/warehouse/"
echo "  - Hive:        team2.crimes (partitioned by year)"
echo "  - Benchmark:   $PROJECT_DIR/docs/benchmark_results.csv"
echo ""
echo "Next: Stage II — ML Pipeline (notebooks/ml_pipeline.ipynb)"
echo "============================================="