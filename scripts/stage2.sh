#!/bin/bash
# ============================================================
# stage2.sh — Stage II: Hive Optimizations + EDA + ML Pipeline
# ============================================================
set -e

cd "$(dirname "$0")/.."
PASSWORD="V2P1hy6zjPqWoXMm"
HIVE_URL="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/"
BEELINE="beeline -n team2 -p $PASSWORD -u $HIVE_URL"

echo "============================================================="
echo "  STAGE II: Hive Optimizations, EDA & ML Pipeline"
echo "============================================================="

mkdir -p output docs

# --- Step 1: Hive DDL (partitioned + bucketed table) ---
echo ""; echo "[1/5] Creating Hive database with partitioned+bucketed table..."
$BEELINE -f sql/db.hql > output/hive_results.txt 2>/dev/null
echo "  Output: output/hive_results.txt"

# --- Step 2: PySpark EDA (7 insights) ---
echo ""; echo "[2/5] Running EDA (7 insights)..."
spark-submit --master local[4] --driver-memory 4g scripts/eda_pyspark.py 2>&1 | grep -E '(===|q[1-7]:|Top 15|hour_of_day|community_area|domestic|Total)'
echo "  Outputs: output/q1.csv through output/q7.csv"

# --- Step 3: ML Pipeline ---
echo ""; echo "[3/5] Running ML Pipeline (LR, RF, GBT)..."
spark-submit --master local[4] --driver-memory 6g scripts/ml_pipeline.py 2>&1 | grep -E '(LOGISTIC|RANDOM|GRADIENT|AUC-ROC|AUC-PR|Accuracy|F1-score|MODEL|Feature Import)'
echo "  Results saved in script output"

# --- Step 4: Verify ---
echo ""; echo "[4/5] Verification..."
echo "  Hive row count:"
$BEELINE --outputformat=tsv2 -e "SELECT COUNT(*) FROM team2_projectdb.crimes_optimized;" 2>/dev/null || echo "    (query failed)"

# --- Step 5: Quality check ---
echo ""; echo "[5/5] Code quality check..."
which pylint > /dev/null 2>&1 && pylint scripts/eda_pyspark.py scripts/ml_pipeline.py --disable=C0301,W0511 2>/dev/null || echo "  pylint not available, skipping"

echo ""; echo "============================================================="
echo "  STAGE II COMPLETE"
echo "============================================================="
echo "  Partitioned+Bucketed: team2_projectdb.crimes_optimized"
echo "  EDA results:          output/q1.csv ... q7.csv"
echo "  ML models:            LR, RF, GBT (see output above)"
echo "  Next:                 Superset charts + Dashboard (Stage III/IV)"
echo "============================================================="
