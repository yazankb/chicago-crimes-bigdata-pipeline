#!/bin/bash
# ============================================================
# stage4.sh — Stage IV: Presentation & Delivery
# Hive tables already created by Stage III pipeline
# ============================================================
set -e

cd "$(dirname "$0")/.."

echo "============================================================="
echo "  STAGE IV: Presentation & Delivery"
echo "============================================================="

mkdir -p output docs

# --- Step 1: Load PostgreSQL tables for Superset ---
echo ""; echo "[1/2] Loading PostgreSQL tables..."
python3 scripts/load_postgres.py 2>&1 | grep -E "(OK|ERROR|Verification|rows)"
echo "  PostgreSQL tables loaded"

# --- Step 2: Quality check ---
echo ""; echo "[2/2] Code quality check..."
if which pylint > /dev/null 2>&1; then
  pylint scripts/load_postgres.py --disable=C0301,W0511 2>&1 | grep -E "rated at|Your code"
fi

echo ""; echo "============================================================="
echo "  STAGE IV SETUP COMPLETE"
echo "============================================================="
echo "  Hive tables (from Stage III):"
echo "    ml_evaluation, ml_predictions_rf, ml_predictions_gbt"
echo "    ml_features, ml_gridsearch"
echo "  PostgreSQL tables:"
echo "    ml_evaluation, dataset_info, schema_info, ml_features, ml_gridsearch"
echo ""
echo "  Manual Superset steps:"
echo "  1. http://hadoop-03.uni.innopolis.ru:8808"
echo "     team2@innopolis.university / V2P1hy6zjPqWoXMm"
echo "  2. Add Hive DB: Data > Databases > +"
echo "     hive://hadoop-03.uni.innopolis.ru:10001/team2_projectdb"
echo "  3. Refresh datasets, build/enhance dashboard"
echo "  4. Publish and export output/dashboard_stage4.jpg"
echo "============================================================="
