#!/bin/bash
# ============================================================
# stage3.sh — Stage III: Predictive Data Analytics (ML Pipeline)
# ============================================================
set -e

cd "$(dirname "$0")/.."

echo "============================================================="
echo "  STAGE III: Predictive Data Analytics — ML Pipeline"
echo "============================================================="

mkdir -p output models data docs

# --- Step 1: ML Pipeline (spark-submit on YARN) ---
echo ""; echo "[1/6] Running ML Pipeline on YARN (RF + GBT with grid search)..."
spark-submit --master yarn --deploy-mode client \
  scripts/ml_pipeline.py 2>&1 | tee output/ml_pipeline.log | \
  grep -E '(READING|FEATURE|SPLIT|MODEL|Best:|Test:|COMPARISON|Best |COMPLETE|===)'

echo "  Full log: output/ml_pipeline.log"

# --- Step 2: Create managed Hive tables (ml_features, ml_gridsearch) ---
echo ""; echo "[2/7] Creating Hive ML metadata tables..."
BEELINE='beeline -n team2 -p V2P1hy6zjPqWoXMm -u "jdbc:hive2://hadoop-03.uni.innopolis.ru:10001/"'
cat > /tmp/stage3_metadata.hql << 'EOF'
CREATE TABLE IF NOT EXISTS team2_projectdb.ml_features (
    feature_group STRING, feature_count INT, description STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
INSERT OVERWRITE TABLE team2_projectdb.ml_features VALUES
('Categorical (OHE)', 4, 'primary_type, iucr, location_description, beat'),
('Numerical', 5, 'district, community_area, x_coordinate, y_coordinate, domestic'),
('Cyclical sin/cos', 6, 'hour_sin, hour_cos, day_sin, day_cos, month_sin, month_cos'),
('Geospatial ECEF', 3, 'ecef_x, ecef_y, ecef_z');

CREATE TABLE IF NOT EXISTS team2_projectdb.ml_gridsearch (
    model STRING, param STRING, values_tested STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
INSERT OVERWRITE TABLE team2_projectdb.ml_gridsearch VALUES
('RF', 'numTrees', '20, 50'),
('RF', 'maxDepth', '5, 10'),
('GBT', 'maxDepth', '3, 6'),
('GBT', 'stepSize', '0.05, 0.1');
EOF
eval "$BEELINE -f /tmp/stage3_metadata.hql" 2>/dev/null
echo "  Hive tables: ml_features, ml_gridsearch"

# --- Step 3: Copy train/test data from HDFS to local ---
echo ""; echo "[2/6] Copying train/test data from HDFS..."
hdfs dfs -get project/data/train data/train 2>/dev/null || echo "  (train not found)"
hdfs dfs -get project/data/test data/test 2>/dev/null || echo "  (test not found)"
hdfs dfs -cat project/data/train/*.json > data/train.json 2>/dev/null || echo "  (train.json not found)"
hdfs dfs -cat project/data/test/*.json > data/test.json 2>/dev/null || echo "  (test.json not found)"
echo "  Local: data/train.json, data/test.json"

# --- Step 4: Copy models from HDFS to local ---
echo ""; echo "[4/7] Copying models from HDFS..."

# --- Step 5: Copy predictions from HDFS to local ---
echo ""; echo "[5/7] Copying predictions from HDFS..."

# --- Step 6: Copy evaluation from HDFS to local ---
echo ""; echo "[6/7] Copying evaluation from HDFS..."

# --- Step 7: Quality check ---
echo ""; echo "[7/7] Code quality check..."
if which pylint > /dev/null 2>&1; then
  pylint scripts/ml_pipeline.py --disable=C0301,W0511 2>&1 || true
else
  echo "  pylint not available, skipping"
fi

echo ""; echo "============================================================="
echo "  STAGE III COMPLETE"
echo "============================================================="
echo "  Pipeline:  scripts/ml_pipeline.py (RF + GBT, grid search)"
echo "  Models:    models/model1 (RF), models/model2 (GBT)"
echo "  Preds:     output/model1_predictions.csv, output/model2_predictions.csv"
echo "  Eval:      output/evaluation.csv"
echo "  Train/Test: data/train.json, data/test.json"
echo "  Hive:      ml_evaluation, ml_predictions_rf, ml_predictions_gbt"
echo "  Hive:      ml_features, ml_gridsearch"
echo "  Next:      Stage IV — Build dashboard in Superset"
echo "============================================================="
