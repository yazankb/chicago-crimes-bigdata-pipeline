#!/bin/bash
# ============================================================
# upload_and_run.sh — Upload files to cluster and run Stage I
# Run from LOCAL machine (requires SSH access)
# ============================================================
set -e

REMOTE_USER="team2"
REMOTE_HOST="10.100.30.57"
REMOTE_BASE="/home/${REMOTE_USER}/project"
REMOTE_DIR="${REMOTE_BASE}/chicago-crimes-pipeline"
LOCAL_DIR="C:\\Users\\язан\\Desktop\\folder\\Big_Data_Project"

echo "================================================"
echo "Upload & Execute Stage I — Chicago Crimes Pipeline"
echo "================================================"

# Create project directory on cluster
echo "[1/5] Creating project directory on cluster..."
ssh ${REMOTE_USER}@${REMOTE_HOST} "mkdir -p ${REMOTE_DIR}/scripts ${REMOTE_DIR}/sql ${REMOTE_DIR}/data ${REMOTE_DIR}/secrets ${REMOTE_DIR}/notebooks ${REMOTE_DIR}/docs"

# Upload all scripts
echo "[2/5] Uploading files to cluster..."
scp "${LOCAL_DIR}/scripts/*.sh"     "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/scripts/"
scp "${LOCAL_DIR}/sql/*.sql"       "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/sql/"
scp "${LOCAL_DIR}/sql/hive_create.sql" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/sql/"
scp "${LOCAL_DIR}/README.md"       "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"
scp "${LOCAL_DIR}/CHECKLIST.md"    "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"
scp "${LOCAL_DIR}/.gitignore"      "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/"

# Run Stage I on cluster
echo "[3/5] Executing Stage I on cluster..."
echo "       This will take 60-120 minutes total (download, import, benchmark)"
ssh ${REMOTE_USER}@${REMOTE_HOST} "cd ${REMOTE_DIR}; bash scripts/remote_stage1.sh"

# Download verification report
echo "[4/5] Downloading verification report..."
scp "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/docs/logs/*.log" "${LOCAL_DIR}/docs/logs/" 2>/dev/null || true
scp "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_DIR}/docs/benchmark_results.csv" "${LOCAL_DIR}/docs/" 2>/dev/null || true

echo ""
echo "================================================"
echo "STAGE I COMPLETE!"
echo "================================================"
echo "Next steps:"
echo "  1. Check verification report in docs/logs/"
echo "  2. Review benchmark_results.csv"
echo "  3. Run Stage II ML pipeline: notebooks/ml_pipeline.ipynb"
echo "================================================"