#!/bin/bash
# ============================================================
# data_storage.sh — Build PostgreSQL database for Stage I
# Runs: python3 scripts/build_projectdb.py
# ============================================================
set -e

echo "============================================="
echo "Building PostgreSQL database..."
echo "============================================="

python3 scripts/build_projectdb.py

echo ""
echo "Database build complete."
echo "============================================="
