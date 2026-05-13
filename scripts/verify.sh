#!/bin/bash
# ============================================================
# verify.sh — Data Quality Verification
# Run after data collection + ingestion
# ============================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data"
LOG_DIR="$PROJECT_DIR/docs"
RAW_FILE="$DATA_DIR/chicago_crimes_raw.csv"
REPORT="$LOG_DIR/verification_report_$(date '+%Y%m%d_%H%M%S').txt"

mkdir -p "$LOG_DIR"

echo "=============================================" | tee "$REPORT"
echo "Data Verification Report" | tee -a "$REPORT"
echo "Timestamp: $(date)" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"

# --- 1. File-level checks ---
echo "" | tee -a "$REPORT"
echo "[1] File-Level Checks" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"

if [ -f "$RAW_FILE" ]; then
    FILE_SIZE=$(ls -lh "$RAW_FILE" | awk '{print $5}')
    LINE_COUNT=$(wc -l < "$RAW_FILE")
    echo "  ✓ File exists: $RAW_FILE" | tee -a "$REPORT"
    echo "  File size: $FILE_SIZE" | tee -a "$REPORT"
    echo "  Total lines (incl header): $LINE_COUNT" | tee -a "$REPORT"
    echo "  Data rows: $((LINE_COUNT - 1))" | tee -a "$REPORT"

    # Verify minimum expected rows (update threshold as needed)
    MIN_ROWS=8500000
    DATA_ROWS=$((LINE_COUNT - 1))
    if [ "$DATA_ROWS" -ge "$MIN_ROWS" ]; then
        echo "  ✓ Row count >= $MIN_ROWS (PASS)" | tee -a "$REPORT"
    else
        echo "  ✗ Row count < $MIN_ROWS (FAIL — expected ~8.5M rows)" | tee -a "$REPORT"
    fi
else
    echo "  ✗ File NOT found: $RAW_FILE" | tee -a "$REPORT"
    exit 1
fi

# --- 2. Header validation ---
echo "" | tee -a "$REPORT"
echo "[2] Header Validation" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"

HEADER=$(head -1 "$RAW_FILE")
EXPECTED_COLS=22
ACTUAL_COLS=$(echo "$HEADER" | awk -F',' '{print NF}')
echo "  Expected columns: $EXPECTED_COLS" | tee -a "$REPORT"
echo "  Actual columns:   $ACTUAL_COLS" | tee -a "$REPORT"

if [ "$ACTUAL_COLS" -ge "$EXPECTED_COLS" ]; then
    echo "  ✓ Column count OK (PASS)" | tee -a "$REPORT"
else
    echo "  ✗ Column count mismatch (FAIL)" | tee -a "$REPORT"
fi

echo "  Columns: $HEADER" | tee -a "$REPORT"

# --- 3. Sample data check ---
echo "" | tee -a "$REPORT"
echo "[3] Sample Data (first 3 data rows)" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"
sed -n '2,4p' "$RAW_FILE" | tee -a "$REPORT"

# --- 4. Check for empty/NULL values in key columns ---
echo "" | tee -a "$REPORT"
echo "[4] Null / Empty Value Analysis (key columns)" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"

# Column indices (1-based, adjust based on actual header):
# 1=id, 4=date, 9=arrest, 19=latitude, 20=longitude, 22=year

# Total rows (excluding header)
TOTAL=$(tail -n +2 "$RAW_FILE" | wc -l)
echo "  Total data rows: $TOTAL" | tee -a "$REPORT"

# Check arrest column (col 9) - must be TRUE/FALSE
ARREST_EMPTY=$(tail -n +2 "$RAW_FILE" | awk -F',' '$9 == ""' | wc -l)
echo "  Empty arrest values: $ARREST_EMPTY" | tee -a "$REPORT"

# Check latitude (col ~19-20) - depends on CSV structure
echo "  (Full null analysis available via PostgreSQL after import)" | tee -a "$REPORT"

# --- 5. Year range check ---
echo "" | tee -a "$REPORT"
echo "[5] Year Range Check" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"

MIN_YEAR=$(tail -n +2 "$RAW_FILE" | awk -F',' '{print $NF}' | sort -n | head -1)
MAX_YEAR=$(tail -n +2 "$RAW_FILE" | awk -F',' '{print $NF}' | sort -n | tail -1)
echo "  Min year: $MIN_YEAR" | tee -a "$REPORT"
echo "  Max year: $MAX_YEAR" | tee -a "$REPORT"

if [ "$MIN_YEAR" -ge 2001 ]; then
    echo "  ✓ Min year >= 2001 (PASS)" | tee -a "$REPORT"
else
    echo "  ✗ Min year < 2001 (FAIL)" | tee -a "$REPORT"
fi

if [ "$MAX_YEAR" -le 2026 ]; then
    echo "  ✓ Max year <= 2026 (PASS)" | tee -a "$REPORT"
else
    echo "  ✗ Max year > 2026 (FAIL)" | tee -a "$REPORT"
fi

# --- 6. Duplicate ID check ---
echo "" | tee -a "$REPORT"
echo "[6] Duplicate ID Check" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"

TOTAL_IDS=$(tail -n +2 "$RAW_FILE" | awk -F',' '{print $1}' | wc -l)
UNIQUE_IDS=$(tail -n +2 "$RAW_FILE" | awk -F',' '{print $1}' | sort -u | wc -l)
echo "  Total IDs:   $TOTAL_IDS" | tee -a "$REPORT"
echo "  Unique IDs:  $UNIQUE_IDS" | tee -a "$REPORT"

if [ "$TOTAL_IDS" -eq "$UNIQUE_IDS" ]; then
    echo "  ✓ No duplicate IDs (PASS)" | tee -a "$REPORT"
else
    DUPES=$((TOTAL_IDS - UNIQUE_IDS))
    echo "  ⚠ Found $DUPES duplicate IDs" | tee -a "$REPORT"
fi

# --- 7. Checksum verification ---
echo "" | tee -a "$REPORT"
echo "[7] MD5 Checksum" | tee -a "$REPORT"
echo "---" | tee -a "$REPORT"
MD5=$(md5sum "$RAW_FILE" | awk '{print $1}')
echo "  MD5: $MD5" | tee -a "$REPORT"

# --- Summary ---
echo "" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"
echo "Verification completed: $(date)" | tee -a "$REPORT"
echo "Full report: $REPORT" | tee -a "$REPORT"
echo "=============================================" | tee -a "$REPORT"

echo ""
echo "✅ Verification report saved to: $REPORT"