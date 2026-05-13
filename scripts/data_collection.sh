#!/bin/bash
# ============================================================
# data_collection.sh — Download Chicago Crimes Dataset
# Source: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_DIR/data"
LOG_FILE="$DATA_DIR/download.log"

mkdir -p "$DATA_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting data collection..." | tee "$LOG_FILE"

# --- Method 1: Direct bulk CSV download via Socrata API ---
# This is the primary method — downloads the full dataset in one shot.
RAW_FILE="$DATA_DIR/chicago_crimes_raw.csv"
BULK_URL="https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD"

if [ -f "$RAW_FILE" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Raw file already exists at $RAW_FILE" | tee -a "$LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Size: $(du -h "$RAW_FILE" | cut -f1)" | tee -a "$LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Skipping download. Delete the file to re-download." | tee -a "$LOG_FILE"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading Chicago Crimes dataset..." | tee -a "$LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] URL: $BULK_URL" | tee -a "$LOG_FILE"

    wget --no-check-certificate -q --show-progress -O "$RAW_FILE" "$BULK_URL" 2>&1 | tee -a "$LOG_FILE"

    if [ -f "$RAW_FILE" ]; then
        FILE_SIZE=$(du -h "$RAW_FILE" | cut -f1)
        LINE_COUNT=$(wc -l < "$RAW_FILE")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Download complete!" | tee -a "$LOG_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] File: $RAW_FILE" | tee -a "$LOG_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Size: $FILE_SIZE" | tee -a "$LOG_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Lines: $LINE_COUNT" | tee -a "$LOG_FILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Download failed!" | tee -a "$LOG_FILE"
        exit 1
    fi
fi

# --- Method 2: Fallback — Paginated API (for limited bandwidth) ---
# Uncomment below if Method 1 fails or times out.
: <<'PAGINATED_FALLBACK'
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Using paginated API fallback..." | tee -a "$LOG_FILE"
SODA_URL="https://data.cityofchicago.org/resource/ijzp-q8t2.csv"
BATCH_SIZE=500000
TOTAL_ROWS=8523527
NUM_BATCHES=$(( (TOTAL_ROWS + BATCH_SIZE - 1) / BATCH_SIZE ))

PARTS_DIR="$DATA_DIR/parts"
mkdir -p "$PARTS_DIR"

for i in $(seq 0 $((NUM_BATCHES - 1))); do
    OFFSET=$((i * BATCH_SIZE))
    PART_FILE="$PARTS_DIR/part_${i}.csv"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading batch $((i+1))/$NUM_BATCHES (offset=$OFFSET)..." | tee -a "$LOG_FILE"
    curl -s "$SODA_URL?\$limit=$BATCH_SIZE&\$offset=$OFFSET" > "$PART_FILE"
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Merging parts..." | tee -a "$LOG_FILE"
# Get header from first file, skip headers from rest
head -1 "$PARTS_DIR/part_0.csv" > "$RAW_FILE"
tail -n +2 -q "$PARTS_DIR"/part_*.csv >> "$RAW_FILE"
rm -rf "$PARTS_DIR"

LINE_COUNT=$(wc -l < "$RAW_FILE")
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Merged file: $LINE_COUNT lines" | tee -a "$LOG_FILE"
PAGINATED_FALLBACK

# --- Compute checksum for integrity ---
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Computing checksum..." | tee -a "$LOG_FILE"
md5sum "$RAW_FILE" > "$RAW_FILE.md5"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Checksum: $(cat $RAW_FILE.md5)" | tee -a "$LOG_FILE"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Data collection complete!" | tee -a "$LOG_FILE"