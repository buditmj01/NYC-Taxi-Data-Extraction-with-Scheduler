#!/bin/bash
################################################################################
# Daily Data Pipeline Automation Script
#
# This script runs the daily data extraction and processing pipeline.
# It extracts NYC Taxi data for the current date and processes it.
#
# Usage: ./daily_data_pipeline.sh
# Scheduled: Daily at 02:00 AM via systemd timer
################################################################################

set -euo pipefail  # Exit on error, undefined variables, and pipe failures

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
VENV_DIR="$PROJECT_DIR/.venv"
DATE=$(date +%Y-%m-%d)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/daily_pipeline_${TIMESTAMP}.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

# Trap errors
trap 'log_error "Script failed at line $LINENO"' ERR

log "========================================="
log "Starting Daily Data Pipeline"
log "Date: $DATE"
log "========================================="

# Navigate to project directory
cd "$PROJECT_DIR"
log "Working directory: $(pwd)"

# Activate virtual environment if it exists
if [ -d "$VENV_DIR" ]; then
    log "Activating virtual environment..."
    source "$VENV_DIR/bin/activate"
else
    log "WARNING: Virtual environment not found at $VENV_DIR"
    log "Using system Python"
fi

# Check Python version
PYTHON_VERSION=$(python --version 2>&1)
log "Python version: $PYTHON_VERSION"

# Step 1: Data Extraction (Daily Mode)
log "----------------------------------------"
log "Step 1: Extracting data for $DATE"
log "----------------------------------------"

if python data_extraction.py --mode daily --date "$DATE" 2>&1 | tee -a "$LOG_FILE"; then
    log "✓ Data extraction completed successfully"
else
    log_error "✗ Data extraction failed"
    exit 1
fi

# Step 2: Data Pipeline (Weekly aggregation if it's Sunday)
DAY_OF_WEEK=$(date +%u)  # 1=Monday, 7=Sunday

if [ "$DAY_OF_WEEK" -eq 7 ]; then
    log "----------------------------------------"
    log "Step 2: Sunday detected - Running weekly aggregation"
    log "----------------------------------------"

    # Calculate week name (week_N_month_year)
    WEEK_NUM=$((($(date +%-d) - 1) / 7 + 1))
    MONTH_NAME=$(date +%B | tr '[:upper:]' '[:lower:]')
    YEAR=$(date +%Y)
    WEEK_NAME="week_${WEEK_NUM}_${MONTH_NAME}_${YEAR}"

    log "Week name: $WEEK_NAME"

    # Upload to PostgreSQL
    if python data_extraction.py --mode weekly 2>&1 | tee -a "$LOG_FILE"; then
        log "✓ Weekly data upload to PostgreSQL completed"

        # Run aggregation pipeline
        if python data_pipeline.py --table "$WEEK_NAME" 2>&1 | tee -a "$LOG_FILE"; then
            log "✓ Weekly aggregation pipeline completed"
        else
            log_error "✗ Weekly aggregation pipeline failed"
            exit 1
        fi
    else
        log_error "✗ Weekly data upload failed"
        exit 1
    fi
else
    log "Not Sunday (day $DAY_OF_WEEK) - Skipping weekly aggregation"
fi

# Cleanup old logs (keep last 30 days)
log "----------------------------------------"
log "Cleaning up old log files..."
log "----------------------------------------"
find "$LOG_DIR" -name "daily_pipeline_*.log" -type f -mtime +30 -delete 2>/dev/null || true
log "Old logs cleaned up"

log "========================================="
log "Daily Data Pipeline Completed Successfully"
log "Log file: $LOG_FILE"
log "========================================="

exit 0
