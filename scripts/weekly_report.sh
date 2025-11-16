#!/bin/bash
################################################################################
# Weekly Report Automation Script
#
# This script sends the weekly report to Discord and Email every Sunday.
# It automatically calculates the week name based on the current date.
#
# Usage: ./weekly_report.sh [--week week_name] [--dry-run]
# Scheduled: Every Sunday at 18:00 (6 PM) via systemd timer
################################################################################

set -euo pipefail  # Exit on error, undefined variables, and pipe failures

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
VENV_DIR="$PROJECT_DIR/.venv"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/weekly_report_${TIMESTAMP}.log"

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
log "Starting Weekly Report"
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

# Parse command line arguments
DRY_RUN=""
WEEK_NAME=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --week)
            WEEK_NAME="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Usage: $0 [--week week_name] [--dry-run]"
            exit 1
            ;;
    esac
done

# Calculate week name if not provided
if [ -z "$WEEK_NAME" ]; then
    WEEK_NUM=$((($(date +%-d) - 1) / 7 + 1))
    MONTH_NAME=$(date +%B | tr '[:upper:]' '[:lower:]')
    YEAR=$(date +%Y)
    WEEK_NAME="week_${WEEK_NUM}_${MONTH_NAME}_${YEAR}"
    log "Auto-calculated week name: $WEEK_NAME"
else
    log "Using provided week name: $WEEK_NAME"
fi

# Check if week data exists
WEEK_DIR="$PROJECT_DIR/output/$WEEK_NAME"
if [ ! -d "$WEEK_DIR" ]; then
    log_error "Week directory not found: $WEEK_DIR"
    log_error "Please ensure the data pipeline has been run for week: $WEEK_NAME"
    exit 1
fi

log "Week directory found: $WEEK_DIR"

# Send weekly report
log "----------------------------------------"
log "Sending weekly report for: $WEEK_NAME"
if [ -n "$DRY_RUN" ]; then
    log "Mode: DRY RUN (preview only)"
fi
log "----------------------------------------"

if python send_weekly_report.py --week "$WEEK_NAME" $DRY_RUN 2>&1 | tee -a "$LOG_FILE"; then
    log "✓ Weekly report sent successfully"
else
    log_error "✗ Failed to send weekly report"
    exit 1
fi

# Cleanup old logs (keep last 60 days)
log "----------------------------------------"
log "Cleaning up old log files..."
log "----------------------------------------"
find "$LOG_DIR" -name "weekly_report_*.log" -type f -mtime +60 -delete 2>/dev/null || true
log "Old logs cleaned up"

log "========================================="
log "Weekly Report Completed Successfully"
log "Log file: $LOG_FILE"
log "========================================="

exit 0
