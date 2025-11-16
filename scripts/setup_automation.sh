#!/bin/bash
################################################################################
# NYC Taxi Data Pipeline - Systemd Automation Setup Script
#
# This script installs and configures systemd services and timers for:
# 1. Daily data pipeline (runs at 2:00 AM daily)
# 2. Weekly report (runs at 6:00 PM every Sunday)
#
# Usage: sudo ./setup_automation.sh
# Requirements: systemd (Linux only)
################################################################################

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    log_error "This script must be run as root (use sudo)"
    exit 1
fi

# Check if systemd is available
if ! command -v systemctl &> /dev/null; then
    log_error "systemd is not available on this system"
    log_error "This script only works on systems with systemd (most modern Linux distributions)"
    exit 1
fi

# Get the actual user (not root when using sudo)
ACTUAL_USER="${SUDO_USER:-$USER}"
ACTUAL_GROUP=$(id -gn "$ACTUAL_USER")

# Determine project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

log_info "========================================="
log_info "NYC Taxi Pipeline Automation Setup"
log_info "========================================="
log_info "Project Directory: $PROJECT_DIR"
log_info "User: $ACTUAL_USER"
log_info "Group: $ACTUAL_GROUP"
log_info ""

# Create logs directory
log_info "Creating logs directory..."
mkdir -p "$PROJECT_DIR/logs"
chown "$ACTUAL_USER:$ACTUAL_GROUP" "$PROJECT_DIR/logs"
log_success "Logs directory created"

# Make shell scripts executable
log_info "Making shell scripts executable..."
chmod +x "$PROJECT_DIR/scripts/daily_data_pipeline.sh"
chmod +x "$PROJECT_DIR/scripts/weekly_report.sh"
log_success "Shell scripts are now executable"

# Copy and configure systemd service files
SYSTEMD_DIR="/etc/systemd/system"

log_info "Installing systemd service files..."

# Process each service file
for service_file in "$PROJECT_DIR"/systemd/*.service; do
    service_name=$(basename "$service_file")

    log_info "  Installing $service_name..."

    # Replace placeholders
    sed -e "s|%USER%|$ACTUAL_USER|g" \
        -e "s|%GROUP%|$ACTUAL_GROUP|g" \
        -e "s|%PROJECT_DIR%|$PROJECT_DIR|g" \
        "$service_file" > "$SYSTEMD_DIR/$service_name"

    chmod 644 "$SYSTEMD_DIR/$service_name"
    log_success "  ✓ $service_name installed"
done

# Copy timer files
log_info "Installing systemd timer files..."

for timer_file in "$PROJECT_DIR"/systemd/*.timer; do
    timer_name=$(basename "$timer_file")

    log_info "  Installing $timer_name..."
    cp "$timer_file" "$SYSTEMD_DIR/$timer_name"
    chmod 644 "$SYSTEMD_DIR/$timer_name"
    log_success "  ✓ $timer_name installed"
done

# Reload systemd daemon
log_info "Reloading systemd daemon..."
systemctl daemon-reload
log_success "Systemd daemon reloaded"

# Enable and start timers
log_info "Enabling and starting systemd timers..."

for timer_file in "$PROJECT_DIR"/systemd/*.timer; do
    timer_name=$(basename "$timer_file")

    log_info "  Enabling $timer_name..."
    systemctl enable "$timer_name"
    systemctl start "$timer_name"
    log_success "  ✓ $timer_name enabled and started"
done

log_info ""
log_info "========================================="
log_success "Installation Complete!"
log_info "========================================="
log_info ""

# Show timer status
log_info "Timer Status:"
log_info "─────────────────────────────────────────"
systemctl list-timers --all | grep nyc-taxi || true
log_info ""

# Show service status
log_info "Service Status:"
log_info "─────────────────────────────────────────"
systemctl status nyc-taxi-daily-pipeline.service --no-pager || true
echo ""
systemctl status nyc-taxi-weekly-report.service --no-pager || true
log_info ""

# Instructions
log_info "========================================="
log_info "Useful Commands:"
log_info "========================================="
echo ""
echo "  View timer status:"
echo "    sudo systemctl list-timers | grep nyc-taxi"
echo ""
echo "  Check service logs:"
echo "    sudo journalctl -u nyc-taxi-daily-pipeline.service -f"
echo "    sudo journalctl -u nyc-taxi-weekly-report.service -f"
echo ""
echo "  Run services manually:"
echo "    sudo systemctl start nyc-taxi-daily-pipeline.service"
echo "    sudo systemctl start nyc-taxi-weekly-report.service"
echo ""
echo "  Disable automation:"
echo "    sudo systemctl stop nyc-taxi-daily-pipeline.timer"
echo "    sudo systemctl stop nyc-taxi-weekly-report.timer"
echo "    sudo systemctl disable nyc-taxi-daily-pipeline.timer"
echo "    sudo systemctl disable nyc-taxi-weekly-report.timer"
echo ""
echo "  Re-enable automation:"
echo "    sudo systemctl enable --now nyc-taxi-daily-pipeline.timer"
echo "    sudo systemctl enable --now nyc-taxi-weekly-report.timer"
echo ""

log_success "Automation is now active!"
log_info "Daily pipeline will run at 2:00 AM every day"
log_info "Weekly report will run at 6:00 PM every Sunday"

exit 0
