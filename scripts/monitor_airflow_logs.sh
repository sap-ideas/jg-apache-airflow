#!/bin/bash
# ==============================================================================
# Airflow Log & Storage Monitoring Script
# ==============================================================================
# Purpose: Monitor Airflow log storage and alert if thresholds exceeded
# Usage: ./monitor_airflow_logs.sh [--alert-threshold-gb 10]
# ==============================================================================

set -e

# Configuration
ALERT_THRESHOLD_GB=${1:-10}  # Default: 10 GB
LOG_DIR="/root/repository/logs"
ALERT_EMAIL=""  # Set email if needed
SLACK_WEBHOOK=""  # Set Slack webhook if needed

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==============================================================================
# Functions
# ==============================================================================

print_header() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║       📊 AIRFLOW LOG STORAGE MONITORING REPORT            ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_disk_usage() {
    echo -e "${GREEN}💾 Current Storage Analysis:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Total size
    TOTAL_SIZE=$(du -sh "$LOG_DIR" 2>/dev/null | cut -f1)
    echo "  Total Log Storage: $TOTAL_SIZE"
    
    # Size in bytes for threshold check
    SIZE_BYTES=$(du -sb "$LOG_DIR" 2>/dev/null | cut -f1)
    SIZE_GB=$(echo "scale=2; $SIZE_BYTES / 1024 / 1024 / 1024" | bc)
    echo "  Size (GB): $SIZE_GB GB"
    
    # System disk space
    echo ""
    echo "  System Disk Space:"
    df -h "$LOG_DIR" | tail -1 | awk '{printf "    Used: %s / %s (%s)\n", $3, $2, $5}'
    
    echo ""
    
    # Check threshold
    THRESHOLD_BYTES=$(echo "$ALERT_THRESHOLD_GB * 1024 * 1024 * 1024" | bc | cut -d. -f1)
    if [ "$SIZE_BYTES" -gt "$THRESHOLD_BYTES" ]; then
        echo -e "${RED}⚠️  WARNING: Storage exceeds threshold!${NC}"
        echo -e "${RED}   Threshold: $ALERT_THRESHOLD_GB GB${NC}"
        echo -e "${RED}   Current: $SIZE_GB GB${NC}"
        return 1
    else
        echo -e "${GREEN}✅ Storage within threshold ($ALERT_THRESHOLD_GB GB)${NC}"
        return 0
    fi
}

analyze_by_dag() {
    echo ""
    echo -e "${GREEN}📂 Storage by DAG:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ -d "$LOG_DIR/dag_id="* ] 2>/dev/null; then
        du -sh "$LOG_DIR"/dag_id=* 2>/dev/null | sort -rh | head -10 | \
        while read size path; do
            dag_name=$(basename "$path" | sed 's/dag_id=//')
            printf "  %-8s %s\n" "$size" "$dag_name"
        done
    else
        echo "  No DAG logs found"
    fi
}

count_log_files() {
    echo ""
    echo -e "${GREEN}📁 Log File Statistics:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Total count
    TOTAL_LOGS=$(find "$LOG_DIR" -type f -name "*.log" 2>/dev/null | wc -l)
    echo "  Total Log Files: $TOTAL_LOGS"
    
    # Count by age
    LOGS_7_DAYS=$(find "$LOG_DIR" -type f -name "*.log" -mtime -7 2>/dev/null | wc -l)
    LOGS_30_DAYS=$(find "$LOG_DIR" -type f -name "*.log" -mtime -30 2>/dev/null | wc -l)
    LOGS_OLD=$(find "$LOG_DIR" -type f -name "*.log" -mtime +30 2>/dev/null | wc -l)
    
    echo "  Last 7 days: $LOGS_7_DAYS files"
    echo "  Last 30 days: $LOGS_30_DAYS files"
    echo "  Older than 30 days: $LOGS_OLD files"
    
    if [ "$LOGS_OLD" -gt 0 ]; then
        echo -e "${YELLOW}  ⚠️  Consider cleanup for old logs${NC}"
    fi
}

show_oldest_logs() {
    echo ""
    echo -e "${GREEN}📅 Oldest Log Files:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    find "$LOG_DIR" -type f -name "*.log" -printf '%T+ %p\n' 2>/dev/null | \
    sort | head -5 | \
    while read datetime path; do
        date=$(echo "$datetime" | cut -d+ -f1)
        relative_path=$(echo "$path" | sed "s|$LOG_DIR/||")
        echo "  $date - $relative_path"
    done
}

show_largest_logs() {
    echo ""
    echo -e "${GREEN}📦 Largest Log Files:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    find "$LOG_DIR" -type f -name "*.log" -exec du -h {} + 2>/dev/null | \
    sort -rh | head -5 | \
    while read size path; do
        relative_path=$(echo "$path" | sed "s|$LOG_DIR/||")
        printf "  %-8s %s\n" "$size" "$relative_path"
    done
}

show_recommendations() {
    echo ""
    echo -e "${BLUE}💡 Recommendations:${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    SIZE_BYTES=$(du -sb "$LOG_DIR" 2>/dev/null | cut -f1)
    SIZE_GB=$(echo "scale=2; $SIZE_BYTES / 1024 / 1024 / 1024" | bc)
    LOGS_OLD=$(find "$LOG_DIR" -type f -name "*.log" -mtime +30 2>/dev/null | wc -l)
    
    if (( $(echo "$SIZE_GB > 5" | bc -l) )); then
        echo "  📌 Storage usage high (${SIZE_GB} GB)"
        echo "     Consider running manual cleanup"
    fi
    
    if [ "$LOGS_OLD" -gt 100 ]; then
        echo "  📌 Many old log files ($LOGS_OLD files > 30 days)"
        echo "     Run: find /root/repository/logs -name '*.log' -mtime +30 -delete"
    fi
    
    echo "  📌 Automatic cleanup enabled via:"
    echo "     - docker-compose.yaml: LOG_RETENTION_DAYS=30"
    echo "     - Maintenance DAG: Runs weekly on Sunday"
    
    echo ""
    echo "  📌 Manual cleanup commands:"
    echo "     cd /root/repository"
    echo "     find logs -name '*.log' -mtime +30 -delete  # Delete > 30 days"
    echo "     docker-compose exec airflow-scheduler airflow db clean \\"
    echo "       --clean-before-timestamp \$(date -d '60 days ago' +%Y-%m-%d) -y"
}

print_footer() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${GREEN}Report Generated: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# ==============================================================================
# Main Execution
# ==============================================================================

print_header

# Run checks
ALERT_STATUS=0
check_disk_usage || ALERT_STATUS=1
analyze_by_dag
count_log_files
show_oldest_logs
show_largest_logs
show_recommendations
print_footer

# Exit with status
if [ $ALERT_STATUS -eq 1 ]; then
    echo ""
    echo -e "${RED}⚠️  ALERT: Action required!${NC}"
    exit 1
else
    echo ""
    echo -e "${GREEN}✅ All checks passed${NC}"
    exit 0
fi

