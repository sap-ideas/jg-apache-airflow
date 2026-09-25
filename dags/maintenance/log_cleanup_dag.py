"""
Airflow Maintenance DAG - Log and Database Cleanup
===================================================

Purpose:
    Automated maintenance tasks for Airflow to keep system healthy:
    - Clean up old log files (> 30 days)
    - Clean up old DAG runs from database (> 60 days)
    - Monitor disk usage
    - Generate cleanup reports

Schedule:
    Weekly on Sunday at 02:00 AM (Asia/Jakarta)

Owner: data-team
Tags: maintenance, cleanup, monitoring
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import subprocess
import logging


# ============================================================
# Configuration
# ============================================================
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_tz = pendulum.timezone("Asia/Jakarta")

# Retention settings (should match docker-compose.yaml)
LOG_RETENTION_DAYS = 7  # Delete logs older than 7 days
DAG_RUN_RETENTION_DAYS = 30  # Delete DAG runs older than 30 days


# ============================================================
# Helper Functions
# ============================================================
def generate_cleanup_report(**context):
    """
    Generate a summary report of cleanup activities
    """
    logging.info("="*60)
    logging.info("📊 AIRFLOW MAINTENANCE SUMMARY REPORT")
    logging.info("="*60)
    
    # Get disk usage
    try:
        result = subprocess.run(
            ['du', '-sh', '/opt/airflow/logs'],
            capture_output=True,
            text=True
        )
        logs_size = result.stdout.split()[0]
        logging.info(f"💾 Current Log Storage: {logs_size}")
    except Exception as e:
        logging.warning(f"Could not get disk usage: {e}")
    
    # Count log files
    try:
        result = subprocess.run(
            ['find', '/opt/airflow/logs', '-type', 'f', '-name', '*.log'],
            capture_output=True,
            text=True
        )
        log_count = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
        logging.info(f"📁 Total Log Files: {log_count}")
    except Exception as e:
        logging.warning(f"Could not count log files: {e}")
    
    # Get oldest log file
    try:
        result = subprocess.run(
            ['find', '/opt/airflow/logs', '-type', 'f', '-name', '*.log', '-printf', '%T+ %p\\n'],
            capture_output=True,
            text=True
        )
        if result.stdout.strip():
            oldest = result.stdout.strip().split('\n')[0]
            logging.info(f"📅 Oldest Log: {oldest[:10]}")
    except Exception as e:
        logging.warning(f"Could not get oldest log: {e}")
    
    logging.info("="*60)
    logging.info("✅ Maintenance report generated")
    logging.info("="*60)


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id='maintenance_log_cleanup',
    default_args=default_args,
    description='Weekly automated cleanup of Airflow logs and database records',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['maintenance', 'cleanup', 'monitoring', 'system'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Task 1: Pre-cleanup disk check
    pre_cleanup_check = BashOperator(
        task_id='pre_cleanup_disk_check',
        bash_command=f"""
        echo "╔════════════════════════════════════════════════════════════╗"
        echo "║          🔍 PRE-CLEANUP DISK ANALYSIS                     ║"
        echo "╚════════════════════════════════════════════════════════════╝"
        echo ""
        echo "📊 Current Disk Usage:"
        du -sh /opt/airflow/logs
        echo ""
        echo "📁 Log Files by DAG:"
        du -sh /opt/airflow/logs/dag_id=* 2>/dev/null || echo "No DAG logs yet"
        echo ""
        echo "📈 Total Log Files:"
        find /opt/airflow/logs -type f -name "*.log" | wc -l
        echo ""
        echo "⏰ Oldest Log File:"
        find /opt/airflow/logs -type f -name "*.log" -printf '%T+ %p\\n' 2>/dev/null | sort | head -1 || echo "No logs found"
        echo ""
        """,
        doc_md="Check current disk usage before cleanup to compare later"
    )

    # Task 2: Clean up old log files
    cleanup_old_logs = BashOperator(
        task_id='cleanup_old_log_files',
        bash_command=f"""
        echo "🧹 Cleaning log files older than {LOG_RETENTION_DAYS} days..."
        echo ""
        
        # Count files before deletion
        BEFORE_COUNT=$(find /opt/airflow/logs -type f -name "*.log" | wc -l)
        echo "📁 Log files before: $BEFORE_COUNT"
        
        # Find and delete old logs
        DELETED=$(find /opt/airflow/logs -type f -name "*.log" -mtime +{LOG_RETENTION_DAYS} -delete -print | wc -l)
        
        # Count files after deletion
        AFTER_COUNT=$(find /opt/airflow/logs -type f -name "*.log" | wc -l)
        echo "📁 Log files after: $AFTER_COUNT"
        echo "🗑️  Files deleted: $DELETED"
        echo ""
        
        if [ $DELETED -gt 0 ]; then
            echo "✅ Successfully cleaned $DELETED old log files"
        else
            echo "ℹ️  No old log files to clean"
        fi
        """,
        doc_md=f"Delete log files older than {LOG_RETENTION_DAYS} days to free up disk space"
    )

    # Task 3: Clean up empty directories
    cleanup_empty_dirs = BashOperator(
        task_id='cleanup_empty_directories',
        bash_command="""
        echo "🧹 Cleaning empty directories..."
        
        # Find and remove empty directories
        REMOVED=$(find /opt/airflow/logs -type d -empty -delete -print | wc -l)
        
        if [ $REMOVED -gt 0 ]; then
            echo "✅ Removed $REMOVED empty directories"
        else
            echo "ℹ️  No empty directories found"
        fi
        """,
        doc_md="Remove empty directories to keep log structure clean"
    )

    # Task 4: Clean up old DAG runs from database
    cleanup_old_dag_runs = BashOperator(
        task_id='cleanup_old_dag_runs_database',
        bash_command=f"""
        echo "🧹 Cleaning DAG runs older than {DAG_RUN_RETENTION_DAYS} days from database..."
        echo ""
        
        # Calculate date
        CUTOFF_DATE=$(date -d '{DAG_RUN_RETENTION_DAYS} days ago' +%Y-%m-%d)
        echo "📅 Cutoff date: $CUTOFF_DATE"
        echo ""
        
        # Run Airflow db cleanup
        airflow db clean \\
            --clean-before-timestamp "$CUTOFF_DATE" \\
            --skip-archive \\
            --yes \\
            --verbose 2>&1 | tail -20
        
        echo ""
        echo "✅ Database cleanup completed"
        """,
        doc_md=f"Clean up DAG runs older than {DAG_RUN_RETENTION_DAYS} days from Airflow metadata database"
    )

    # Task 5: Optimize database
    optimize_database = BashOperator(
        task_id='optimize_airflow_database',
        bash_command="""
        echo "🔧 Optimizing Airflow metadata database..."
        
        # Vacuum and analyze PostgreSQL
        psql $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN -c "VACUUM ANALYZE;" 2>&1 || echo "⚠️  Could not optimize (may need manual intervention)"
        
        echo "✅ Database optimization attempted"
        """,
        doc_md="Optimize PostgreSQL database to reclaim space and improve performance"
    )

    # Task 6: Post-cleanup disk check
    post_cleanup_check = BashOperator(
        task_id='post_cleanup_disk_check',
        bash_command="""
        echo "╔════════════════════════════════════════════════════════════╗"
        echo "║          📊 POST-CLEANUP DISK ANALYSIS                    ║"
        echo "╚════════════════════════════════════════════════════════════╝"
        echo ""
        echo "💾 Current Disk Usage:"
        du -sh /opt/airflow/logs
        echo ""
        echo "📁 Log Files by DAG:"
        du -sh /opt/airflow/logs/dag_id=* 2>/dev/null || echo "No DAG logs"
        echo ""
        echo "📈 Total Log Files:"
        find /opt/airflow/logs -type f -name "*.log" | wc -l
        echo ""
        echo "💽 System Disk Space:"
        df -h /opt/airflow/logs | tail -1
        echo ""
        """,
        doc_md="Check disk usage after cleanup to measure impact"
    )

    # Task 7: Generate summary report
    generate_report = PythonOperator(
        task_id='generate_cleanup_report',
        python_callable=generate_cleanup_report,
        doc_md="Generate comprehensive cleanup summary report"
    )

    # Task dependencies
    pre_cleanup_check >> cleanup_old_logs >> cleanup_empty_dirs
    cleanup_empty_dirs >> cleanup_old_dag_runs >> optimize_database
    optimize_database >> post_cleanup_check >> generate_report

