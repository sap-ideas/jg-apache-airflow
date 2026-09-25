"""
DAG: ETL Sejutajiwa Riders - KSJ Link Mapping
Extracts missing riders from KSJ Link PostgreSQL, transforms them,
and loads into MySQL Data Lake + Data Warehouse.
Runs two ownership segments (KBN and MITRA) through the same pipeline,
identified by transactions.ownership_status, into their own tables.
PostgreSQL sync is handled by a separate DAG (sync_sejutajiwa_riders_hubs_to_postgresql).
"""
from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import logging
import sys
import os
import importlib.util

# Add current directory to Python path for imports — evicting any stale same-named
# module first (see plugins/common/pipeline_loader.py for why this matters).
from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.dirname(os.path.abspath(__file__)))

# Import extraction functions
from extract import (
    get_missing_ksj_link_riders,
    get_rider_list,
    get_hub_id,
    get_mitra_hub_outlets
)

# Import transformation
from transform import transform_ksj_link

# Import load functions (reuse existing MySQL and PostgreSQL loaders)
from load import append_only_ignore_duplicates
from retry_helpers import retry_with_backoff

from common.db_helpers import get_engine
from common.teams_powerautomate_report import resolve_power_automate_webhook_url
from teams_report_riders_mapping import (
    fetch_riders_mapping_summary,
    send_riders_mapping_email_report,
    send_riders_mapping_teams_report,
)


# ==================== CONFIGURATION ====================
def load_config(config_path):
    """Load configuration from Python file"""
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Config


# Load configurations
CONFIG_DWH = load_config("/opt/airflow/config/config_db_datawarehouse.py")
CONFIG_KSJ = load_config("/opt/airflow/config/config_db_ksj_link.py")
CONFIG_JIWAPLUS = load_config("/opt/airflow/config/config_db_jiwaplus.py")


# ==================== DATABASE URIS ====================
# MySQL URIs (Data Lake & Data Warehouse)
MYSQL_URI_DATALAKE = (
    f"mysql+pymysql://{CONFIG_DWH.username}:{CONFIG_DWH.password}"
    f"@{CONFIG_DWH.hostname}:3306/{CONFIG_DWH.db_resource}"
)

MYSQL_URI_WAREHOUSE = (
    f"mysql+pymysql://{CONFIG_DWH.username}:{CONFIG_DWH.password}"
    f"@{CONFIG_DWH.hostname}:3306/{CONFIG_DWH.db_target}"
)

# ==================== TABLE NAMES ====================
MYSQL_TABLE = "sejutajiwa_riders_hubs_mapping"  # PRODUCTION! (ownership_status = KBN)
MYSQL_TABLE_MITRA = "sejutajiwa_riders_hubs_mapping_mitra"  # ownership_status = MITRA
PG_MAPPING_TABLE = "d_sejutajiwa_riders_hubs_mapping"
PG_MAPPING_TABLE_MITRA = "d_sejutajiwa_riders_hubs_mapping_mitra"
UPDATED_BY = "NEXUS_AIRFLOW"

# Dataset: triggers PostgreSQL sync DAG when this ETL completes
DATASET_MYSQL_UPDATED = Dataset("sejutajiwa_riders_hubs_mapping://mysql_updated")

# ==================== ETL PIPELINE ====================
def run_pipeline(*, ownership_status, mysql_table, pg_mapping_table, label, get_hub_df):
    """ETL pipeline for one ownership segment (KBN or MITRA).

    get_hub_df: no-arg callable returning the hub_id/jilid/hub_name DataFrame —
    KBN reads it from the Data Lake mapping table, MITRA reads it from JiwaPlus
    outlets. Both shapes are identical so the rest of the pipeline (transform,
    load) doesn't need to know which segment it's running.
    """

    logging.info(f"🚀 ETL KSJ Link Riders–Hubs Mapping started ({label}, PRODUCTION)")
    logging.info(f"   Targets: MySQL Lake/Warehouse → {mysql_table}")

    mysql_lake_inserted = 0
    mysql_warehouse_inserted = 0

    # ========== EXTRACT ==========
    logging.info("📥 EXTRACT")

    df_missing = retry_with_backoff(
        lambda: get_missing_ksj_link_riders(
            CONFIG_KSJ, CONFIG_DWH,
            ownership_status=ownership_status,
            mapping_table=pg_mapping_table,
        ),
        description=f"PostgreSQL extract missing {label} riders"
    )
    logging.info(f"🆕 Missing {label} riders detected: {len(df_missing)}")

    if df_missing.empty:
        logging.info(f"ℹ️ No missing {label} riders found. Skipping MySQL insert.")
    else:
        # Display missing riders list
        missing_riders_list = df_missing['rider_code'].tolist()
        logging.info(f"📋 Missing {label} Riders List:")
        for rider_code in missing_riders_list:
            logging.info(f"   - {rider_code}")

        # Extract full rider list from KSJ Link
        df_rider_list = retry_with_backoff(
            lambda: get_rider_list(CONFIG_KSJ),
            description="PostgreSQL extract KSJ rider list"
        )
        logging.info(f"📋 KSJ Link rider list rows: {len(df_rider_list)}")

        # Extract hub mapping (KBN: MySQL Data Lake, MITRA: JiwaPlus outlets)
        df_hub = retry_with_backoff(
            get_hub_df,
            description=f"Extract {label} hub master"
        )
        logging.info(f"🏢 {label} hub master rows: {len(df_hub)}")

        # ========== TRANSFORM ==========
        logging.info("⚙️ TRANSFORM")
        df_final = transform_ksj_link(
            df_rider_list=df_rider_list,
            df_missing=df_missing,
            df_hub=df_hub,
            updated_by=UPDATED_BY
        )

        if df_final.empty:
            logging.info(f"⚠️ No {label} data to insert after transformation")
        else:
            logging.info(f"✅ Final {label} rows: {len(df_final)}")

            # ========== LOAD TO MYSQL ==========
            logging.info("📤 LOAD")

            logging.info(f"📤 Loading {label} to MySQL Data Lake...")
            mysql_lake_inserted = append_only_ignore_duplicates(
                df_final,
                MYSQL_URI_DATALAKE,
                mysql_table
            )
            logging.info(f"✅ MySQL Data Lake ({label}): {mysql_lake_inserted} rows inserted")

            logging.info(f"📤 Loading {label} to MySQL Data Warehouse...")
            mysql_warehouse_inserted = append_only_ignore_duplicates(
                df_final,
                MYSQL_URI_WAREHOUSE,
                mysql_table
            )
            logging.info(f"✅ MySQL Data Warehouse ({label}): {mysql_warehouse_inserted} rows inserted")

    # ========== SUMMARY ==========
    logging.info(
        f"✅ ETL Complete ({label}) | Lake: {mysql_lake_inserted} | "
        f"Warehouse: {mysql_warehouse_inserted}"
    )


def run_kbn():
    run_pipeline(
        ownership_status="KBN",
        mysql_table=MYSQL_TABLE,
        pg_mapping_table=PG_MAPPING_TABLE,
        label="KBN",
        get_hub_df=lambda: get_hub_id(CONFIG_DWH),
    )


def run_mitra():
    run_pipeline(
        ownership_status="MITRA",
        mysql_table=MYSQL_TABLE_MITRA,
        pg_mapping_table=PG_MAPPING_TABLE_MITRA,
        label="MITRA",
        get_hub_df=lambda: get_mitra_hub_outlets(CONFIG_JIWAPLUS),
    )


def send_teams_and_email_notification(**context):
    """Post the same summary to Teams (Adaptive Card) and email — one DB query, identical body text."""
    webhook = resolve_power_automate_webhook_url()
    if not webhook:
        logging.warning("Teams: POWER_AUTOMATE_TEAMS_WEBHOOK_URL is unset; skipping Teams notification.")
        webhook = None

    now_jakarta = pendulum.now("Asia/Jakarta")
    report_date = now_jakarta.format("YYYY-MM-DD")
    # Explicit WIB wall clock — avoids clients treating ISO8601 as UTC-only display
    executed_at = now_jakarta.format("YYYY-MM-DD HH:mm:ss") + " WIB"
    dag_run = context["dag_run"]

    engine = get_engine(CONFIG_DWH)
    summary_rows = fetch_riders_mapping_summary(engine)

    if webhook:
        send_riders_mapping_teams_report(
            engine=engine,
            webhook_url=webhook,
            report_title="Riders Mapping Report",
            report_date=report_date,
            dag_id=context["dag"].dag_id,
            run_id=dag_run.run_id,
            executed_at_iso=executed_at,
            summary_rows=summary_rows,
        )

    send_riders_mapping_email_report(
        rows=summary_rows,
        report_date=report_date,
        dag_id=context["dag"].dag_id,
        run_id=dag_run.run_id,
        executed_at_display=executed_at,
    )


# ==================== DAG DEFINITION ====================
local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="etl_sejutajiwa_riders_ksj_link_mapping",
    default_args=default_args,
    description="ETL: Sejutajiwa Riders - KSJ Link Mapping (MySQL Lake + Warehouse)",
    schedule_interval="0 8 * * *",  # Daily at 8 AM Jakarta time
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["dimension", "ksj-link", "master-data", "production"],
    max_active_runs=1
) as dag:
    
    run_pipeline_kbn = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_kbn,
        outlets=[DATASET_MYSQL_UPDATED]
    )

    run_pipeline_mitra = PythonOperator(
        task_id="run_pipeline_mitra",
        python_callable=run_mitra,
        outlets=[DATASET_MYSQL_UPDATED]
    )

    send_reports_task = PythonOperator(
        task_id="send_teams_and_email_notification",
        python_callable=send_teams_and_email_notification,
        trigger_rule="all_success",
    )

    [run_pipeline_kbn, run_pipeline_mitra] >> send_reports_task

