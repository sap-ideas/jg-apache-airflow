"""
DAG: Sync Sejutajiwa Riders Hubs Mapping -> PostgreSQL
Reads FULL tables from MySQL Data Lake (KBN + MITRA), replaces in PostgreSQL
(d_transaction + d_location + jiwa-ksj).

Trigger: Dataset-based. Automatically runs when KSJ Link ETL DAG completes.
Safe to run multiple times (idempotent) since it does TRUNCATE + INSERT.
"""
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import logging
import sys
import os
import importlib.util

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.dirname(os.path.abspath(__file__)))

from sync_postgresql import sync_mysql_to_postgresql


# ==================== DATASET (triggered by KSJ Link ETL DAG) ====================
DATASET_MYSQL_UPDATED = Dataset("sejutajiwa_riders_hubs_mapping://mysql_updated")


# ==================== CONFIGURATION ====================
def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DWH = load_config("/opt/airflow/config/config_db_datawarehouse.py")
CONFIG_KSJ = load_config("/opt/airflow/config/config_db_ksj_link.py")

MYSQL_URI_DATALAKE = (
    f"mysql+pymysql://{CONFIG_DWH.username_lake}:"
    f"{CONFIG_DWH.password_lake}@{CONFIG_DWH.hostname_lake}/"
    f"{CONFIG_DWH.db_lake}"
)

MYSQL_TABLE = "sejutajiwa_riders_hubs_mapping"
POSTGRES_TABLE = "d_sejutajiwa_riders_hubs_mapping"
MYSQL_TABLE_MITRA = "sejutajiwa_riders_hubs_mapping_mitra"
POSTGRES_TABLE_MITRA = "d_sejutajiwa_riders_hubs_mapping_mitra"
UPDATED_BY = "NEXUS_AIRFLOW"

# (mysql_table, postgres_table, label) — each synced to d_transaction + d_location + jiwa-ksj
PIPELINES = [
    (MYSQL_TABLE, POSTGRES_TABLE, "KBN"),
    (MYSQL_TABLE_MITRA, POSTGRES_TABLE_MITRA, "MITRA"),
]


# ==================== PIPELINE ====================
def run():
    logging.info("🔄 Sync MySQL Data Lake -> PostgreSQL started")

    total_rows = 0
    for mysql_table, postgres_table, label in PIPELINES:
        logging.info(f"   Source: MySQL {mysql_table} ({label})")
        logging.info(f"   Target: PostgreSQL {postgres_table} (d_transaction + d_location + jiwa-ksj)")

        postgres_rows = sync_mysql_to_postgresql(
            mysql_uri=MYSQL_URI_DATALAKE,
            mysql_table=mysql_table,
            pg_config=CONFIG_KSJ,
            pg_table=postgres_table,
            load_data_by=UPDATED_BY
        )
        total_rows += postgres_rows

        logging.info(f"✅ Sync complete ({label}) | PostgreSQL rows: {postgres_rows}")

    logging.info(f"✅ All sync complete | Total PostgreSQL rows: {total_rows}")


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
    dag_id="sync_sejutajiwa_riders_hubs_to_postgresql",
    default_args=default_args,
    description="Sync MySQL Data Lake -> PostgreSQL (triggered after KSJ Link ETL completes)",
    schedule=[DATASET_MYSQL_UPDATED],
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["sync", "postgresql", "master-data", "production"],
    max_active_runs=1
) as dag:

    PythonOperator(
        task_id="sync_to_postgresql",
        python_callable=run
    )
