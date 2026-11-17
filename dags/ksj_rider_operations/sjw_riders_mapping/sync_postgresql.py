"""
PostgreSQL sync loader
Copies ENTIRE MySQL Data Lake table to PostgreSQL (TRUNCATE + INSERT strategy)
Preserves table structure and permissions (no DROP/CREATE).
Includes retry with exponential backoff for connection resilience.

Flow:
  1. SELECT DISTINCT from MySQL Data Lake -> DataFrame (with retry)
  2. Add load_data_at, load_data_by columns
  3. TRUNCATE target table -> to_sql(if_exists='append') (with retry)
"""
import logging
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote
from retry_helpers import retry_with_backoff


def sync_mysql_to_postgresql(mysql_uri, mysql_table, pg_config, pg_table,
                              load_data_by='NEXUS_AIRFLOW'):
    """
    Read ALL data from MySQL table, then TRUNCATE + INSERT in PostgreSQL.
    Dumps to d_transaction, d_location, and d_jiwa_ksj (jiwa-ksj) databases.
    Table structure and permissions are preserved (no DROP TABLE).
    """
    # ========================================
    # STEP 1: Read ALL data from MySQL (with retry)
    # ========================================
    logging.info(f"📥 Reading all data from MySQL: {mysql_table}")

    def _read_mysql():
        engine = create_engine(mysql_uri)
        try:
            return pd.read_sql(f"""
                SELECT DISTINCT
                    hub_id, jilid, hub_name, rider_id, cart_id,
                    rider_name, status, scheme, phone_number, cashier_id
                FROM {mysql_table}
            """, con=engine)
        finally:
            engine.dispose()

    df = retry_with_backoff(_read_mysql, description=f"MySQL read from {mysql_table}")

    logging.info(f"📊 Fetched {len(df)} rows from MySQL Data Lake")

    if df.empty:
        logging.warning("⚠️ No data found in MySQL table. Skipping PostgreSQL sync.")
        return 0

    # ========================================
    # STEP 2: Add metadata columns
    # ========================================
    df['load_data_at'] = datetime.datetime.now()
    df['load_data_by'] = load_data_by

    # ========================================
    # STEP 3: TRUNCATE + INSERT in PostgreSQL (with retry per DB)
    # ========================================
    pg_databases = [pg_config.d_transaction, pg_config.d_location, pg_config.d_jiwa_ksj]
    total_rows = 0

    for db_name in pg_databases:
        pg_host = (
            getattr(pg_config, "hostname_location", pg_config.hostname)
            if db_name == pg_config.d_location
            else pg_config.hostname
        )
        logging.info(f"📤 Syncing to PostgreSQL {db_name}.{pg_table} @ {pg_host}...")

        def _truncate_and_insert(db=db_name, host=pg_host):
            engine = create_engine(
                f"postgresql://{pg_config.username}:{quote(pg_config.password)}"
                f"@{host}:{pg_config.port}/{db}"
            )
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {pg_table}"))
                    logging.info(f"🗑️ Truncated {pg_table} in {db}")

                return df.to_sql(pg_table, con=engine, if_exists='append', index=False)
            finally:
                engine.dispose()

        rows_written = retry_with_backoff(
            _truncate_and_insert,
            description=f"PostgreSQL sync {db_name}.{pg_table}"
        )

        row_count = rows_written if rows_written is not None else len(df)
        total_rows += row_count

        logging.info(f"✅ Synced {pg_table} in {db_name}: {row_count} rows")

    # ========================================
    # SUMMARY
    # ========================================
    db_display = "d_transaction + d_location + jiwa-ksj"
    table_display = pg_table[:15] if len(pg_table) <= 15 else pg_table[:12] + "..."

    logging.info(f"""
    ╔════════════════════════════════════════════╗
    ║   POSTGRESQL SYNC SUMMARY                 ║
    ╠════════════════════════════════════════════╣
    ║  Source:    MySQL Data Lake                ║
    ║  Table:     {table_display:<28} ║
    ║  Rows:      {len(df):<28} ║
    ║  Targets:   {db_display:<28} ║
    ║  Strategy:  TRUNCATE + INSERT              ║
    ║  Status:    ✅ SUCCESS                      ║
    ╚════════════════════════════════════════════╝
    """)

    return total_rows
