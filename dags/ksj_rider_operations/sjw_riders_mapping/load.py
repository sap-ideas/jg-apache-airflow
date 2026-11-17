"""
MySQL loader with batch insert for better performance
Uses INSERT IGNORE for safe duplicate handling
Includes retry with exponential backoff for connection resilience
"""
import logging
import mysql.connector
import numpy as np
import pandas as pd
from urllib.parse import urlparse
from retry_helpers import retry_with_backoff


def convert_dataframe_to_records(df):
    """
    Convert DataFrame to list of tuples for database insertion
    Handles pandas/numpy type conversions
    """
    records = []
    for _, row in df.iterrows():
        record = tuple(
            None if pd.isna(val)
            else int(val) if isinstance(val, np.integer)
            else float(val) if isinstance(val, np.floating)
            else str(val) if not isinstance(val, (str, int, float, type(None)))
            else val
            for val in row
        )
        records.append(record)
    return records


def log_insert_summary(db_type, database, table_name, total, inserted, skipped, failed):
    """
    Print formatted summary box for insert operations
    """
    # Truncate names if too long for the box
    db_display = database[:15] if len(database) <= 15 else database[:12] + "..."
    table_display = table_name[:15] if len(table_name) <= 15 else table_name[:12] + "..."

    # Calculate padding for db_type
    header_padding = ' ' * (24 - len(db_type))

    logging.info(f"""
    ╔════════════════════════════════════╗
    ║   {db_type} INSERT SUMMARY{header_padding}║
    ╠════════════════════════════════════╣
    ║  Database:        {db_display:<15} ║
    ║  Table:           {table_display:<15} ║
    ║  Total Processed: {total:>4}            ║
    ║  ✅ Inserted:      {inserted:>4}            ║
    ║  ⚠️  Skipped:       {skipped:>4}            ║
    ║  ❌ Failed:        {failed:>4}            ║
    ╚════════════════════════════════════╝
    """)


def append_only_ignore_duplicates(df, mysql_uri, table_name):
    """
    Batch insert DataFrame to MySQL using INSERT IGNORE
    Retries up to 3 times with exponential backoff on failure.
    """
    parsed = urlparse(mysql_uri)
    db_name = parsed.path.strip('/')
    records = convert_dataframe_to_records(df)

    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    sql = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"

    logging.info(f"🚀 MySQL batch insert to {table_name} ({len(records)} rows)")

    def _do_insert():
        conn = mysql.connector.connect(
            host=parsed.hostname,
            user=parsed.username,
            password=parsed.password,
            database=db_name,
            port=parsed.port or 3306,
            autocommit=False
        )
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, records)
            inserted = cursor.rowcount
            conn.commit()
            return inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    inserted = retry_with_backoff(
        _do_insert,
        description=f"MySQL insert to {db_name}.{table_name}"
    )

    skipped = len(records) - inserted
    logging.info(f"✅ Inserted {inserted} rows to {db_name}.{table_name}")

    log_insert_summary('MYSQL', db_name, table_name, len(records), inserted, skipped, 0)

    return inserted
