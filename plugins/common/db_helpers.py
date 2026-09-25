"""
Shared DB helpers used across all DAGs.

Single source of truth — update here, every DAG benefits.

Provides:
- MySQL Data Lake / Data Warehouse connections & engines (config_db_datawarehouse.py)
- KSJ Link PostgreSQL connections (config_db_ksj_link.py)
- JiwaPlus PostgreSQL connection (config_db_jiwaplus.py)
- Small DWH redump utilities (get_summary_before, print_before_after)

Importable from any DAG/pipeline file as:
    from common.db_helpers import (
        get_connection, get_engine, get_engine_datalake,
        ksj_link_db_conn, ksj_link_location_db_conn, jiwaplus_db_conn,
        get_summary_before, print_before_after,
    )
"""

import logging
import re
import socket
import time

import mysql.connector
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# If config already passes a literal IPv4, skip DNS entirely.
_IPV4_LITERAL = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


# ---------------------------------------------------------------------------
# MySQL — Data Lake & Data Warehouse  (config_db_datawarehouse.py)
# ---------------------------------------------------------------------------

def _mysql_tcp_host(hostname: str) -> str:
    """Resolve hostname to IPv4 for TCP; helps when Docker DNS is flaky (-3)."""
    if not hostname:
        return hostname
    h = hostname.strip()
    if _IPV4_LITERAL.match(h):
        return h
    try:
        infos = socket.getaddrinfo(h, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except OSError:
        try:
            return socket.gethostbyname(h)
        except OSError:
            pass
    return h


def _is_transient_mysql_network_err(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "name resolution" in msg
        or "temporary failure" in msg
        or "2003" in msg
        or "2005" in msg
        or "can't connect" in msg
        or "gaierror" in msg
    )


def get_connection(config_dw, db_name, max_retries: int = 8, base_delay_sec: float = 2.0):
    """Open a raw mysql.connector connection to a specific MySQL DB.

    Retries on transient Docker DNS failures (errno -3, 2003, 2005).
    """
    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        host = _mysql_tcp_host(config_dw.hostname)
        try:
            return mysql.connector.connect(
                host=host,
                port=3306,
                user=config_dw.username,
                password=config_dw.password,
                db=db_name,
                use_pure=True,
            )
        except mysql.connector.Error as e:
            last_exc = e
            if attempt < max_retries - 1 and _is_transient_mysql_network_err(e):
                wait = base_delay_sec * (2**attempt)
                logger.warning(
                    "MySQL connect %s/%s failed (%s); retry in %.1fs (resolved_host=%s cfg_host=%s)",
                    attempt + 1,
                    max_retries,
                    e,
                    wait,
                    host,
                    config_dw.hostname,
                )
                time.sleep(wait)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("get_connection: unreachable")


def _build_mysql_uri(config_dw, db_name):
    host = _mysql_tcp_host(config_dw.hostname)
    return (
        "mysql+pymysql://{user}:{pw}@{host}/{db}?charset=utf8mb4".format(
            host=host,
            user=config_dw.username,
            pw=config_dw.password,
            db=db_name,
        )
    )


def get_engine(config_dw):
    """SQLAlchemy engine for the MySQL Data Warehouse (config_dw.db_target)."""
    return create_engine(
        _build_mysql_uri(config_dw, config_dw.db_target),
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def get_engine_datalake(config_dw):
    """SQLAlchemy engine for the MySQL Data Lake (config_dw.db_resource)."""
    return create_engine(
        _build_mysql_uri(config_dw, config_dw.db_resource),
        pool_pre_ping=True,
        pool_recycle=3600,
    )


# ---------------------------------------------------------------------------
# PostgreSQL — KSJ Link  (config_db_ksj_link.py)
# ---------------------------------------------------------------------------

def ksj_link_db_conn(config_ksj, db_name):
    """Connect to a KSJ Link PostgreSQL database (transaction / rider / etc.)."""
    return psycopg2.connect(
        host=config_ksj.hostname,
        database=db_name,
        user=config_ksj.username,
        password=config_ksj.password,
        port=config_ksj.port,
    )


def jiwaplus_db_conn(config_jiwaplus):
    """Connect to the JiwaPlus consumer-app PostgreSQL database."""
    return psycopg2.connect(
        host=config_jiwaplus.hostname,
        database=config_jiwaplus.database,
        user=config_jiwaplus.username,
        password=config_jiwaplus.password,
        port=config_jiwaplus.port,
    )


def ksj_link_location_db_conn(config_ksj, db_name):
    """Connect to a KSJ Link Location-service PostgreSQL database (separate cluster)."""
    return psycopg2.connect(
        host=config_ksj.hostname_location,
        database=db_name,
        user=config_ksj.username,
        password=config_ksj.password,
        port=config_ksj.port,
    )


# ---------------------------------------------------------------------------
# Reusable DWH redump utilities
# ---------------------------------------------------------------------------

def get_summary_before(config_dw, table_name, sum_columns, start_date, end_date, sources=None):
    """Query a DWH summary table to get sum of columns BEFORE delete/redump.

    Args:
        config_dw: data-warehouse config module
        table_name: DWH summary table name
        sum_columns: list of numeric column names to aggregate
        start_date / end_date: date strings 'YYYY-MM-DD'
        sources: optional `sources` filter (e.g. 'iseller_pusat', 'iseller_mitra').
                 When None, no filter on `sources` is applied.

    Returns:
        dict {column_name: float_sum_value}
    """
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()

    select_parts = ", ".join([f"COALESCE(SUM({col}), 0)" for col in sum_columns])
    sources_filter = f"AND sources = '{sources}'" if sources else ""
    cursor.execute(f"""
        SELECT {select_parts}
        FROM {table_name}
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            {sources_filter}
    """)
    result = cursor.fetchone()
    conn.close()

    return {col: float(result[i]) for i, col in enumerate(sum_columns)}


def print_before_after(before, after):
    """Print BEFORE vs AFTER comparison for redump pipelines."""
    print(f"        --- Before vs After ---")
    for col in before:
        b, a = before[col], after[col]
        diff = a - b
        sign = "+" if diff >= 0 else ""
        print(f"        {col:20s}  BEFORE: {b:>15,.0f}  |  AFTER: {a:>15,.0f}  |  diff: {sign}{diff:,.0f}")
