"""
Extract Missing KSJ Link Riders
Fetches riders from KSJ Link PostgreSQL that are not yet mapped.

Two ownership segments share these functions via parameters instead of
duplicated code: KBN (default) and MITRA.
"""
import pandas as pd
import psycopg2
import pymysql
import logging


def get_missing_ksj_link_riders(config_ksj, config_dwh, ownership_status='KBN',
                                  mapping_table='d_sejutajiwa_riders_hubs_mapping'):
    """
    Get riders from KSJ Link transactions that are missing in the mapping table.
    Queries d_transaction database for completed transactions in the last 10 days.

    Args:
        config_ksj: KSJ Link PostgreSQL config
        config_dwh: Data warehouse MySQL config (for checking existing mappings)
        ownership_status: 'KBN' or 'MITRA' — which ownership segment to extract
        mapping_table: Postgres mapping table to check against (existing vs _mitra)

    Returns:
        DataFrame with missing riders
    """
    logging.info(f"Connecting to KSJ Link d_transaction database ({ownership_status})...")

    connection = psycopg2.connect(
        host=config_ksj.hostname,
        database=config_ksj.d_transaction,
        user=config_ksj.username,
        password=config_ksj.password,
        port=config_ksj.port
    )
    cursor = connection.cursor()

    query = f"""
        SELECT
            DISTINCT rider_code,
            transactions.rider_name,
            jilid,
            hub_name AS outlet_name
        FROM
            transactions
        LEFT JOIN {mapping_table}
            ON transactions.rider_code = {mapping_table}.rider_id
        WHERE
            transaction_status = 'COMPLETED'
            AND jilid IS NULL
            AND transactions.transaction_date >= CURRENT_DATE - INTERVAL '10 days'
            AND transactions.ownership_status = %s
    """

    logging.info(f"Executing query to find missing {ownership_status} riders...")
    cursor.execute(query, (ownership_status,))

    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()

    logging.info(f"Found {len(df)} missing {ownership_status} riders")
    return df


def get_rider_list(config_ksj):
    """
    Get full rider list from KSJ Link d_rider database.
    
    Args:
        config_ksj: KSJ Link PostgreSQL config
    
    Returns:
        DataFrame with all riders
    """
    logging.info("Fetching rider list from KSJ Link d_rider database...")
    
    connection = psycopg2.connect(
        host=config_ksj.hostname,
        database=config_ksj.d_rider,
        user=config_ksj.username,
        password=config_ksj.password,
        port=config_ksj.port
    )
    cursor = connection.cursor()
    
    query = """
        SELECT * FROM riders
    """
    
    cursor.execute(query)

    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    
    logging.info(f"Fetched {len(df)} riders from rider list")
    return df


def get_hub_id(config_dwh):
    """
    Get hub ID mapping from Data Lake MySQL (KBN riders only).

    Args:
        config_dwh: Data warehouse MySQL config

    Returns:
        DataFrame with hub_id, jilid, hub_name mapping
    """
    logging.info("Fetching hub ID mapping from Data Lake...")

    connection = pymysql.connect(
        host=config_dwh.hostname,
        db=config_dwh.db_resource,  # data_lake_jiwa
        user=config_dwh.username,
        password=config_dwh.password,
        port=3306
    )
    cursor = connection.cursor()

    query = """
        SELECT DISTINCT jilid, hub_id, hub_name
        FROM sejutajiwa_riders_hubs_mapping
    """

    cursor.execute(query)
    records = cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()

    logging.info(f"Fetched {len(df)} hub mappings")
    return df


def get_mitra_hub_outlets(config_jiwaplus):
    """
    Get MITRA hub outlets from JiwaPlus (hub source for MITRA riders only).
    Shaped to the same hub_id/jilid/hub_name columns as get_hub_id() so
    transform_ksj_link() can merge it the same way regardless of segment:
    outlet_code doubles as both hub_id and jilid for MITRA.

    Args:
        config_jiwaplus: JiwaPlus PostgreSQL config

    Returns:
        DataFrame with hub_id, jilid, hub_name mapping
    """
    logging.info("Fetching MITRA hub outlets from JiwaPlus outlet table...")

    connection = psycopg2.connect(
        host=config_jiwaplus.hostname,
        database=config_jiwaplus.database,
        user=config_jiwaplus.username,
        password=config_jiwaplus.password,
        port=config_jiwaplus.port
    )
    cursor = connection.cursor()

    query = """
        SELECT outlet_code, outlet_name
        FROM outlet
        WHERE outlet_ownership_status = 'MITRA'
          AND ksj_hub_type = 'HUB'
    """

    cursor.execute(query)
    records = cursor.fetchall()

    columns = [col[0] for col in cursor.description]
    df_outlet = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()

    df_hub = pd.DataFrame({
        'hub_id': df_outlet['outlet_code'],
        'jilid': df_outlet['outlet_code'],
        'hub_name': df_outlet['outlet_name'],
    })

    logging.info(f"Fetched {len(df_hub)} MITRA hub outlets")
    return df_hub

