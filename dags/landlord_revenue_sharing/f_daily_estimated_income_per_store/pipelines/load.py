"""Load functions - DELETE+INSERT the target date into the Data Warehouse."""

from datetime import datetime

from pytz import timezone
from sqlalchemy import text

from common.db_helpers import get_engine

TARGET_TABLE = "daily_est_income_per_store_for_landlord"
DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"
LOCAL_TZ = timezone("Asia/Jakarta")


def load_daily_income(config_dw, df, target_date_str, load_data_by=DEFAULT_LOAD_DATA_BY):
    """Atomic DELETE + INSERT for target_date_str — safe to retry.

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if df.empty:
        return 0, 0

    df = df.copy()
    df['load_data_at'] = datetime.now(LOCAL_TZ).replace(tzinfo=None)
    df['load_data_by'] = load_data_by

    engine = get_engine(config_dw)
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TARGET_TABLE} WHERE date = :d"),
            {"d": target_date_str},
        )
        deleted = result.rowcount

        df.to_sql(TARGET_TABLE, con=conn, if_exists="append", index=False)

    return deleted, len(df)
