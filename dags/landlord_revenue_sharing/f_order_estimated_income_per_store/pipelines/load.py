"""Load functions - DELETE range + INSERT into the Data Warehouse in one transaction."""

from datetime import datetime

from pytz import timezone
from sqlalchemy import text

from common.db_helpers import get_engine
from extract import OUTLET_CODES

TARGET_TABLE = "order_est_income_for_landlord"
DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"
LOCAL_TZ = timezone("Asia/Jakarta")


def load_order_income(config_dw, df, start_date_str, end_date_str, load_data_by=DEFAULT_LOAD_DATA_BY):
    """Atomic DELETE (date range + OUTLET_CODES) + INSERT - safe to retry.

    Returns:
        (deleted_rowcount, inserted_rowcount). df kosong -> tabel tidak disentuh, (0, 0).
    """
    if df.empty:
        return 0, 0

    df = df.copy()
    df["load_data_at"] = datetime.now(LOCAL_TZ).replace(tzinfo=None)
    df["load_data_by"] = load_data_by

    outlet_params = {f"o{i}": o for i, o in enumerate(OUTLET_CODES)}
    outlet_in = ", ".join(f":{k}" for k in outlet_params)

    engine = get_engine(config_dw)
    with engine.begin() as conn:
        deleted = conn.execute(
            text(f"DELETE FROM {TARGET_TABLE} WHERE date BETWEEN :s AND :e AND outlet_code IN ({outlet_in})"),
            {"s": start_date_str, "e": end_date_str, **outlet_params},
        ).rowcount
        df.to_sql(TARGET_TABLE, con=conn, if_exists="append", index=False)

    return deleted, len(df)
