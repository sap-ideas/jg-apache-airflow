"""Load - tulis rider_weekly_commission_additional ke KSJ Link PRODUCTION (PostgreSQL).

Idempotent via DELETE-lalu-INSERT dalam SATU transaksi (rollback kalau gagal). Hanya tabel
weekly yang di-load (tabel daily dihitung untuk bahan weekly tapi tidak ditulis). Tabel tidak
punya kolom `load_data_by`, jadi tag audit NEXUS_AIRFLOW tidak bisa dipakai di sini.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

BATCH_SIZE = 1000

TABLE_WEEKLY = "rider_weekly_commission_additional"


def get_engine_ksj_link(config_ksj):
    return create_engine(URL.create(
        "postgresql+psycopg2",
        username=config_ksj.username,
        password=config_ksj.password,
        host=config_ksj.hostname,
        port=int(config_ksj.port),
        database=config_ksj.d_transaction,
    ))


def _restart_sequence(conn, table_name):
    max_id = conn.execute(text(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}")).scalar()
    conn.execute(text(f"ALTER SEQUENCE {table_name}_id_seq RESTART WITH {max_id + 1}"))
    return max_id + 1


def _insert(conn, df, table_name):
    for i in range(0, len(df), BATCH_SIZE):
        df.iloc[i:i + BATCH_SIZE].to_sql(
            table_name, con=conn, if_exists="append", index=False, method="multi"
        )


def replace_weekly(conn, df, start_date, end_date):
    deleted = conn.execute(
        text(f"DELETE FROM {TABLE_WEEKLY} WHERE start_date = :s AND end_date = :e"),
        {"s": start_date, "e": end_date},
    ).rowcount
    _restart_sequence(conn, TABLE_WEEKLY)
    _insert(conn, df, TABLE_WEEKLY)
    return deleted, len(df)


def load_all(engine, df_weekly, start_date, end_date):
    """Return {table: (deleted, inserted)}."""
    with engine.begin() as conn:
        return {TABLE_WEEKLY: replace_weekly(conn, df_weekly, start_date, end_date)}
