"""Load - UPDATE html_content (+ updated_at) row dashboard 01165 di data_report.custom_menus (PostgreSQL)."""

import psycopg2

CUSTOM_MENU_ID = 80
CUSTOM_MENU_NAME = "OPR - Summarecon Emerald Karawang Sales"


def update_custom_menu(config_jiwaplus, html):
    """Satu transaksi. Hanya html_content + updated_at; baris lain / kolom lain tidak disentuh.

    updated_at = UTC (kolom timestamp tanpa TZ, nilai existing juga UTC).
    Rowcount != 1 -> rollback + raise.
    """
    conn = psycopg2.connect(
        host=config_jiwaplus.hostname,
        database=config_jiwaplus.database_data_report,
        user=config_jiwaplus.username,
        password=config_jiwaplus.password,
        port=config_jiwaplus.port,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE custom_menus
                   SET html_content = %s,
                       updated_at   = (NOW() AT TIME ZONE 'UTC')
                 WHERE id = %s
                   AND name = %s
                """,
                (html, CUSTOM_MENU_ID, CUSTOM_MENU_NAME),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"UPDATE custom_menus id={CUSTOM_MENU_ID} mengubah {cur.rowcount} baris (harus 1); rollback"
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"  Updated custom_menus id={CUSTOM_MENU_ID} ({len(html):,} chars)")
    return 1
