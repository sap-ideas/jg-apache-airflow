"""
Microsoft Teams (Power Automate) report for iSeller Mitra DWH duplicate check.

Uses the same DataFrames as the email reports (outlet 01154 summary from Data Lake).
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from common.teams_powerautomate_report import (
    ADAPTIVE_CARD_SCHEMA,
    ADAPTIVE_CARD_VERSION,
    build_power_automate_payload,
    post_to_power_automate,
    resolve_power_automate_webhook_url,
    section_column_table,
    text_block,
)

logger = logging.getLogger(__name__)

_TABLE_HEADERS = ["transaction_date", "total_order_count", "total_amount"]
_TABLE_ALIGN = ["Left", "Right", "Right"]


def _get_webhook_url() -> str | None:
    return resolve_power_automate_webhook_url()


def _df_to_rows(df: pd.DataFrame) -> list[list[str]]:
    if df is None or df.empty:
        return []
    rows: list[list[str]] = []
    for _, row in df.iterrows():
        rows.append(
            [
                str(row["transaction_date"]),
                f"{int(row['total_order_count']):,}",
                f"{float(row['total_amount']):,.0f}",
            ]
        )
    rows.append(
        [
            "Grand Total",
            f"{int(df['total_order_count'].sum()):,}",
            f"{float(df['total_amount'].sum()):,.0f}",
        ]
    )
    return rows


def _outlet_01154_table_blocks(df: pd.DataFrame, section_title: str) -> list[dict[str, Any]]:
    rows = _df_to_rows(df)
    return section_column_table(
        section_title=section_title,
        column_headers=_TABLE_HEADERS,
        column_horizontal_alignments=_TABLE_ALIGN,
        rows=rows if rows else [["—", "—", "—"]],
        empty_message="No rows in this period.",
    )


def _meta_facts(dag_id: str, run_id: str, executed_at: str) -> dict[str, Any]:
    return {
        "type": "FactSet",
        "facts": [
            {"title": "DAG", "value": dag_id},
            {"title": "Run ID", "value": run_id},
            {"title": "Executed at", "value": executed_at},
        ],
    }


def send_mitra_dwh_teams_report_clean(
    *,
    report_df: pd.DataFrame,
    period_start: str,
    period_end: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    """No duplicates — green banner, single Outlet 01154 table."""
    url = webhook_url or _get_webhook_url()
    if not url:
        logger.warning("Teams Mitra: no webhook URL; skip.")
        return

    period = period_start if period_start == period_end else f"{period_start} – {period_end}"
    body: list[dict[str, Any]] = [
        text_block("Mitra Sales - Duplicate Data Check", weight="Bolder", size="Large"),
        text_block(f"Period: {period}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "good",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("All clear — no duplicate rows detected", weight="Bolder"),
                text_block(
                    "No duplicate header keys (order_id, transaction_id) in transactions_iseller_mitra "
                    "and no duplicate non-combo detail keys (composite key) in transactions_items_iseller_mitra "
                    "for this period. DWH summary re-dump is not required — data is already clean.",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
    ]
    body.extend(
        _outlet_01154_table_blocks(
            report_df,
            "Outlet 01154 — summary (same basis as email)",
        )
    )
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))

    card = {
        "type": "AdaptiveCard",
        "$schema": ADAPTIVE_CARD_SCHEMA,
        "version": ADAPTIVE_CARD_VERSION,
        "body": body,
    }
    post_to_power_automate(url, build_power_automate_payload(card))


def send_mitra_dwh_teams_report_duplicate_flow(
    *,
    total_duplicated_orders: int,
    total_duplicated_details: int,
    report_before: pd.DataFrame,
    report_after: pd.DataFrame,
    period_start: str,
    period_end: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    clean_stats: dict[str, int] | None = None,
    webhook_url: str | None = None,
    datalake_only: bool = False,
) -> None:
    """
    Duplicates were present: red banner, table before cleaning, then table after DL clean + DWH re-dump.
    Single Adaptive Card (same as combining duplicate alert + post-redump insight).
    """
    url = webhook_url or _get_webhook_url()
    if not url:
        logger.warning("Teams Mitra: no webhook URL; skip.")
        return

    period = period_start if period_start == period_end else f"{period_start} – {period_end}"
    body: list[dict[str, Any]] = [
        text_block("Mitra Sales - Duplicate Data Check", weight="Bolder", size="Large"),
        text_block(f"Period: {period}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "attention",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("Alert — duplicate data detected", weight="Bolder"),
                text_block(
                    f"{total_duplicated_orders:,} duplicate header key(s) (order_id, transaction_id) "
                    f"and {total_duplicated_details:,} duplicate non-combo detail key(s) (composite key) "
                    "found in Data Lake for this period.",
                    spacing="Small",
                ),
                text_block(
                    "Run mode datalake_only: Data Lake cleaned, DWH summary tables intentionally NOT reloaded "
                    "(run dwh_only to rebuild them)."
                    if datalake_only
                    else "The five DWH summary tables are being reloaded automatically after duplicate removal.",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
    ]

    if clean_stats:
        hb = clean_stats.get("headers_before")
        ha = clean_stats.get("headers_after")
        db = clean_stats.get("details_before")
        da = clean_stats.get("details_after")
        parts = []
        if hb is not None:
            parts.append(f"transaction headers {hb:,} → {ha:,} rows")
        if db is not None:
            parts.append(f"line items {db:,} → {da:,} rows")
        if parts:
            body.append(
                text_block(
                    f"Data Lake cleanup: {'; '.join(parts)}.",
                    is_subtle=True,
                    spacing="Medium",
                )
            )

    body.extend(
        _outlet_01154_table_blocks(
            report_before,
            "Outlet 01154 — before cleaning (includes duplicates)",
        )
    )
    body.extend(
        _outlet_01154_table_blocks(
            report_after,
            "Outlet 01154 — after Data Lake cleaning" if datalake_only else "Outlet 01154 — after cleaning & DWH re-dump",
        )
    )

    body.append(text_block("", spacing="Small"))
    body.append(
        text_block(
            "Compare the two tables above: lower order counts / amounts after cleaning indicate "
            "duplicate transactions were removed.",
            is_subtle=True,
        )
    )
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))

    card = {
        "type": "AdaptiveCard",
        "$schema": ADAPTIVE_CARD_SCHEMA,
        "version": ADAPTIVE_CARD_VERSION,
        "body": body,
    }
    post_to_power_automate(url, build_power_automate_payload(card))


def send_mitra_dwh_teams_report_master_error(
    *,
    title: str,
    message: str,
    action: str,
    reason: str,
    total_duplicated_headers: int,
    total_duplicated_details: int,
    period_start: str,
    period_end: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    """Pipeline STOP: master bundling (API iSeller Mitra) gagal di-load. DL & DWH tidak disentuh."""
    url = webhook_url or _get_webhook_url()
    if not url:
        logger.warning("Teams Mitra: no webhook URL; skip.")
        return

    period = period_start if period_start == period_end else f"{period_start} – {period_end}"
    body: list[dict[str, Any]] = [
        text_block("Mitra Sales - Duplicate Data Check", weight="Bolder", size="Large"),
        text_block(f"Period: {period}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "attention",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block(f"STOPPED — {title}", weight="Bolder"),
                text_block(message, spacing="Small"),
                text_block(
                    f"Duplikat terdeteksi ({total_duplicated_headers:,} header key, "
                    f"{total_duplicated_details:,} detail key non-combo) tapi cleaning TIDAK dijalankan: "
                    "Data Lake & DWH tidak disentuh.",
                    spacing="Small",
                ),
                text_block(action, is_subtle=True, spacing="Small"),
            ],
        },
        text_block("", spacing="Medium"),
        {
            "type": "FactSet",
            "facts": [{"title": "Reason", "value": reason}],
        },
        _meta_facts(dag_id, run_id, executed_at_display),
    ]

    card = {
        "type": "AdaptiveCard",
        "$schema": ADAPTIVE_CARD_SCHEMA,
        "version": ADAPTIVE_CARD_VERSION,
        "body": body,
    }
    post_to_power_automate(url, build_power_automate_payload(card))
