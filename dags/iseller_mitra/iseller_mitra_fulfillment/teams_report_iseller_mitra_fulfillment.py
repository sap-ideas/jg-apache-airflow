"""
Teams (Power Automate) notifications for iSeller Mitra unfulfilled-order / backfill flow (Data Lake only).

Mirrors the same facts shown in etl_orchestrator email helpers — no extra queries.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

from common.teams_powerautomate_report import (
    ADAPTIVE_CARD_SCHEMA,
    ADAPTIVE_CARD_VERSION,
    build_power_automate_payload,
    format_int_commas,
    post_to_power_automate,
    resolve_power_automate_webhook_url,
    section_column_table,
    text_block,
)

logger = logging.getLogger(__name__)

CARD_TITLE = "iSeller Mitra — Unfulfilled orders (Data Lake)"


def _get_webhook_url() -> str | None:
    return resolve_power_automate_webhook_url()


def _meta_facts(dag_id: str, run_id: str, executed_at: str) -> dict[str, Any]:
    return {
        "type": "FactSet",
        "facts": [
            {"title": "DAG", "value": dag_id},
            {"title": "Run ID", "value": run_id},
            {"title": "Executed at", "value": executed_at},
        ],
    }


def _post_card(body: list[dict[str, Any]], webhook_url: str | None = None) -> None:
    url = webhook_url or _get_webhook_url()
    if not url:
        logger.warning("Teams Mitra: no webhook; skip.")
        return
    card = {
        "type": "AdaptiveCard",
        "$schema": ADAPTIVE_CARD_SCHEMA,
        "version": ADAPTIVE_CARD_VERSION,
        "body": body,
    }
    post_to_power_automate(url, build_power_automate_payload(card))


def post_teams_safe(label: str, fn: Callable[..., None], **kwargs: Any) -> None:
    try:
        fn(**kwargs)
        print(f"  Teams ({label}) sent.")
    except Exception as e:
        print(f"  WARNING: Teams ({label}) failed: {e}")


def _period_line(start_date: str, end_date: str) -> str:
    return start_date if start_date == end_date else f"{start_date} – {end_date}"


# --- Scenario senders ---


def send_teams_mitra_all_fulfilled(
    *,
    period_start: str,
    period_end: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "good",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("All clear — no unfulfilled orders", weight="Bolder"),
                text_block(
                    "Every order in the Data Lake is fulfilled. Nothing to backfill.",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
        text_block("", spacing="Medium"),
        _meta_facts(dag_id, run_id, executed_at_display),
    ]
    _post_card(body, webhook_url)


def send_teams_mitra_hub_sync_required(
    *,
    period_start: str,
    period_end: str,
    still_unfulfilled_count: int,
    total_amount: float = 0.0,
    by_outlet: list[dict[str, Any]] | None = None,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    by_outlet = by_outlet or []
    rows = [
        [
            str(r.get("outlet_code", "") or ""),
            str(r.get("outlet_name", "") or "-"),
            str(r.get("date", "") or ""),
            format_int_commas(int(r.get("total_orders_unfulfilled", 0) or 0)),
            f"{float(r.get('total_amount', 0) or 0):,.0f}",
        ]
        for r in by_outlet
    ]

    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "warning",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("Action required — orders still unfulfilled", weight="Bolder"),
                text_block(
                    "Data was pulled from the iSeller API, but no orders moved to fulfilled (0 fulfilled). "
                    "The pipeline stopped; Data Lake was not changed.",
                    spacing="Small",
                ),
                text_block(
                    f"Total unfulfilled orders: {still_unfulfilled_count:,}    |    "
                    f"Total amount: Rp {total_amount:,.0f}",
                    weight="Bolder",
                    spacing="Small",
                ),
                text_block(
                    "Ask Hub Manager to sync so orders can be fulfilled in iSeller, then re-run this DAG manually.",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
    ]
    body.extend(
        section_column_table(
            section_title="Unfulfilled orders by outlet",
            column_headers=["Jilid", "Nama Outlet", "Date", "Total Orders Unfulfilled", "Total Amount"],
            column_horizontal_alignments=["Left", "Left", "Left", "Right", "Right"],
            rows=rows,
            empty_message="No row detail available.",
        )
    )
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))
    _post_card(body, webhook_url)


def send_teams_mitra_token_expired(
    *,
    period_start: str,
    period_end: str,
    df_unfulfilled: pd.DataFrame | None,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    rows: list[list[str]] = []
    total_amount = 0.0
    if df_unfulfilled is not None and len(df_unfulfilled) > 0:
        total_amount = float(df_unfulfilled["total_amount"].astype(float).sum())
        for _, row in df_unfulfilled.iterrows():
            rows.append(
                [
                    str(row.get("date", "") or ""),
                    str(row.get("outlet_code", "") or ""),
                    str(row.get("order_id", "") or ""),
                    str(row.get("order_reference", "") or ""),
                    str(row.get("fulfillment_status", "") or ""),
                    f"{float(row.get('total_amount', 0) or 0):,.0f}",
                ]
            )

    headers = ["date", "outlet_code", "order_id", "order_reference", "fulfillment_status", "amount"]
    aligns = ["Left", "Left", "Left", "Left", "Left", "Right"]

    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "attention",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("Stopped — API token expired", weight="Bolder"),
                text_block(
                    "access_token_mitra is expired. Unfulfilled orders could not be processed. "
                    "Refresh the token in the environment and re-run.",
                    spacing="Small",
                ),
                text_block(
                    f"Unfulfilled orders in scope: {len(df_unfulfilled) if df_unfulfilled is not None else 0} "
                    f"(total amount Rp {total_amount:,.0f}).",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
    ]
    body.extend(
        section_column_table(
            section_title="Unfulfilled orders (same list as email)",
            column_headers=headers,
            column_horizontal_alignments=aligns,
            rows=rows,
            empty_message="No row detail available.",
        )
    )
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))
    _post_card(body, webhook_url)


def send_teams_mitra_missing_access_token(
    *,
    period_start: str,
    period_end: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "attention",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("Stopped — access_token_mitra not set", weight="Bolder"),
                text_block(
                    "Environment variable access_token_mitra is missing or empty on the worker. "
                    "Backfill cannot run. Set it in .env / Compose and restart services.",
                    spacing="Small",
                ),
            ],
        },
        text_block("", spacing="Medium"),
        _meta_facts(dag_id, run_id, executed_at_display),
    ]
    _post_card(body, webhook_url)


def send_teams_mitra_backfill_failed(
    *,
    period_start: str,
    period_end: str,
    error_message: str,
    error_type: str = "Exception",
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    err_preview = (error_message or "").strip()[:800] or "(no message)"
    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "attention",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block(f"Stopped — backfill failed: {error_type}", weight="Bolder"),
                text_block(
                    "Backfill step raised an exception. Data Lake writes are rolled back. Check task logs for full traceback.",
                    spacing="Small",
                ),
                text_block("Log detail:", weight="Bolder", size="Small", spacing="Medium"),
                text_block(err_preview, is_subtle=True, spacing="None", font_type="Monospace"),
            ],
        },
        text_block("", spacing="Medium"),
        _meta_facts(dag_id, run_id, executed_at_display),
    ]
    _post_card(body, webhook_url)


def send_teams_mitra_completed(
    *,
    period_start: str,
    period_end: str,
    backfill_stats: dict[str, Any],
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    body: list[dict[str, Any]] = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "good",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("ETL completed — Data Lake updated", weight="Bolder"),
                text_block(
                    "Backfill fulfillment finished. DWH summary tables are not part of this pipeline. Summary is shown below.",
                    is_subtle=True,
                    spacing="Small",
                ),
            ],
        },
    ]
    body.extend(_backfill_summary_blocks(backfill_stats or {}))
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))
    _post_card(body, webhook_url)


def _backfill_summary_blocks(backfill_stats: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        ["Orders fulfilled", format_int_commas(int(backfill_stats.get("total_fulfilled", 0)))],
        ["Fulfilled amount", f"Rp {float(backfill_stats.get('fulfilled_amount', 0) or 0):,.0f}"],
        ["Orders still unfulfilled", format_int_commas(int(backfill_stats.get("total_unfulfilled", 0)))],
        ["Unfulfilled amount", f"Rp {float(backfill_stats.get('unfulfilled_amount', 0) or 0):,.0f}"],
        ["Detail rows dumped", format_int_commas(int(backfill_stats.get("total_details_dumped", 0)))],
    ]
    return section_column_table(
        section_title="Backfill fulfillment",
        column_headers=["Metric", "Value"],
        column_horizontal_alignments=["Left", "Right"],
        rows=rows,
        empty_message="—",
    )
