"""
Teams (Power Automate) notifications untuk backfilling iSeller Pusat (order yang tidak ada di Data Lake).

Mirror fakta yang sama dengan email di etl_orchestrator — tanpa query tambahan. Best-effort via post_teams_safe.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

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

CARD_TITLE = "iSeller Pusat — Backfilling (order tidak ada di Data Lake)"


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
    url = webhook_url or resolve_power_automate_webhook_url()
    if not url:
        logger.warning("Teams Pusat backfilling: no webhook; skip.")
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


def _status_container(style: str, headline: str, detail: str) -> dict[str, Any]:
    return {
        "type": "Container",
        "style": style,
        "bleed": True,
        "spacing": "Medium",
        "items": [
            text_block(headline, weight="Bolder"),
            text_block(detail, is_subtle=True, spacing="Small"),
        ],
    }


def _stats_blocks(stats: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        ["Order dari API", format_int_commas(int(stats.get("orders_from_api", 0)))],
        ["Sudah ada di Data Lake", format_int_commas(int(stats.get("orders_already_in_dl", 0)))],
        ["Order baru di-insert", format_int_commas(int(stats.get("orders_inserted", 0)))],
        ["Header rows", format_int_commas(int(stats.get("headers_inserted", 0)))],
        ["Details rows", format_int_commas(int(stats.get("details_inserted", 0)))],
        ["Amount header", f"Rp {float(stats.get('headers_amount', 0) or 0):,.0f}"],
        ["Amount details", f"Rp {float(stats.get('details_amount', 0) or 0):,.0f}"],
    ]
    return section_column_table(
        section_title="Backfilling",
        column_headers=["Metric", "Value"],
        column_horizontal_alignments=["Left", "Right"],
        rows=rows,
        empty_message="—",
    )


def send_teams_backfilling_completed(
    *,
    period_start: str,
    period_end: str,
    stats: dict[str, Any],
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    dry_run: bool = False,
    webhook_url: str | None = None,
) -> None:
    n = int(stats.get("orders_inserted", 0))
    headline = ("DRY RUN — tidak ada yang ditulis" if dry_run
                else ("Backfilling selesai — Data Lake sudah lengkap" if n == 0 else "Backfilling selesai — Data Lake diperbarui"))
    detail = ("Semua order dari API sudah ada di Data Lake." if n == 0
              else f"{format_int_commas(n)} order yang sebelumnya tidak ada {'AKAN ' if dry_run else ''}dimasukkan.")
    body = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        _status_container("good", headline, detail),
    ]
    body.extend(_stats_blocks(stats))
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))
    _post_card(body, webhook_url)


def send_teams_backfilling_partial(
    *,
    period_start: str,
    period_end: str,
    stats: dict[str, Any],
    failed_calls: list[tuple],
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    body = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        _status_container(
            "warning",
            f"PARTIAL — {len(failed_calls)} panggilan API tetap gagal",
            "Data yang berhasil sudah masuk. Task digagalkan supaya Airflow retry: run ulang hanya memasukkan order yang belum ada.",
        ),
    ]
    body.extend(_stats_blocks(stats))
    body.extend(section_column_table(
        section_title="Panggilan gagal (maks 15)",
        column_headers=["Outlet", "Jendela (UTC)"],
        column_horizontal_alignments=["Left", "Left"],
        rows=[[o, f"{s} ~ {e}"] for o, s, e, _m in failed_calls[:15]],
        empty_message="—",
    ))
    body.append(text_block("", spacing="Medium"))
    body.append(_meta_facts(dag_id, run_id, executed_at_display))
    _post_card(body, webhook_url)


def send_teams_backfilling_failed(
    *,
    period_start: str,
    period_end: str,
    error_type: str,
    error_message: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
    webhook_url: str | None = None,
) -> None:
    body = [
        text_block(CARD_TITLE, weight="Bolder", size="Large"),
        text_block(f"Period: {_period_line(period_start, period_end)}", is_subtle=True, spacing="None"),
        _status_container("attention", f"Pipeline dihentikan — {error_type}", (error_message or "")[:600]),
        text_block("", spacing="Medium"),
        _meta_facts(dag_id, run_id, executed_at_display),
    ]
    _post_card(body, webhook_url)
