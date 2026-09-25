"""
Shared Teams notifications via Power Automate HTTP trigger (Adaptive Cards).

- POST JSON shape: {"adaptiveCard": { "type": "AdaptiveCard", ... }}
- Webhook URL: Airflow Variable (e.g. POWER_AUTOMATE_TEAMS_WEBHOOK_URL).

Do not put DAG-specific SQL or report copy here — colocate those under the DAG
folder (see .cursor/rules/teams-power-automate-reports.mdc).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterable

import requests

logger = logging.getLogger(__name__)

ADAPTIVE_CARD_SCHEMA = "http://adaptivecards.io/schemas/adaptive-card.json"
ADAPTIVE_CARD_VERSION = "1.5"

WEBHOOK_ENV_VAR = "POWER_AUTOMATE_TEAMS_WEBHOOK_URL"


def resolve_power_automate_webhook_url() -> str | None:
    """
    Resolve Power Automate webhook URL from one global place.

    Priority:
      1) Airflow Variable POWER_AUTOMATE_TEAMS_WEBHOOK_URL (if Airflow is available)
      2) Environment variable POWER_AUTOMATE_TEAMS_WEBHOOK_URL

    Returns:
      URL string, or None if not configured.
    """
    try:
        from airflow.models import Variable  # type: ignore

        w = (Variable.get(WEBHOOK_ENV_VAR, default_var="") or "").strip()
        if w:
            return w
    except Exception:
        # Airflow not importable in local/unit contexts; fall back to env var.
        pass

    w = (os.environ.get(WEBHOOK_ENV_VAR) or "").strip()
    return w or None


def require_power_automate_webhook_url() -> str:
    url = resolve_power_automate_webhook_url()
    if not url:
        raise ValueError(
            f"{WEBHOOK_ENV_VAR} is not set. Configure it as an Airflow Variable or environment variable."
        )
    return url


def format_int_commas(n: int | float) -> str:
    return f"{int(n):,}"


def text_block(
    text: str,
    *,
    weight: str = "Default",
    horizontal_alignment: str = "Left",
    size: str | None = None,
    is_subtle: bool = False,
    spacing: str | None = None,
    font_type: str | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "type": "TextBlock",
        "text": text,
        "wrap": True,
        "weight": weight,
        "horizontalAlignment": horizontal_alignment,
    }
    if size:
        block["size"] = size
    if is_subtle:
        block["isSubtle"] = True
    if spacing:
        block["spacing"] = spacing
    if font_type:
        block["fontType"] = font_type
    return block


def column_set(columns: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "ColumnSet", "columns": columns}


def column(width: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "Column", "width": width, "items": items}


def section_column_table(
    *,
    section_title: str,
    column_headers: list[str],
    column_horizontal_alignments: list[str],
    rows: list[list[str]],
    empty_message: str,
) -> list[dict[str, Any]]:
    """Build Adaptive Card body blocks: section title + header row + data rows."""
    if len(column_headers) != len(column_horizontal_alignments):
        raise ValueError("column_headers and column_horizontal_alignments must have same length")

    blocks: list[dict[str, Any]] = [
        text_block(section_title, weight="Bolder", size="Medium", spacing="Medium"),
    ]
    if not rows:
        blocks.append(text_block(empty_message, is_subtle=True))
        return blocks

    header_cells = []
    for h, align in zip(column_headers, column_horizontal_alignments):
        w = "auto" if align == "Right" else "stretch"
        header_cells.append(
            column(
                w,
                [text_block(h, weight="Bolder", horizontal_alignment=align)],
            )
        )
    blocks.append(column_set(header_cells))

    for row in rows:
        if len(row) != len(column_headers):
            raise ValueError("each row must have same length as column_headers")
        data_cells = []
        for cell, align in zip(row, column_horizontal_alignments):
            w = "auto" if align == "Right" else "stretch"
            data_cells.append(
                column(
                    w,
                    [text_block(str(cell), horizontal_alignment=align)],
                )
            )
        blocks.append(column_set(data_cells))

    return blocks


def build_etl_report_adaptive_card(
    *,
    report_title: str,
    report_date: str,
    dag_id: str,
    run_id: str,
    executed_at_iso: str,
    status: str,
    section_blocks: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    facts = [
        {"title": "DAG", "value": dag_id},
        {"title": "Run ID", "value": run_id},
        {"title": "Executed at", "value": executed_at_iso},
        {"title": "Status", "value": status},
    ]

    body: list[dict[str, Any]] = [
        text_block(report_title, weight="Bolder", size="Large"),
        text_block(f"Date: {report_date}", is_subtle=True, spacing="None"),
        {
            "type": "Container",
            "style": "good",
            "bleed": True,
            "spacing": "Medium",
            "items": [
                text_block("ETL completed successfully", weight="Bolder", spacing="None"),
                {"type": "FactSet", "facts": facts},
            ],
        },
    ]
    body.extend(section_blocks)

    return {
        "type": "AdaptiveCard",
        "$schema": ADAPTIVE_CARD_SCHEMA,
        "version": ADAPTIVE_CARD_VERSION,
        "body": body,
    }


def build_power_automate_payload(adaptive_card: dict[str, Any]) -> dict[str, Any]:
    return {"adaptiveCard": adaptive_card}


def post_to_power_automate(webhook_url: str, payload: dict[str, Any], timeout_sec: float = 30.0) -> None:
    logger.info("Posting Adaptive Card to Power Automate (POST, timeout=%ss).", timeout_sec)
    r = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout_sec,
    )
    r.raise_for_status()
    logger.info("Power Automate webhook POST ok status=%s", r.status_code)
