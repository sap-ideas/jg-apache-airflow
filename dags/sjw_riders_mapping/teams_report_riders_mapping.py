"""
Teams / Power Automate report for the Sejutajiwa riders KSJ link mapping DAG.

DAG-specific SQL lives under sql/; shared Adaptive Card + HTTP helpers live in
common.teams_powerautomate_report.

Email uses the same summary rows + plain-text body as the Teams card (HUB / SJO breakdown).
"""

from __future__ import annotations

import html as html_lib
import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from common.teams_powerautomate_report import (
    build_etl_report_adaptive_card,
    build_power_automate_payload,
    format_int_commas,
    post_to_power_automate,
    text_block,
)

_SQL_FILE = Path(__file__).resolve().parent / "sql" / "riders_mapping_teams_summary.sql"

logger = logging.getLogger(__name__)

# Same SMTP defaults as other DatO ETL DAGs (iSeller Pusat / Mitra / Rider Mangkal)
_SMTP_PORT = 587
_SMTP_SERVER = "smtp.gmail.com"
_SENDER_EMAIL = "saputra.christabel20@gmail.com"
_RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
_SMTP_PASSWORD = os.getenv("GMAIL_SMTP_PASSWORD")


def _load_summary_sql() -> str:
    return _SQL_FILE.read_text(encoding="utf-8")


@dataclass(frozen=True)
class RiderTypeCount:
    sejuta_jiwa_type: str
    total_riders: int


def fetch_riders_mapping_summary(engine: Engine) -> list[RiderTypeCount]:
    sql = _load_summary_sql()
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    out: list[RiderTypeCount] = []
    for r in rows:
        out.append(
            RiderTypeCount(
                sejuta_jiwa_type=str(r["sejuta_jiwa_type"] or ""),
                total_riders=int(r["total_riders"] or 0),
            )
        )
    return out


def build_riders_mapping_report_lines(rows: list[RiderTypeCount]) -> list[str]:
    """Same lines as the Teams Adaptive Card body (Total Riders Mapped + per-type counts)."""
    lines = ["Total Riders Mapped:"]
    if not rows:
        lines.append("No rows for today.")
    else:
        for r in rows:
            label = r.sejuta_jiwa_type.strip() if r.sejuta_jiwa_type else "—"
            lines.append(f"{label}: {format_int_commas(r.total_riders)} riders")
    return lines


def format_riders_mapping_report_plain(rows: list[RiderTypeCount]) -> str:
    """Plain text identical to the Teams summary block."""
    return "\n".join(build_riders_mapping_report_lines(rows))


def _total_riders_mapped_section(rows: list[RiderTypeCount]) -> list[dict[str, Any]]:
    """Plain multiline summary — no ColumnSet table."""
    return [text_block(format_riders_mapping_report_plain(rows), spacing="Medium")]


def send_riders_mapping_teams_report(
    *,
    engine: Engine,
    webhook_url: str,
    report_title: str,
    report_date: str,
    dag_id: str,
    run_id: str,
    executed_at_iso: str,
    summary_rows: list[RiderTypeCount] | None = None,
) -> None:
    rows = summary_rows if summary_rows is not None else fetch_riders_mapping_summary(engine)
    section_blocks = _total_riders_mapped_section(rows)
    card = build_etl_report_adaptive_card(
        report_title=report_title,
        report_date=report_date,
        dag_id=dag_id,
        run_id=run_id,
        executed_at_iso=executed_at_iso,
        status="SUCCESS",
        section_blocks=section_blocks,
    )
    post_to_power_automate(webhook_url, build_power_automate_payload(card))


def send_riders_mapping_email_report(
    *,
    rows: list[RiderTypeCount],
    report_date: str,
    dag_id: str,
    run_id: str,
    executed_at_display: str,
) -> None:
    """HTML email with the same summary text as Teams (HUB / SJO rider counts for the day)."""
    body_plain = format_riders_mapping_report_plain(rows)
    body_escaped = html_lib.escape(body_plain)
    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#4472C4;">Riders Mapping Report</h2>
        <p>Report date: <strong>{html_lib.escape(report_date)}</strong></p>
        <p>
            DAG: <code>{html_lib.escape(dag_id)}</code><br>
            Run: <code>{html_lib.escape(run_id)}</code><br>
            Executed: <strong>{html_lib.escape(executed_at_display)}</strong>
        </p>
        <p style="margin:0 0 8px 0;color:#555;">Total riders mapped (by type — same as Teams):</p>
        <pre style="font-family:Consolas,'Courier New',monospace;font-size:14px;
background:#f5f5f5;border:1px solid #ddd;padding:12px;border-radius:4px;white-space:pre-wrap;">{body_escaped}</pre>
        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[REPORT] Riders Mapping — {report_date} (HUB / SJO breakdown)"

    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = _SENDER_EMAIL
    msg["To"] = ", ".join(_RECEIVER_EMAIL)

    context = ssl.create_default_context()
    with smtplib.SMTP(_SMTP_SERVER, _SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(_SENDER_EMAIL, _SMTP_PASSWORD)
        server.sendmail(_SENDER_EMAIL, _RECEIVER_EMAIL, msg.as_string())

    logger.info("Riders mapping report email sent to: %s", ", ".join(_RECEIVER_EMAIL))
