"""
send_alerts.py
--------------
Checks alert conditions against analytics.sqlite and delivers notifications
via Slack webhook and/or email.

Alert conditions:
  1. Compression Alert  — any kpi_daily_summary occ_pct > 80 in next 30 days
  2. RevPAR Drop        — 7-day avg RevPAR down >5% vs prior 7-day avg
  3. High-Priority Insight — priority=1 insights inserted in last 24 h

Delivery env vars:
    SLACK_WEBHOOK_URL  -- Slack incoming webhook URL
    ALERT_EMAIL_TO     -- recipient(s), comma-separated (uses DIGEST_SMTP_* vars)
    DIGEST_EMAIL_FROM  -- sender address
    DIGEST_SMTP_HOST   -- SMTP host (default: smtp.gmail.com)
    DIGEST_SMTP_PORT   -- SMTP port (default: 587)
    DIGEST_SMTP_USER   -- SMTP username
    DIGEST_SMTP_PASS   -- SMTP password

If neither SLACK_WEBHOOK_URL nor ALERT_EMAIL_TO is set, alerts are printed
to stdout and the script exits cleanly (no-op mode).

Run:
    python3 scripts/send_alerts.py
"""

from __future__ import annotations

import json
import os
import smtplib
import sqlite3
import urllib.request
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")

TODAY    = date.today()
TODAY_S  = TODAY.isoformat()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)


def check_compression_alert() -> str | None:
    """Return alert string if high-occ days found in next 30 days, else None."""
    cutoff = (TODAY + timedelta(days=30)).isoformat()
    try:
        with _connect() as con:
            df = pd.read_sql_query(
                """
                SELECT as_of_date, occ_pct
                FROM kpi_daily_summary
                WHERE as_of_date > ? AND as_of_date <= ?
                  AND occ_pct > 80
                ORDER BY as_of_date
                """,
                con,
                params=(TODAY_S, cutoff),
            )
        if df.empty:
            return None
        n = len(df)
        return (
            f"Compression Alert: {n} high-occupancy day{'s' if n != 1 else ''} detected "
            f"in next 30 days. Rate floor opportunity."
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[{_ts()}] ALERT WARNING: compression check failed — {exc}")
        return None


def check_revpar_drop() -> str | None:
    """Return alert string if 7-day avg RevPAR dropped >5%, else None."""
    cutoff_recent = TODAY_S
    cutoff_prior  = (TODAY - timedelta(days=7)).isoformat()
    cutoff_oldest = (TODAY - timedelta(days=14)).isoformat()
    try:
        with _connect() as con:
            df = pd.read_sql_query(
                """
                SELECT as_of_date, revpar
                FROM kpi_daily_summary
                WHERE as_of_date > ? AND as_of_date <= ?
                ORDER BY as_of_date DESC
                LIMIT 14
                """,
                con,
                params=(cutoff_oldest, cutoff_recent),
            )
        if df.empty or len(df) < 7:
            return None

        df = df.sort_values("as_of_date")
        recent_7 = df.tail(7)["revpar"].dropna()
        prior_7  = df.head(7)["revpar"].dropna()

        if recent_7.empty or prior_7.empty:
            return None

        avg_recent = recent_7.mean()
        avg_prior  = prior_7.mean()

        if avg_prior == 0:
            return None

        delta_pct = (avg_recent - avg_prior) / avg_prior * 100
        if delta_pct < -5:
            return (
                f"RevPAR softness: 7-day avg ${avg_recent:,.0f} vs ${avg_prior:,.0f} "
                f"({delta_pct:+.1f}%)"
            )
        return None
    except Exception as exc:  # noqa: BLE001
        print(f"[{_ts()}] ALERT WARNING: RevPAR check failed — {exc}")
        return None


def check_priority_insights() -> list[str]:
    """Return headlines of priority=1 insights inserted in last 24 h."""
    since = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%d")
    try:
        with _connect() as con:
            df = pd.read_sql_query(
                """
                SELECT headline, audience
                FROM insights_daily
                WHERE priority = 1
                  AND as_of_date >= ?
                ORDER BY audience
                """,
                con,
                params=(since,),
            )
        return [
            f"[{row['audience'].upper()}] {row['headline']}"
            for _, row in df.iterrows()
        ]
    except Exception as exc:  # noqa: BLE001
        print(f"[{_ts()}] ALERT WARNING: insights check failed — {exc}")
        return []


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _build_alert_lines(
    compression: str | None,
    revpar: str | None,
    priority_insights: list[str],
) -> list[str]:
    lines = []
    if compression:
        lines.append(f"🏨 {compression}")
    if revpar:
        lines.append(f"⚠️ {revpar}")
    for headline in priority_insights:
        lines.append(f"🔔 High-Priority Insight: {headline}")
    return lines


# ---------------------------------------------------------------------------
# Slack delivery
# ---------------------------------------------------------------------------

def _build_slack_payload(alert_lines: list[str]) -> dict:
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Dana Point PULSE Alert — {TODAY_S}"},
        }
    ]
    for line in alert_lines:
        # Split emoji prefix (first word) as headline, rest as body
        parts  = line.split(" ", 2)
        emoji  = parts[0] if parts else ""
        rest   = " ".join(parts[1:]) if len(parts) > 1 else line
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{emoji} {rest}*"},
            }
        )
        blocks.append({"type": "divider"})

    return {
        "text": f"Dana Point PULSE: {len(alert_lines)} alert(s) on {TODAY_S}",
        "blocks": blocks,
    }


def send_slack(alert_lines: list[str]) -> None:
    url     = os.environ.get("SLACK_WEBHOOK_URL", "")
    payload = json.dumps(_build_slack_payload(alert_lines)).encode("utf-8")
    req     = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
        print(f"[{_ts()}] ALERT: Slack delivery status {status}")
    except Exception as exc:  # noqa: BLE001
        print(f"[{_ts()}] ALERT WARNING: Slack send failed — {exc}")


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

def _build_alert_html(alert_lines: list[str]) -> str:
    cards = ""
    for line in alert_lines:
        cards += f"""
<div style="background:#fff3cd;border-left:4px solid #e65100;border-radius:4px;
            padding:12px 14px;margin-bottom:10px;font-family:Arial,sans-serif;">
  <div style="font-size:14px;color:#222;">{line}</div>
</div>"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f8f9fa;font-family:Arial,Helvetica,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0"
       style="background:#f8f9fa;padding:24px 0;">
  <tr>
    <td align="center">
      <table role="presentation" width="600" cellpadding="0" cellspacing="0"
             style="max-width:600px;width:100%;background:#ffffff;border-radius:8px;
                    overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
        <tr>
          <td style="background:#b71c1c;padding:22px 32px;">
            <div style="font-size:18px;font-weight:700;color:#ffffff;">
              Dana Point PULSE — Alert Notification
            </div>
            <div style="font-size:12px;color:#ffcdd2;margin-top:4px;">{TODAY_S}</div>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 32px;">
            {cards}
          </td>
        </tr>
        <tr>
          <td style="background:#1a3a5c;padding:14px 32px;text-align:center;">
            <div style="font-size:11px;color:#aec6e8;">
              Dana Point PULSE &nbsp;|&nbsp; GloCon Solutions LLC
            </div>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</body>
</html>"""


def send_alert_email(alert_lines: list[str]) -> None:
    to_raw    = os.environ.get("ALERT_EMAIL_TO", "")
    from_addr = os.environ.get("DIGEST_EMAIL_FROM", "")
    smtp_host = os.environ.get("DIGEST_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("DIGEST_SMTP_PORT", "587"))
    smtp_user = os.environ.get("DIGEST_SMTP_USER", "")
    smtp_pass = os.environ.get("DIGEST_SMTP_PASS", "")

    if not all([to_raw, from_addr, smtp_user, smtp_pass]):
        print(f"[{_ts()}] ALERT WARNING: Missing SMTP env vars — skipping email delivery")
        return

    recipients = [r.strip() for r in to_raw.split(",") if r.strip()]
    html       = _build_alert_html(alert_lines)
    subject    = f"Dana Point PULSE Alert ({TODAY_S})"

    try:
        # SendGrid's SMTP relay is frequently filtered outbound by PaaS hosts,
        # which makes smtplib hang instead of failing cleanly. Their HTTP API
        # runs over standard HTTPS and is used directly when SendGrid is the
        # configured host. See send_weekly_digest.py for the same pattern.
        if "sendgrid" in smtp_host.lower():
            import requests

            payload = {
                "personalizations": [{"to": [{"email": r} for r in recipients]}],
                "from": {"email": from_addr},
                "subject": subject,
                "content": [{"type": "text/html", "value": html}],
            }
            resp = requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {smtp_pass}", "Content-Type": "application/json"},
                json=payload,
                timeout=20,
            )
            if resp.status_code not in (200, 202):
                raise RuntimeError(f"SendGrid API error {resp.status_code}: {resp.text[:300]}")
        else:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = from_addr
            msg["To"]      = ", ".join(recipients)
            msg.attach(MIMEText(html, "html"))
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                server.ehlo()
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.sendmail(from_addr, recipients, msg.as_string())
        print(f"[{_ts()}] ALERT: Email sent to {len(recipients)} recipient(s)")
    except Exception as exc:  # noqa: BLE001
        print(f"[{_ts()}] ALERT WARNING: Email send failed — {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"[{_ts()}] ALERT: Checking alert conditions")

    compression      = check_compression_alert()
    revpar_drop      = check_revpar_drop()
    priority_insights = check_priority_insights()

    alert_lines = _build_alert_lines(compression, revpar_drop, priority_insights)

    if not alert_lines:
        print(f"[{_ts()}] ALERT: No alert conditions met — nothing to send")
        return

    print(f"[{_ts()}] ALERT: {len(alert_lines)} condition(s) triggered")
    for line in alert_lines:
        print(f"  {line}")

    slack_url  = os.environ.get("SLACK_WEBHOOK_URL", "")
    alert_to   = os.environ.get("ALERT_EMAIL_TO", "")

    if not slack_url and not alert_to:
        print(f"[{_ts()}] ALERT: No delivery targets configured (no-op mode) — set SLACK_WEBHOOK_URL or ALERT_EMAIL_TO")
        return

    if slack_url:
        send_slack(alert_lines)

    if alert_to:
        send_alert_email(alert_lines)


if __name__ == "__main__":
    main()
