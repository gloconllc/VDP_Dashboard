"""
send_weekly_digest.py
---------------------
Reads insights_daily and kpi_daily_summary from analytics.sqlite and sends
a formatted HTML weekly email digest.

Env vars (all required for live send; missing any triggers dry-run mode):
    DIGEST_EMAIL_FROM   -- sender address (must be a verified sender if using SendGrid/Mailgun)
    DIGEST_EMAIL_TO     -- recipient(s), comma-separated
    DIGEST_SMTP_HOST    -- SMTP host (default: smtp.gmail.com)
    DIGEST_SMTP_PORT    -- SMTP port (default: 587)
    DIGEST_SMTP_USER    -- SMTP username
    DIGEST_SMTP_PASS    -- SMTP password
    DIGEST_REPORT_URL   -- link the "View Full Report" button points to
                           (default: https://vdppulse.gloconsolutions.com/)

Any standard SMTP relay works, including transactional providers via their
SMTP interface, e.g. SendGrid: DIGEST_SMTP_HOST=smtp.sendgrid.net,
DIGEST_SMTP_PORT=587, DIGEST_SMTP_USER=apikey, DIGEST_SMTP_PASS=<API key>.

Dry-run: writes HTML to logs/weekly_digest_YYYY-MM-DD.html

Run:
    python3 scripts/send_weekly_digest.py
"""

from __future__ import annotations

import os
import smtplib
import sqlite3
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH      = os.path.join(PROJECT_ROOT, "data", "analytics.sqlite")
LOGS_DIR     = os.path.join(PROJECT_ROOT, "logs")

TODAY = date.today().isoformat()
REPORT_URL = os.environ.get("DIGEST_REPORT_URL", "https://vdppulse.gloconsolutions.com/")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)


def load_latest_kpis() -> dict:
    """Return the most recent row from kpi_daily_summary as a dict."""
    try:
        with _connect() as con:
            df = pd.read_sql_query(
                """
                SELECT occ_pct, adr, revpar, occ_yoy, adr_yoy, revpar_yoy, as_of_date
                FROM kpi_daily_summary
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                con,
            )
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    except Exception as exc:  # noqa: BLE001
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] DIGEST WARNING: kpi load failed — {exc}")
        return {}


def load_insights() -> pd.DataFrame:
    """Return the most recent insight per audience/category."""
    try:
        with _connect() as con:
            df = pd.read_sql_query(
                """
                SELECT audience, category, headline, body, priority, as_of_date
                FROM insights_daily
                WHERE as_of_date = (SELECT MAX(as_of_date) FROM insights_daily)
                ORDER BY audience, priority ASC
                """,
                con,
            )
        return df
    except Exception as exc:  # noqa: BLE001
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] DIGEST WARNING: insights load failed — {exc}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

AUDIENCE_LABELS = {
    "dmo":      "DMO / TBID Board",
    "city":     "City of Dana Point",
    "visitor":  "Visitor Intelligence",
    "resident": "Resident Impact",
    "cross":    "Market Intelligence",
}

AUDIENCE_ORDER = ["dmo", "city", "visitor", "resident", "cross"]


def _first_sentence(text: str, max_chars: int = 280) -> str:
    """Return first sentence of text, capped at max_chars."""
    if not text:
        return ""
    for i in range(60, len(text)):
        if text[i] in ".!?" and i + 1 < len(text) and text[i + 1] == " ":
            return text[: i + 1]
    return text[:max_chars]


def _fmt_pct(val, decimals: int = 1) -> str:
    try:
        return f"{float(val):{'+' if decimals else ''},.{decimals}f}%"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_usd(val) -> str:
    try:
        return f"${float(val):,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def _hero_box(label: str, value: str, sub: str = "", color: str = "#1a3a5c") -> str:
    return f"""
<td style="width:25%;padding:8px;">
  <div style="background:#ffffff;border-top:4px solid {color};border-radius:6px;
              padding:16px 12px;text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.08);">
    <div style="font-size:22px;font-weight:700;color:{color};">{value}</div>
    <div style="font-size:11px;color:#555;margin-top:4px;text-transform:uppercase;
                letter-spacing:.5px;">{label}</div>
    {f'<div style="font-size:12px;color:#888;margin-top:2px;">{sub}</div>' if sub else ''}
  </div>
</td>"""


def _insight_card(headline: str, body: str) -> str:
    first = _first_sentence(body)
    return f"""
<div style="background:#f0f4f8;border-left:4px solid #1a3a5c;border-radius:4px;
            padding:12px 14px;margin-bottom:10px;">
  <div style="font-size:14px;font-weight:600;color:#1a3a5c;margin-bottom:4px;">{headline}</div>
  <div style="font-size:13px;color:#444;line-height:1.5;">{first}</div>
</div>"""


def _section(label: str, rows: pd.DataFrame) -> str:
    if rows.empty:
        return ""
    cards = ""
    for _, row in rows.head(2).iterrows():
        cards += _insight_card(row.get("headline", ""), row.get("body", ""))
    return f"""
<tr>
  <td style="padding:20px 32px 4px;">
    <h3 style="margin:0 0 10px;font-size:15px;color:#1a3a5c;border-bottom:1px solid #dde3ea;
               padding-bottom:6px;">{label}</h3>
    {cards}
  </td>
</tr>"""


def build_html(kpis: dict, insights_df: pd.DataFrame) -> str:
    """Assemble the full HTML email string."""

    occ_val    = _fmt_pct(kpis.get("occ_pct"), 1).replace("+", "")
    adr_val    = _fmt_usd(kpis.get("adr"))
    revpar_val = _fmt_usd(kpis.get("revpar"))

    try:
        revpar_yoy = float(kpis.get("revpar_yoy") or 0)
        revpar_sub = f"{'▲' if revpar_yoy >= 0 else '▼'} {abs(revpar_yoy):.1f}% YOY"
    except (TypeError, ValueError):
        revpar_sub = ""

    try:
        adr_yoy = float(kpis.get("adr_yoy") or 0)
        adr_sub = f"{'▲' if adr_yoy >= 0 else '▼'} {abs(adr_yoy):.1f}% YOY"
    except (TypeError, ValueError):
        adr_sub = ""

    try:
        occ_yoy = float(kpis.get("occ_yoy") or 0)
        occ_sub = f"{'▲' if occ_yoy >= 0 else '▼'} {abs(occ_yoy):.1f}% YOY"
    except (TypeError, ValueError):
        occ_sub = ""

    kpi_as_of = kpis.get("as_of_date", "")
    kpi_note  = f"KPIs as of {kpi_as_of}" if kpi_as_of else ""

    hero_row = (
        _hero_box("Occupancy", occ_val, occ_sub)
        + _hero_box("ADR", adr_val, adr_sub, "#2e7d32")
        + _hero_box("RevPAR", revpar_val, revpar_sub, "#b71c1c")
        + _hero_box("RevPAR YOY", revpar_sub or "N/A", "", "#5c3317")
    )

    insight_sections = ""
    for aud in AUDIENCE_ORDER:
        label = AUDIENCE_LABELS.get(aud, aud.upper())
        rows  = insights_df[insights_df["audience"] == aud] if not insights_df.empty else pd.DataFrame()
        insight_sections += _section(label, rows)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Dana Point PULSE, Intelligence Brief</title>
</head>
<body style="margin:0;padding:0;background:#f8f9fa;font-family:Arial,Helvetica,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0"
       style="background:#f8f9fa;padding:24px 0;">
  <tr>
    <td align="center">
      <table role="presentation" width="600" cellpadding="0" cellspacing="0"
             style="max-width:600px;width:100%;background:#ffffff;border-radius:8px;
                    overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">

        <!-- HEADER -->
        <tr>
          <td style="background:#1a3a5c;padding:28px 32px;">
            <div style="font-size:20px;font-weight:700;color:#ffffff;letter-spacing:.3px;">
              Dana Point PULSE
            </div>
            <div style="font-size:13px;color:#aec6e8;margin-top:4px;">
              Intelligence Brief, {TODAY}
            </div>
          </td>
        </tr>

        <!-- HERO METRICS -->
        <tr>
          <td style="padding:20px 32px 8px;">
            <div style="font-size:12px;color:#888;margin-bottom:10px;text-transform:uppercase;
                        letter-spacing:.5px;">Performance Snapshot{' | ' + kpi_note if kpi_note else ''}</div>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
              <tr>{hero_row}</tr>
            </table>
          </td>
        </tr>

        <!-- CTA -->
        <tr>
          <td style="padding:4px 32px 20px;text-align:center;">
            <a href="{REPORT_URL}" target="_blank"
               style="display:inline-block;background:#1D6E86;color:#ffffff;text-decoration:none;
                      font-size:14px;font-weight:600;padding:12px 28px;border-radius:6px;">
              View Full Report &rarr;
            </a>
          </td>
        </tr>

        <!-- INSIGHT SECTIONS -->
        {insight_sections}

        <!-- SPACER -->
        <tr><td style="height:16px;"></td></tr>

        <!-- FOOTER -->
        <tr>
          <td style="background:#1a3a5c;padding:16px 32px;text-align:center;">
            <div style="font-size:11px;color:#aec6e8;line-height:1.6;">
              Generated by Dana Point PULSE &nbsp;|&nbsp; GloCon Solutions LLC<br>
              This report contains market-sensitive data. For authorized recipients only.
            </div>
          </td>
        </tr>

      </table>
    </td>
  </tr>
</table>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Send / dry-run
# ---------------------------------------------------------------------------

REQUIRED_ENV = ["DIGEST_EMAIL_FROM", "DIGEST_EMAIL_TO", "DIGEST_SMTP_USER", "DIGEST_SMTP_PASS"]


def _missing_env() -> list[str]:
    return [k for k in REQUIRED_ENV if not os.environ.get(k)]


def send_email(html: str) -> int:
    """Send HTML email. Returns recipient count."""
    from_addr  = os.environ["DIGEST_EMAIL_FROM"]
    to_raw     = os.environ["DIGEST_EMAIL_TO"]
    smtp_host  = os.environ.get("DIGEST_SMTP_HOST", "smtp.gmail.com")
    smtp_port  = int(os.environ.get("DIGEST_SMTP_PORT", "587"))
    smtp_user  = os.environ["DIGEST_SMTP_USER"]
    smtp_pass  = os.environ["DIGEST_SMTP_PASS"]

    recipients = [r.strip() for r in to_raw.split(",") if r.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Dana Point PULSE, Intelligence Brief ({TODAY})"
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_addr, recipients, msg.as_string())

    return len(recipients)


def dry_run_save(html: str) -> str:
    """Write HTML to logs/. Returns path."""
    Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)
    path = os.path.join(LOGS_DIR, f"weekly_digest_{TODAY}.html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ts = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"[{ts()}] DIGEST: Loading data from {DB_PATH}")
    kpis       = load_latest_kpis()
    insights   = load_insights()

    print(f"[{ts()}] DIGEST: Building HTML ({len(insights)} insight rows)")
    html = build_html(kpis, insights)

    missing = _missing_env()
    if missing:
        print(f"[{ts()}] DIGEST WARNING: Missing env vars {missing} — running in dry-run mode")
        path = dry_run_save(html)
        print(f"[{ts()}] DIGEST: Dry-run saved to {path}")
        return

    try:
        n = send_email(html)
        print(f"[{ts()}] DIGEST: Sent to {n} recipient(s)")
    except Exception as exc:  # noqa: BLE001
        print(f"[{ts()}] DIGEST ERROR: Send failed — {exc}")
        path = dry_run_save(html)
        print(f"[{ts()}] DIGEST: Fallback dry-run saved to {path}")


if __name__ == "__main__":
    main()
