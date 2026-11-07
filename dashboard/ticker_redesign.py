# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
Redesigned KPI Ticker Component

Addresses:
1. Stale data visibility - separates current from historical
2. Readability - improved spacing and clear hierarchy
3. Forward-looking focus - highlights actionable insights

Uses data_quality checks to validate freshness before display.
"""

import pandas as pd
from datetime import datetime
from typing import Optional
from data_quality import (
    validate_ticker_data, check_str_data_freshness,
    generate_freshness_badge, DataQualityReport
)
import logging

_logger = logging.getLogger("ticker_redesign")


def render_ticker_section_current(df_kpi: pd.DataFrame, df_dfy: pd.DataFrame,
                                  df_compression: pd.DataFrame, quality_report: Optional[DataQualityReport] = None) -> str:
    """
    Render CURRENT DATA section of ticker.

    Shows only:
    - Today's STR metrics (occupancy, ADR, RevPAR)
    - Current performance deltas (YoY)
    - Forward-looking compression data

    Data not current is omitted from this section.
    """
    if quality_report is None:
        quality_report = validate_ticker_data(df_kpi, df_dfy, df_compression)

    items_html = []

    # STR metrics (most recent available)
    if quality_report.fresh_metrics.get("occupancy", False) and not df_kpi.empty:
        try:
            rec = df_kpi.sort_values("as_of_date").iloc[-1]
            occ_val = f"{float(rec.get('occ_pct', 0)):.1f}%"
            occ_yoy = float(rec.get("occ_yoy", 0))
            occ_delta = f"{'▲' if occ_yoy >= 0 else '▼'} {abs(occ_yoy):.1f}pp"
            date_label = str(rec.get('as_of_date', ''))[:10]

            items_html.append(f"""
            <div class="ticker-item-group">
                <div class="ticker-metric-block">
                    <div class="ticker-label">OCCUPANCY</div>
                    <div class="ticker-value">{occ_val}</div>
                    <div class="ticker-delta {('positive' if occ_yoy >= 0 else 'negative')}">{occ_delta} YoY</div>
                    <div class="ticker-date">{date_label}</div>
                </div>
            </div>
            """)
        except Exception as e:
            _logger.debug(f"Occupancy render error: {e}")

    # ADR
    if quality_report.fresh_metrics.get("adr", False) and not df_kpi.empty:
        try:
            rec = df_kpi.sort_values("as_of_date").iloc[-1]
            adr_val = f"${float(rec.get('adr', 0)):,.0f}"
            adr_yoy = float(rec.get("adr_yoy", 0))
            adr_delta = f"{'▲' if adr_yoy >= 0 else '▼'} {abs(adr_yoy):.1f}%"

            items_html.append(f"""
            <div class="ticker-item-group">
                <div class="ticker-metric-block">
                    <div class="ticker-label">ADR</div>
                    <div class="ticker-value">{adr_val}</div>
                    <div class="ticker-delta {('positive' if adr_yoy >= 0 else 'negative')}">{adr_delta} YoY</div>
                </div>
            </div>
            """)
        except Exception as e:
            _logger.debug(f"ADR render error: {e}")

    # RevPAR
    if quality_report.fresh_metrics.get("revpar", False) and not df_kpi.empty:
        try:
            rec = df_kpi.sort_values("as_of_date").iloc[-1]
            revpar_val = f"${float(rec.get('revpar', 0)):,.0f}"
            revpar_yoy = float(rec.get("revpar_yoy", 0))
            revpar_delta = f"{'▲' if revpar_yoy >= 0 else '▼'} {abs(revpar_yoy):.1f}%"

            items_html.append(f"""
            <div class="ticker-item-group">
                <div class="ticker-metric-block">
                    <div class="ticker-label">REVPAR</div>
                    <div class="ticker-value">{revpar_val}</div>
                    <div class="ticker-delta {('positive' if revpar_yoy >= 0 else 'negative')}">{revpar_delta} YoY</div>
                </div>
            </div>
            """)
        except Exception as e:
            _logger.debug(f"RevPAR render error: {e}")

    if not items_html:
        items_html.append('<div class="ticker-no-data">STR data not currently available</div>')

    return f"""
<div class="ticker-section current">
    <div class="ticker-section-header">
        🟢 CURRENT PERFORMANCE (STR Daily)
        <span class="ticker-freshness-badge">{quality_report.last_update}</span>
    </div>
    <div class="ticker-items-row">
        {"".join(items_html)}
    </div>
</div>
"""


def render_ticker_section_forward_looking(df_compression: pd.DataFrame,
                                          quality_report: Optional[DataQualityReport] = None) -> str:
    """
    Render FORWARD-LOOKING DATA section.

    Shows quarterly compression outlook and KPIs that help with planning.
    """
    if quality_report is None or df_compression is None or df_compression.empty:
        return ""

    items_html = []

    try:
        # Get current quarter compression
        today = datetime.now().date()
        current_q = f"{today.year}-Q{(today.month - 1) // 3 + 1}"

        # Try to find current quarter data
        current_quarter_rows = df_compression[df_compression["quarter"] == current_q]

        if not current_quarter_rows.empty:
            row = current_quarter_rows.iloc[0]
            days_80 = int(row.get("days_above_80_occ", 0) or 0)
            days_90 = int(row.get("days_above_90_occ", 0) or 0)

            status = "🔥 HIGH COMPRESSION" if days_80 > 25 else ("🎯 MODERATE" if days_80 > 10 else "📊 SHOULDER")

            items_html.append(f"""
            <div class="ticker-item-group">
                <div class="ticker-metric-block">
                    <div class="ticker-label">{current_q} COMPRESSION</div>
                    <div class="ticker-value">{days_80} days (80%+)</div>
                    <div class="ticker-delta">{days_90} days (90%+)</div>
                    <div class="ticker-status">{status}</div>
                </div>
            </div>
            """)
        else:
            items_html.append(f"""
            <div class="ticker-item-group">
                <div class="ticker-metric-block">
                    <div class="ticker-label">{current_q} COMPRESSION</div>
                    <div class="ticker-no-data">Forecast pending</div>
                </div>
            </div>
            """)

    except Exception as e:
        _logger.debug(f"Forward-looking render error: {e}")

    if not items_html:
        return ""

    return f"""
<div class="ticker-section forward">
    <div class="ticker-section-header">
        🚀 FORWARD-LOOKING (Next 90 Days)
    </div>
    <div class="ticker-items-row">
        {"".join(items_html)}
    </div>
</div>
"""


def render_ticker_section_reference(df_dfy: pd.DataFrame, df_later_ig: pd.DataFrame) -> str:
    """
    Render REFERENCE DATA section.

    Shows benchmarks, historical context, and non-operational metrics.
    Clearly labeled as reference/context data.
    """
    items_html = []

    # Datafy annual data (reference only, likely 2024)
    if df_dfy is not None and not df_dfy.empty:
        try:
            row = df_dfy.iloc[0]
            annual_trips = row.get("total_trips", 0)
            oos_pct = row.get("out_of_state_vd_pct", 0)

            if annual_trips:
                trips_val = f"{float(annual_trips) / 1e6:.2f}M"
                items_html.append(f"""
                <div class="ticker-item-group reference">
                    <div class="ticker-metric-block">
                        <div class="ticker-label">ANNUAL TRIPS</div>
                        <div class="ticker-value">{trips_val}</div>
                        <div class="ticker-note">Historical reference</div>
                    </div>
                </div>
                """)

            if oos_pct:
                items_html.append(f"""
                <div class="ticker-item-group reference">
                    <div class="ticker-metric-block">
                        <div class="ticker-label">OUT-OF-STATE %</div>
                        <div class="ticker-value">{float(oos_pct):.0f}%</div>
                        <div class="ticker-note">Historical baseline</div>
                    </div>
                </div>
                """)
        except Exception as e:
            _logger.debug(f"Datafy reference render error: {e}")

    # Social media followers (latest available)
    if df_later_ig is not None and not df_later_ig.empty:
        try:
            row = df_later_ig.sort_values("date").iloc[-1]
            followers = int(float(row.get("followers", 0)) or 0)
            date_val = str(row.get("date", ""))[:7]

            if followers:
                items_html.append(f"""
                <div class="ticker-item-group">
                    <div class="ticker-metric-block">
                        <div class="ticker-label">IG FOLLOWERS</div>
                        <div class="ticker-value">{followers:,}</div>
                        <div class="ticker-date">{date_val}</div>
                    </div>
                </div>
                """)
        except Exception as e:
            _logger.debug(f"Social reference render error: {e}")

    if not items_html:
        return ""

    return f"""
<div class="ticker-section reference">
    <div class="ticker-section-header">
        📚 REFERENCE DATA (Context & Benchmarks)
    </div>
    <div class="ticker-items-row">
        {"".join(items_html)}
    </div>
</div>
"""


def render_ticker_redesigned(df_kpi: pd.DataFrame, df_dfy: pd.DataFrame,
                             df_compression: pd.DataFrame, df_later_ig: pd.DataFrame,
                             quality_report: Optional[DataQualityReport] = None) -> str:
    """
    Render completely redesigned ticker with three sections:
    1. CURRENT - today's STR metrics
    2. FORWARD-LOOKING - quarterly compression & outlook
    3. REFERENCE - benchmarks & context

    Each section clearly separated with visual hierarchy.
    """
    if quality_report is None:
        quality_report = validate_ticker_data(df_kpi, df_dfy, df_compression)

    sections = []

    # Status header
    status_html = f"""
<div class="ticker-status-bar">
    <div class="ticker-status-left">
        <div class="ticker-status-icon">⚡</div>
        <div class="ticker-status-text">
            <strong>Dana Point PULSE</strong>
            <div class="ticker-status-note">{quality_report.summary_html()}</div>
        </div>
    </div>
    <div class="ticker-status-right">
        Last updated: <strong>{quality_report.last_update}</strong>
    </div>
</div>
"""
    sections.append(status_html)

    # Current data section
    current_section = render_ticker_section_current(df_kpi, df_dfy, df_compression, quality_report)
    if current_section:
        sections.append(current_section)

    # Forward-looking section
    forward_section = render_ticker_section_forward_looking(df_compression, quality_report)
    if forward_section:
        sections.append(forward_section)

    # Reference section
    reference_section = render_ticker_section_reference(df_dfy, df_later_ig)
    if reference_section:
        sections.append(reference_section)

    return f"""
<div class="ticker-redesigned-wrapper">
    {"".join(sections)}
</div>
"""


# CSS for redesigned ticker
TICKER_REDESIGNED_CSS = """
<style>
.ticker-redesigned-wrapper {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #FFFFFF;
    border-bottom: 1px solid #E5E7EB;
    padding: 16px 0;
    margin-bottom: 20px;
}

.ticker-status-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 20px;
    background: linear-gradient(135deg, #1F2937 0%, #111827 100%);
    color: #FFFFFF;
    border-radius: 8px;
    margin: 0 20px 16px 20px;
    font-size: 13px;
}

.ticker-status-left {
    display: flex;
    gap: 12px;
    align-items: center;
}

.ticker-status-icon {
    font-size: 24px;
}

.ticker-status-text {
    line-height: 1.4;
}

.ticker-status-text strong {
    font-weight: 700;
    display: block;
}

.ticker-status-note {
    font-size: 11px;
    opacity: 0.8;
    margin-top: 2px;
}

.ticker-status-right {
    font-size: 12px;
    opacity: 0.9;
}

.ticker-section {
    margin: 0 20px 20px 20px;
    padding: 16px;
    background: #F9FAFB;
    border-radius: 8px;
    border-left: 4px solid #0891B2;
}

.ticker-section.forward {
    border-left-color: #06B6D4;
}

.ticker-section.reference {
    border-left-color: #8B5CF6;
    background: #F5F3FF;
}

.ticker-section-header {
    font-size: 12px;
    font-weight: 700;
    color: #1F2937;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 12px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.ticker-freshness-badge {
    font-size: 11px;
    font-weight: 600;
    color: #6B7280;
    text-transform: none;
    letter-spacing: normal;
    font-family: 'SF Mono', Monaco, monospace;
}

.ticker-items-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 12px;
}

.ticker-item-group {
    display: flex;
}

.ticker-metric-block {
    flex: 1;
    padding: 12px;
    background: #FFFFFF;
    border-radius: 6px;
    border: 1px solid #E5E7EB;
}

.ticker-metric-block.reference {
    background: #FFFBEB;
    border-color: #FEE3B1;
}

.ticker-label {
    font-size: 10px;
    font-weight: 700;
    color: #6B7280;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 4px;
}

.ticker-value {
    font-size: 18px;
    font-weight: 800;
    color: #1F2937;
    line-height: 1.2;
    margin-bottom: 2px;
}

.ticker-delta {
    font-size: 12px;
    font-weight: 600;
    color: #6B7280;
    margin-bottom: 4px;
}

.ticker-delta.positive {
    color: #059669;
}

.ticker-delta.negative {
    color: #DC2626;
}

.ticker-date {
    font-size: 10px;
    color: #9CA3AF;
    font-family: 'SF Mono', Monaco, monospace;
    margin-top: 2px;
}

.ticker-status {
    font-size: 10px;
    font-weight: 700;
    color: #DC2626;
    text-transform: uppercase;
    margin-top: 4px;
    letter-spacing: 0.05em;
}

.ticker-note {
    font-size: 10px;
    color: #9CA3AF;
    margin-top: 4px;
    font-style: italic;
}

.ticker-no-data {
    font-size: 12px;
    color: #9CA3AF;
    padding: 12px;
    text-align: center;
    background: #F3F4F6;
    border-radius: 4px;
    grid-column: 1 / -1;
}

@media (max-width: 768px) {
    .ticker-status-bar {
        flex-direction: column;
        gap: 12px;
        align-items: flex-start;
    }

    .ticker-status-right {
        width: 100%;
    }

    .ticker-items-row {
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    }
}
</style>
"""
