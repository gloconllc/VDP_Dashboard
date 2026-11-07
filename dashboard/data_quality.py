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
Data Quality Checker for VDP Dashboard

Ensures ticker and dashboard data is current, forward-looking, and actionable.
Validates freshness thresholds and flags stale data appropriately.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import logging

_logger = logging.getLogger("vdp_quality")

# Freshness thresholds (days)
FRESHNESS_THRESHOLDS = {
    "str_daily": 5,           # STR data should be within 5 days
    "str_monthly": 35,        # Monthly within ~month
    "datafy_annual": 365,     # Annual is OK if within 1 year
    "social": 7,              # Social should be within 1 week
    "compression": 90,        # Compression projections OK for quarter
}

# Date labels to avoid (stale markers)
STALE_MARKERS = {"2024 Annual", "2023", "2024", "2025", "Jun 2025 snapshot", "Zartico"}


class DataQualityReport:
    """Container for data quality assessment."""

    def __init__(self):
        self.issues: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.fresh_metrics: Dict[str, bool] = {}
        self.last_update: str = ""

    def add_issue(self, metric_name: str, issue_type: str, message: str, data_age_days: int = 0):
        """Flag a data quality issue."""
        self.issues.append({
            "metric": metric_name,
            "type": issue_type,  # "stale", "missing", "inconsistent"
            "message": message,
            "data_age_days": data_age_days,
        })

    def add_warning(self, metric_name: str, message: str):
        """Flag a minor warning."""
        self.warnings.append({
            "metric": metric_name,
            "message": message,
        })

    def mark_fresh(self, metric_name: str, is_fresh: bool):
        """Mark a metric as fresh or stale."""
        self.fresh_metrics[metric_name] = is_fresh

    def is_healthy(self) -> bool:
        """Return True if no critical issues."""
        return len(self.issues) == 0

    def get_stale_count(self) -> int:
        """Count stale metrics."""
        return sum(1 for i in self.issues if i["type"] == "stale")

    def summary_html(self) -> str:
        """Generate HTML status badge."""
        if self.is_healthy():
            return '🟢 <span style="color:#10b981">All data fresh</span>'
        elif self.get_stale_count() <= 2:
            return f'🟡 <span style="color:#f59e0b">{len(self.warnings)} warnings</span>'
        else:
            return f'🔴 <span style="color:#ef4444">{self.get_stale_count()} stale metrics</span>'


def check_str_data_freshness(df_kpi: pd.DataFrame) -> Tuple[bool, str, int]:
    """
    Check if STR KPI data is fresh (within threshold).

    Returns: (is_fresh, last_date_str, days_old)
    """
    if df_kpi is None or df_kpi.empty:
        return False, "No data", 999

    try:
        last_date = pd.to_datetime(df_kpi["as_of_date"]).max()
        today = datetime.now().date()
        days_old = (today - last_date.date()).days
        is_fresh = days_old <= FRESHNESS_THRESHOLDS["str_daily"]
        date_str = last_date.strftime("%Y-%m-%d")
        return is_fresh, date_str, days_old
    except Exception as e:
        _logger.error(f"STR freshness check failed: {e}")
        return False, "Error reading data", 999


def check_datafy_data_freshness(df_datafy: pd.DataFrame, report_period: str = "2025-annual") -> Tuple[bool, str]:
    """
    Check if Datafy visitor economy data is recent.

    Datafy data is typically annual, so we accept data within 1 year
    but flag if it's labeled as 2024 or earlier.

    Returns: (is_forward_looking, freshness_note)
    """
    if df_datafy is None or df_datafy.empty:
        return False, "No Datafy data available"

    # Check if it's historical/stale marker
    if "2024" in report_period or "2023" in report_period:
        return False, f"Datafy data is from {report_period} (use for trend context only)"

    if "2025" in report_period:
        return True, f"Datafy {report_period} (current annual)"

    return True, f"Datafy {report_period}"


def check_compression_data_freshness(df_compression: pd.DataFrame) -> Tuple[bool, str, int]:
    """
    Check compression quarter data.

    Compression is forward-looking, so we only flag if it's from a
    historical quarter that has already passed.

    Returns: (is_forward_looking, quarter_str, current_days)
    """
    if df_compression is None or df_compression.empty:
        return False, "No compression data", 0

    try:
        today = datetime.now().date()
        current_quarter = f"{today.year}-Q{(today.month - 1) // 3 + 1}"

        # Get most recent quarter from data
        latest_quarter = str(df_compression["quarter"].max())

        # Parse quarter string (e.g., "2026-Q3")
        if latest_quarter.startswith(str(today.year)):
            # Current year compression data is forward-looking
            is_forward = True
            days_in_q = int(df_compression[df_compression["quarter"] == latest_quarter]["days_above_80_occ"].iloc[0] or 0)
            return is_forward, latest_quarter, days_in_q
        else:
            # Historical quarter
            return False, latest_quarter, 0
    except Exception as e:
        _logger.error(f"Compression freshness check failed: {e}")
        return False, "Error reading compression", 0


def validate_ticker_data(df_kpi: pd.DataFrame, df_datafy: pd.DataFrame,
                        df_compression: pd.DataFrame) -> DataQualityReport:
    """
    Comprehensive ticker data quality audit.

    Returns a report identifying which ticker items are:
    - Fresh & current (show prominently)
    - Stale or historical (move to deep-dive section)
    - Missing or errored (hide or show as "not available")
    """
    report = DataQualityReport()

    # Check STR data (core operational metrics)
    str_fresh, str_date, str_age = check_str_data_freshness(df_kpi)
    report.mark_fresh("occupancy", str_fresh)
    report.mark_fresh("adr", str_fresh)
    report.mark_fresh("revpar", str_fresh)
    report.last_update = str_date

    if not str_fresh:
        msg = f"STR data is {str_age} days old (last: {str_date}). Show with caution."
        report.add_issue("str_metrics", "stale", msg, str_age)

    # Check Datafy data (visitor economy)
    datafy_fresh, datafy_note = check_datafy_data_freshness(df_datafy)
    report.mark_fresh("visitor_trips", datafy_fresh)
    report.mark_fresh("oos_pct", datafy_fresh)
    report.mark_fresh("spend_multiplier", datafy_fresh)

    if not datafy_fresh:
        report.add_warning("datafy_metrics", datafy_note)

    # Check compression data (forward-looking)
    comp_forward, comp_quarter, comp_days = check_compression_data_freshness(df_compression)
    report.mark_fresh("compression", comp_forward)

    if not comp_forward and comp_days == 0:
        report.add_warning("compression", f"Compression data from {comp_quarter} is historical (already passed)")

    return report


def filter_ticker_items(current_items: List[Dict], stale_items: List[Dict],
                       report: DataQualityReport) -> Tuple[List[Dict], List[Dict]]:
    """
    Separate ticker items into current (show) and stale (hide/deep-dive).

    Returns: (current_for_display, stale_for_reference)
    """
    display_items = []
    reference_items = []

    for item in current_items:
        metric_key = item.get("key", "")

        # If fresh according to report, display it
        if report.fresh_metrics.get(metric_key, False):
            display_items.append(item)
        else:
            reference_items.append(item)

    # Stale items go to reference section
    reference_items.extend(stale_items)

    return display_items, reference_items


def generate_freshness_badge(is_fresh: bool, days_old: int = 0, date_str: str = "") -> str:
    """Generate HTML badge showing data freshness."""
    if is_fresh:
        return f'<span style="color:#10b981;font-weight:600;font-size:10px;">✓ Current</span>'
    else:
        return f'<span style="color:#f59e0b;font-weight:600;font-size:10px;">⚠ {days_old}d old</span>'


def validate_insight_freshness(insight_row: pd.Series) -> Dict[str, Any]:
    """
    Check if an insight is current or stale.

    Returns metadata about the insight's recency and relevance.
    """
    if insight_row is None:
        return {"is_current": False, "reason": "No insight data"}

    try:
        as_of_date = pd.to_datetime(insight_row.get("as_of_date", datetime.now()))
        days_old = (datetime.now().date() - as_of_date.date()).days

        # Forward-looking insights (horizon_days) are current
        horizon = int(insight_row.get("horizon_days", 30))
        is_current = days_old <= 1 and horizon > 0  # Generated today and forward-looking

        return {
            "is_current": is_current,
            "days_old": days_old,
            "horizon_days": horizon,
            "generated": as_of_date.strftime("%Y-%m-%d"),
            "reason": f"Generated {days_old}d ago, forward outlook {horizon}d",
        }
    except Exception as e:
        _logger.error(f"Insight freshness check failed: {e}")
        return {"is_current": False, "reason": "Error processing insight"}
