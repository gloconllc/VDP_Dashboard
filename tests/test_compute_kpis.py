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
Tests for scripts/compute_kpis.py

Covers:
  - YoY percent-change math (occ_yoy, adr_yoy, revpar_yoy)
  - NULL YoY when no prior-year data exists
  - Compression flags (is_occ_80, is_occ_90)
  - Quarterly aggregation (days_above_80_occ, days_above_90_occ)
  - Division-by-zero guard (zero prior-year metric → NULL yoy)
"""

import sys
from pathlib import Path

import pytest

# Allow direct import from scripts/
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from compute_kpis import (
    build_kpi_daily_summary,
    build_kpi_compression_quarterly,
)
from helpers import insert_daily_str as _insert_daily_str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _kpi_row(conn, as_of_date: str) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT as_of_date, occ_pct, adr, revpar, "
        "occ_yoy, adr_yoy, revpar_yoy, is_occ_80, is_occ_90 "
        "FROM kpi_daily_summary WHERE as_of_date = ?",
        (as_of_date,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = ["as_of_date", "occ_pct", "adr", "revpar", "occ_yoy", "adr_yoy", "revpar_yoy", "is_occ_80", "is_occ_90"]
    return dict(zip(cols, row))


def _compression_row(conn, quarter: str) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        "SELECT quarter, days_above_80_occ, days_above_90_occ "
        "FROM kpi_compression_quarterly WHERE quarter = ?",
        (quarter,),
    )
    row = cur.fetchone()
    return dict(zip(["quarter", "days_above_80_occ", "days_above_90_occ"], row)) if row else None


# ---------------------------------------------------------------------------
# Tests: kpi_daily_summary
# ---------------------------------------------------------------------------

class TestKpiDailySummary:

    def test_occ_decimal_converted_to_percent(self, mem_conn, str_daily_rows):
        """fact_str_metrics stores occ as decimal (0.688); kpi should store 68.8."""
        _insert_daily_str(mem_conn, str_daily_rows("2025-06-01", occ=0.688, adr=200.0, revpar=137.6))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        row = _kpi_row(mem_conn, "2025-06-01")
        assert row is not None
        assert abs(row["occ_pct"] - 68.8) < 0.01

    def test_yoy_null_when_no_prior_year(self, mem_conn, str_daily_rows):
        """Dates without a prior-year match produce NULL yoy columns."""
        _insert_daily_str(mem_conn, str_daily_rows("2025-03-15", occ=0.75, adr=210.0, revpar=157.5))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        row = _kpi_row(mem_conn, "2025-03-15")
        assert row["occ_yoy"] is None
        assert row["adr_yoy"] is None
        assert row["revpar_yoy"] is None

    def test_yoy_correct_percent_change(self, mem_conn, str_daily_rows):
        """YoY = (current - prior) / prior * 100, rounded to 2 decimal places."""
        _insert_daily_str(mem_conn, str_daily_rows("2024-06-01", occ=0.70, adr=190.0, revpar=133.0))
        _insert_daily_str(mem_conn, str_daily_rows("2025-06-01", occ=0.77, adr=209.0, revpar=160.93))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        row = _kpi_row(mem_conn, "2025-06-01")
        assert row is not None
        # occ: (77 - 70) / 70 * 100 = 10.0
        assert abs(row["occ_yoy"] - 10.0) < 0.05
        # adr: (209 - 190) / 190 * 100 ≈ 10.0
        assert abs(row["adr_yoy"] - 10.0) < 0.05
        # revpar: (160.93 - 133) / 133 * 100 ≈ 21.0
        assert abs(row["revpar_yoy"] - 21.0) < 0.5

    def test_yoy_zero_prior_produces_null(self, mem_conn, str_daily_rows):
        """If prior-year ADR = 0, adr_yoy must be NULL (not divide by zero)."""
        _insert_daily_str(mem_conn, str_daily_rows("2024-07-04", occ=0.0, adr=0.0, revpar=0.0))
        _insert_daily_str(mem_conn, str_daily_rows("2025-07-04", occ=0.85, adr=250.0, revpar=212.5))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        row = _kpi_row(mem_conn, "2025-07-04")
        assert row["adr_yoy"] is None
        assert row["revpar_yoy"] is None

    def test_is_occ_80_flag(self, mem_conn, str_daily_rows):
        """is_occ_80 = 1 when occ_pct >= 80, else 0."""
        _insert_daily_str(mem_conn, str_daily_rows("2025-08-01", occ=0.80, adr=220.0, revpar=176.0))
        _insert_daily_str(mem_conn, str_daily_rows("2025-08-02", occ=0.79, adr=215.0, revpar=169.85))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        assert _kpi_row(mem_conn, "2025-08-01")["is_occ_80"] == 1
        assert _kpi_row(mem_conn, "2025-08-02")["is_occ_80"] == 0

    def test_is_occ_90_flag(self, mem_conn, str_daily_rows):
        """is_occ_90 = 1 when occ_pct >= 90, else 0."""
        _insert_daily_str(mem_conn, str_daily_rows("2025-07-04", occ=0.92, adr=300.0, revpar=276.0))
        _insert_daily_str(mem_conn, str_daily_rows("2025-07-05", occ=0.89, adr=280.0, revpar=249.2))
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        assert _kpi_row(mem_conn, "2025-07-04")["is_occ_90"] == 1
        assert _kpi_row(mem_conn, "2025-07-05")["is_occ_90"] == 0

    def test_rebuild_is_idempotent(self, mem_conn, str_daily_rows):
        """Running build_kpi_daily_summary twice produces the same result (no duplicate rows)."""
        _insert_daily_str(mem_conn, str_daily_rows("2025-05-01", occ=0.72, adr=195.0, revpar=140.4))
        cur = mem_conn.cursor()
        build_kpi_daily_summary(cur)
        mem_conn.commit()
        build_kpi_daily_summary(cur)
        mem_conn.commit()
        count = cur.execute("SELECT COUNT(*) FROM kpi_daily_summary").fetchone()[0]
        assert count == 1  # DELETE + INSERT — always exactly one row per date

    def test_empty_fact_table_produces_no_rows(self, mem_conn):
        """Empty source table → kpi_daily_summary has 0 rows (no crash)."""
        build_kpi_daily_summary(mem_conn.cursor())
        mem_conn.commit()
        count = mem_conn.execute("SELECT COUNT(*) FROM kpi_daily_summary").fetchone()[0]
        assert count == 0


# ---------------------------------------------------------------------------
# Tests: kpi_compression_quarterly
# ---------------------------------------------------------------------------

class TestKpiCompressionQuarterly:

    def _populate_quarter(self, conn, str_daily_rows, year: int, month: int, occ_values: list[float]):
        """Insert N days of data at the given occ levels and build KPI tables."""
        from datetime import date
        rows = []
        for i, occ in enumerate(occ_values):
            d = date(year, month, i + 1).isoformat()
            rows.extend(str_daily_rows(d, occ=occ, adr=200.0, revpar=200.0 * occ))
        _insert_daily_str(conn, rows)
        cur = conn.cursor()
        build_kpi_daily_summary(cur)
        build_kpi_compression_quarterly(cur)
        conn.commit()

    def test_compression_counts_correct(self, mem_conn, str_daily_rows):
        """3 days >=80%, 1 day >=90% in a quarter."""
        occs = [0.91, 0.83, 0.81, 0.65, 0.50]  # 3 above 80%, 1 above 90%
        self._populate_quarter(mem_conn, str_daily_rows, 2025, 7, occs)
        row = _compression_row(mem_conn, "2025-Q3")
        assert row is not None
        assert row["days_above_80_occ"] == 3
        assert row["days_above_90_occ"] == 1

    def test_no_compression_days(self, mem_conn, str_daily_rows):
        """All days below 80% → both compression counts = 0."""
        occs = [0.65, 0.70, 0.72]
        self._populate_quarter(mem_conn, str_daily_rows, 2025, 1, occs)
        row = _compression_row(mem_conn, "2025-Q1")
        assert row["days_above_80_occ"] == 0
        assert row["days_above_90_occ"] == 0

    def test_quarter_label_format(self, mem_conn, str_daily_rows):
        """Quarter labels follow YYYY-Qn format for all four quarters."""
        quarter_months = [(1, "2025-Q1"), (4, "2025-Q2"), (7, "2025-Q3"), (10, "2025-Q4")]
        for month, expected_q in quarter_months:
            _insert_daily_str(mem_conn, str_daily_rows(f"2025-{month:02d}-01", 0.70, 200.0, 140.0))
        cur = mem_conn.cursor()
        build_kpi_daily_summary(cur)
        build_kpi_compression_quarterly(cur)
        mem_conn.commit()
        for _, expected_q in quarter_months:
            assert _compression_row(mem_conn, expected_q) is not None
