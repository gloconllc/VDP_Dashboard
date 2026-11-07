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
Shared pytest fixtures for VDP Analytics tests.
All fixtures use in-memory SQLite so no real database is touched.
"""

import sys
from pathlib import Path

# Ensure tests/ is on sys.path so test files can import helpers.py
sys.path.insert(0, str(Path(__file__).parent))

from datetime import date, timedelta

import pandas as pd
import pytest

from helpers import make_mem_conn, insert_daily_str


@pytest.fixture
def mem_conn():
    """Fresh in-memory SQLite with fact_str_metrics + KPI tables."""
    conn = make_mem_conn()
    yield conn
    conn.close()


@pytest.fixture
def str_daily_rows():
    """Factory: returns a list of fact_str_metrics rows for one date and three metrics."""
    def _make(as_of_date: str, occ: float, adr: float, revpar: float):
        base = ("STR", "daily", "VDP Select Portfolio", "Anaheim Area", as_of_date)
        return [
            base + ("occ",    occ,    "decimal"),
            base + ("adr",    adr,    "USD"),
            base + ("revpar", revpar, "USD"),
        ]
    return _make


@pytest.fixture
def sample_str_df() -> pd.DataFrame:
    """Wide-format STR daily DataFrame covering 60 days."""
    start = date(2025, 1, 1)
    dates = [start + timedelta(days=i) for i in range(60)]
    return pd.DataFrame({
        "as_of_date": pd.to_datetime(dates),
        "occupancy":  [70.0 + (i % 20) for i in range(60)],
        "adr":        [180.0 + i * 0.5 for i in range(60)],
        "revpar":     [126.0 + i * 0.4 for i in range(60)],
        "revenue":    [50000.0 + i * 100 for i in range(60)],
        "demand":     [300 + i for i in range(60)],
        "supply":     [400] * 60,
    })
