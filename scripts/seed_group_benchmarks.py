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
seed_group_benchmarks.py
────────────────────────
Creates and seeds the `group_intelligence` table with:
  - Industry benchmarks for group-travel demand share
  - Dana Point chain-scale-derived group capacity estimates
  - TBID/TOT revenue impact projections for group vs. leisure mix shifts
  - Scaffold for future STR group-segment data ingestion

Sources used:
  - CoStar chain-scale and market snapshot data already in analytics.sqlite
  - STR market-level ADR/RevPAR from kpi_daily_summary
  - Industry benchmarks: STR/CBRE Group Business Index (upper-upscale coastal resort markets,
    2024 averages): ~28-32% group demand share; group negotiated rates average 15-20% below
    blended market ADR; SMERF = Social/Military/Educational/Religious/Fraternal (primary shoulder)

Run as pipeline step (non-fatal). Adds one row per benchmark_date (today).
Re-running is safe — UPSERT keyed on benchmark_date.
"""

import sqlite3
import os
from datetime import date, datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "analytics.sqlite")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS group_intelligence (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    benchmark_date              TEXT NOT NULL UNIQUE,

    -- Market capacity context (from CoStar chain_scale_breakdown)
    total_market_rooms          INTEGER,
    group_primary_rooms         INTEGER,   -- Upper Upscale + Upscale (highest group amenity supply)
    group_primary_pct           REAL,      -- group_primary_rooms / total_market_rooms

    -- Industry benchmark ranges for resort coastal markets (STR/CBRE 2024)
    benchmark_group_demand_share_low   REAL,  -- 0.25 (25% of demand from group bookings)
    benchmark_group_demand_share_high  REAL,  -- 0.32 (32%)
    benchmark_group_adr_discount_pct   REAL,  -- -0.18 (group ADR typically 18% below blended ADR)

    -- Dana Point estimates derived from CoStar + STR data
    market_blended_adr          REAL,  -- CoStar South OC market ADR (most recent annual)
    market_blended_occ_pct      REAL,  -- CoStar blended occupancy %
    estimated_group_adr         REAL,  -- market_blended_adr × (1 - benchmark_group_adr_discount_pct)
    estimated_annual_room_rev   REAL,  -- CoStar annual room revenue ($)

    -- TBID/TOT impact estimates
    estimated_group_room_rev_low     REAL,   -- annual_room_rev × group_demand_share_low
    estimated_group_room_rev_high    REAL,   -- annual_room_rev × group_demand_share_high
    estimated_group_tbid_rev_low     REAL,   -- group_room_rev_low × 0.0125
    estimated_group_tbid_rev_high    REAL,   -- group_room_rev_high × 0.0125
    estimated_group_tot_rev_low      REAL,   -- group_room_rev_low × 0.10
    estimated_group_tot_rev_high     REAL,   -- group_room_rev_high × 0.10

    -- Opportunity: incremental TBID if group mix shifts +5pp
    tbid_uplift_per_5pp_shift        REAL,   -- incremental TBID from +5pp group mix growth

    -- Displacement risk on compression days
    compression_days_annual          INTEGER,  -- from kpi_compression_quarterly
    group_displacement_note          TEXT,     -- narrative flag for board

    -- Scaffold for future STR group-segment data
    -- These columns will be populated when STR provides group demand segmentation
    str_group_demand_rooms           INTEGER,  -- NULL until STR group data arrives
    str_group_adr                    REAL,     -- NULL until STR group data arrives
    str_group_room_rev               REAL,     -- NULL until STR group data arrives
    str_group_data_available         INTEGER DEFAULT 0,  -- 0=benchmark only, 1=STR-sourced

    data_sources                     TEXT,
    notes                            TEXT,
    loaded_at                        TEXT DEFAULT (datetime('now'))
);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def load_costar_chain_scale(conn: sqlite3.Connection) -> dict:
    """Pull chain scale breakdown for the most recent year (South OC market)."""
    rows = conn.execute(
        """
        SELECT chain_scale, supply_rooms, occupancy_pct, adr_usd, revpar_usd,
               room_revenue_usd, year
        FROM costar_chain_scale_breakdown
        ORDER BY year DESC, loaded_at DESC
        """
    ).fetchall()

    if not rows:
        return {}

    # Use the most recent year only; skip duplicate chain_scale rows
    most_recent_year = rows[0]["year"]
    total_rooms = 0
    group_primary_rooms = 0
    chain_data = {}
    seen = set()
    for r in rows:
        if r["year"] != most_recent_year:
            continue
        scale = (r["chain_scale"] or "").strip()
        if scale in seen:
            continue
        seen.add(scale)
        rooms = int(r["supply_rooms"] or 0)
        total_rooms += rooms
        if scale in ("Upper Upscale", "Upscale"):
            group_primary_rooms += rooms
        chain_data[scale] = {
            "rooms": rooms,
            "occ": float(r["occupancy_pct"] or 0),
            "adr": float(r["adr_usd"] or 0),
            "room_revenue": float(r["room_revenue_usd"] or 0),
        }

    return {
        "total_rooms": total_rooms,
        "group_primary_rooms": group_primary_rooms,
        "chain_data": chain_data,
        "year": most_recent_year,
    }


def load_market_snapshot(conn: sqlite3.Connection) -> dict:
    """Pull most recent CoStar market snapshot for Newport Beach/Dana Point submarket."""
    row = conn.execute(
        """
        SELECT occupancy_pct, adr_usd, revpar_usd, room_revenue_usd
        FROM costar_market_snapshot
        WHERE market LIKE '%Newport%' OR market LIKE '%Dana Point%'
        ORDER BY report_period DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        # Fallback: any market snapshot
        row = conn.execute(
            """
            SELECT occupancy_pct, adr_usd, revpar_usd, room_revenue_usd
            FROM costar_market_snapshot
            WHERE market NOT LIKE '%United States%'
            ORDER BY report_period DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return {}
    return {
        "occ_pct": float(row["occupancy_pct"] or 0),
        "adr": float(row["adr_usd"] or 0),
        "revpar": float(row["revpar_usd"] or 0),
        "room_revenue": float(row["room_revenue_usd"] or 0),
    }


# Dana Point TBID tiered assessment rates (per City ordinance)
# ≤$199.99 → 1.0%  |  $200–$399.99 → 1.5%  |  ≥$400 → 2.0%
def _tbid_rate(adr: float) -> float:
    if adr >= 400.0:
        return 0.020
    if adr >= 200.0:
        return 0.015
    return 0.010


def load_compression_annual(conn: sqlite3.Connection) -> int:
    """Sum compression days (80%+) across all quarters."""
    row = conn.execute(
        "SELECT SUM(days_above_80_occ) as total FROM kpi_compression_quarterly"
    ).fetchone()
    return int(row["total"] or 0) if row else 0


def seed(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_SQL)
    conn.commit()

    chain = load_costar_chain_scale(conn)
    snap = load_market_snapshot(conn)
    compression_days = load_compression_annual(conn)

    total_rooms = chain.get("total_rooms", 5120)
    group_primary_rooms = chain.get("group_primary_rooms", 3098)  # UU + Upscale fallback
    group_primary_pct = round(group_primary_rooms / total_rooms, 3) if total_rooms else 0.605

    # Prefer Newport Beach/Dana Point submarket ADR/occ from market snapshot
    market_adr = snap.get("adr") or 300.89
    market_occ = snap.get("occ_pct") or 71.4

    # Annual room revenue: sum directly from CoStar chain scale (most accurate source).
    # CoStar reports actual room_revenue_usd per chain scale for South OC 2024.
    chain_data = chain.get("chain_data", {})
    chain_total_rev = sum(v["room_revenue"] for v in chain_data.values() if v["room_revenue"] > 0)
    if chain_total_rev > 1_000_000:
        annual_room_rev = round(chain_total_rev, 2)
    else:
        # Fallback: estimate from ADR × occ × rooms × 365
        annual_room_rev = round(market_adr * (market_occ / 100) * total_rooms * 365, 2)

    # Industry benchmarks: STR/CBRE upper-upscale resort coastal (2024)
    BENCH_GROUP_SHARE_LOW = 0.25
    BENCH_GROUP_SHARE_HIGH = 0.32
    BENCH_GROUP_ADR_DISCOUNT = 0.18  # group negotiated rates ~18% below blended ADR

    estimated_group_adr = round(market_adr * (1 - BENCH_GROUP_ADR_DISCOUNT), 2)
    group_rev_low = round(annual_room_rev * BENCH_GROUP_SHARE_LOW, 2)
    group_rev_high = round(annual_room_rev * BENCH_GROUP_SHARE_HIGH, 2)

    # TBID impact — tiered by chain scale per Dana Point ordinance:
    #   ≤$199.99 → 1.0%  |  $200–$399.99 → 1.5%  |  ≥$400 → 2.0%
    # Compute total market TBID using actual room revenue + per-scale ADR tier.
    # Then apply group demand share to estimate group-originated TBID.
    if chain_data:
        total_market_tbid = sum(
            v["room_revenue"] * _tbid_rate(v["adr"])
            for v in chain_data.values()
            if v["room_revenue"] > 0 and v["adr"] > 0
        )
    else:
        total_market_tbid = annual_room_rev * 0.0125  # blended fallback

    tbid_low = round(total_market_tbid * BENCH_GROUP_SHARE_LOW, 2)
    tbid_high = round(total_market_tbid * BENCH_GROUP_SHARE_HIGH, 2)
    tot_low = round(group_rev_low * 0.10, 2)
    tot_high = round(group_rev_high * 0.10, 2)

    # Incremental TBID if group mix grows +5pp (using blended rate from chain data)
    blended_tbid_rate = (total_market_tbid / annual_room_rev) if annual_room_rev > 0 else 0.0125
    tbid_uplift = round(annual_room_rev * 0.05 * blended_tbid_rate, 2)

    # Displacement note
    if compression_days >= 30:
        disp_note = (
            f"{compression_days} compression days/year (80%+ occ): on peak days, "
            "group room blocks displace higher-rate transient leisure demand. "
            "Group business most valuable in shoulder Q1/Q4 when transient demand is soft."
        )
    else:
        disp_note = (
            "Compression risk is moderate. Group blocks can be accommodated without "
            "significant leisure displacement; shoulder-season group strategy is low-risk."
        )

    today = date.today().isoformat()

    conn.execute(
        """
        INSERT INTO group_intelligence (
            benchmark_date,
            total_market_rooms, group_primary_rooms, group_primary_pct,
            benchmark_group_demand_share_low, benchmark_group_demand_share_high,
            benchmark_group_adr_discount_pct,
            market_blended_adr, market_blended_occ_pct, estimated_group_adr,
            estimated_annual_room_rev,
            estimated_group_room_rev_low, estimated_group_room_rev_high,
            estimated_group_tbid_rev_low, estimated_group_tbid_rev_high,
            estimated_group_tot_rev_low, estimated_group_tot_rev_high,
            tbid_uplift_per_5pp_shift,
            compression_days_annual, group_displacement_note,
            str_group_demand_rooms, str_group_adr, str_group_room_rev,
            str_group_data_available,
            data_sources, notes
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?,
            ?, ?,
            ?, ?,
            ?, ?,
            ?,
            ?, ?,
            NULL, NULL, NULL,
            0,
            ?, ?
        )
        ON CONFLICT(benchmark_date) DO UPDATE SET
            total_market_rooms            = excluded.total_market_rooms,
            group_primary_rooms           = excluded.group_primary_rooms,
            group_primary_pct             = excluded.group_primary_pct,
            market_blended_adr            = excluded.market_blended_adr,
            market_blended_occ_pct        = excluded.market_blended_occ_pct,
            estimated_group_adr           = excluded.estimated_group_adr,
            estimated_annual_room_rev     = excluded.estimated_annual_room_rev,
            estimated_group_room_rev_low  = excluded.estimated_group_room_rev_low,
            estimated_group_room_rev_high = excluded.estimated_group_room_rev_high,
            estimated_group_tbid_rev_low  = excluded.estimated_group_tbid_rev_low,
            estimated_group_tbid_rev_high = excluded.estimated_group_tbid_rev_high,
            estimated_group_tot_rev_low   = excluded.estimated_group_tot_rev_low,
            estimated_group_tot_rev_high  = excluded.estimated_group_tot_rev_high,
            tbid_uplift_per_5pp_shift     = excluded.tbid_uplift_per_5pp_shift,
            compression_days_annual       = excluded.compression_days_annual,
            group_displacement_note       = excluded.group_displacement_note,
            data_sources                  = excluded.data_sources,
            notes                         = excluded.notes,
            loaded_at                     = datetime('now')
        """,
        (
            today,
            total_rooms, group_primary_rooms, group_primary_pct,
            BENCH_GROUP_SHARE_LOW, BENCH_GROUP_SHARE_HIGH, BENCH_GROUP_ADR_DISCOUNT,
            market_adr, market_occ, estimated_group_adr,
            annual_room_rev,
            group_rev_low, group_rev_high,
            tbid_low, tbid_high,
            tot_low, tot_high,
            tbid_uplift,
            compression_days, disp_note,
            "costar_chain_scale_breakdown,costar_market_snapshot,kpi_compression_quarterly",
            (
                "Benchmark data only — STR group-segment data not yet available. "
                "Group demand share (25-32%) and ADR discount (18%) are industry benchmarks "
                "for upper-upscale coastal resort markets (STR/CBRE 2024). "
                "Update str_group_* columns and set str_group_data_available=1 "
                "when STR provides group demand segmentation exports."
            ),
        ),
    )
    conn.commit()


def main() -> None:
    print("=== seed_group_benchmarks.py ===\n")
    conn = get_connection()
    try:
        seed(conn)
        row = conn.execute(
            "SELECT * FROM group_intelligence ORDER BY benchmark_date DESC LIMIT 1"
        ).fetchone()
        if row:
            print(f"  [OK] group_intelligence seeded for {row['benchmark_date']}")
            print(f"       Market rooms: {row['total_market_rooms']:,} total | "
                  f"{row['group_primary_rooms']:,} group-primary ({row['group_primary_pct']*100:.0f}%)")
            print(f"       Group room revenue est: "
                  f"${row['estimated_group_room_rev_low']/1e6:.0f}M–"
                  f"${row['estimated_group_room_rev_high']/1e6:.0f}M/yr")
            print(f"       Group TBID est: "
                  f"${row['estimated_group_tbid_rev_low']/1e6:.2f}M–"
                  f"${row['estimated_group_tbid_rev_high']/1e6:.2f}M/yr")
            print(f"       TBID uplift per +5pp group mix: "
                  f"${row['tbid_uplift_per_5pp_shift']/1000:.0f}K")
            print(f"       STR group data: {'available' if row['str_group_data_available'] else 'pending'}")
        conn.execute(
            "INSERT INTO load_log (source, grain, file_name, rows_inserted) "
            "VALUES (?, ?, ?, ?)",
            ("GROUP_BENCHMARKS", "daily", "seed_group_benchmarks.py", 1),
        )
        conn.commit()
    except Exception as exc:
        print(f"  [WARN] seed_group_benchmarks failed: {exc}")
    finally:
        conn.close()
    print("\nDone.\n")


if __name__ == "__main__":
    main()
