"""
pulse_charts.py, Plotly figures for the Dana Point PULSE board view.

Color follows the Visit Dana Point design system and the dataviz method:
  * current period  = teal (solid line or bar)
  * same period last year = sand (dashed line or lighter bar), the neutral
    comparison mark
  * Dana Point when compared with other places = terracotta (maroon), the
    "hotel truth" color, with every comparison place in sand
  * a single measure across categories = one color (teal), never a rainbow
Every chart has one y axis. Occupancy and rate are separate small charts, not
a dual-axis chart, so neither scale can exaggerate the other.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import brand_tokens as bt
from chart_theme import brand_fig

CUR = bt.TEAL
PRIOR = bt.SAND
DP = bt.MAROON
OTHER = bt.SAND
SEG = {"Transient": bt.TEAL, "Group": bt.MAROON, "Contract": bt.SAND}

CONFIG = {"displayModeBar": False, "responsive": True}


def _thin_ticks(fig: go.Figure, labels, max_ticks: int = 7) -> None:
    """Category axes draw every label, which collides on a 12 to 24 month
    trend. Show at most max_ticks evenly spaced labels, and print a month's
    year only when it changes ("Sep<br>2025", "Nov", "Jan<br>2026"), so a
    phone-width chart stays readable without rotating the text."""
    labels = [str(x) for x in labels]
    n = len(labels)
    if n == 0:
        return
    step = max(1, -(-n // max_ticks))
    idx = list(range(0, n, step))
    text, prev_year = [], None
    for i in idx:
        parts = labels[i].split()
        if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
            text.append(parts[0] if parts[1] == prev_year else f"{parts[0]}<br>{parts[1]}")
            prev_year = parts[1]
        else:
            text.append(labels[i])
    fig.update_xaxes(tickmode="array", tickvals=[labels[i] for i in idx], ticktext=text, tickangle=0)


def _line_pair(df: pd.DataFrame, cur: str, ly: str, fmt: str, suffix: str = "", prefix: str = "",
               height: int = 280, name_cur: str = "This period", name_ly: str = "Same period last year") -> go.Figure:
    fig = go.Figure()
    if ly in df and df[ly].notna().any():
        fig.add_trace(go.Scatter(
            x=df["label"], y=df[ly], name=name_ly, mode="lines",
            line=dict(color=PRIOR, width=2, dash="dash"),
            hovertemplate=f"%{{x}}<br>Last year: {prefix}%{{y:{fmt}}}{suffix}<extra></extra>",
        ))
    fig.add_trace(go.Scatter(
        x=df["label"], y=df[cur], name=name_cur, mode="lines+markers",
        line=dict(color=CUR, width=2.5), marker=dict(size=7, color=CUR, line=dict(color="#FFFFFF", width=1.5)),
        hovertemplate=f"%{{x}}<br>This period: {prefix}%{{y:{fmt}}}{suffix}<extra></extra>",
    ))
    brand_fig(fig, height=height, hover="x unified", y_suffix=suffix, y_prefix=prefix)
    _thin_ticks(fig, df["label"], max_ticks=7)
    return fig


def occupancy_trend(trend: pd.DataFrame, height: int = 280) -> go.Figure | None:
    if trend is None or trend.empty:
        return None
    return _line_pair(trend, "occ", "occ_ly", ".1f", suffix="%", height=height)


def adr_trend(trend: pd.DataFrame, height: int = 280) -> go.Figure | None:
    if trend is None or trend.empty:
        return None
    return _line_pair(trend, "adr", "adr_ly", ",.0f", prefix="$", height=height)


def day_of_week(dow: pd.DataFrame, height: int = 260) -> go.Figure | None:
    if dow is None or dow.empty:
        return None
    colors = [CUR if d in (4, 5) else PRIOR for d in dow["dow"]]
    fig = go.Figure(go.Bar(
        x=dow["label"], y=dow["occ"], marker=dict(color=colors),
        text=[f"{v:.0f}%" for v in dow["occ"]], textposition="outside", cliponaxis=False,
        textfont=dict(size=11.5, color=bt.INK_2),
        customdata=dow[["adr", "revpar"]].values,
        hovertemplate="%{x} nights<br>Occupancy %{y:.1f}%<br>ADR $%{customdata[0]:,.0f}"
                      "<br>RevPAR $%{customdata[1]:,.0f}<extra></extra>",
    ))
    brand_fig(fig, height=height, legend=False, y_suffix="%")
    fig.update_yaxes(range=[0, max(100, float(dow["occ"].max()) * 1.15)], showticklabels=False, showgrid=False)
    return fig


def spend_trend(dt: pd.DataFrame, height: int = 280) -> go.Figure | None:
    if dt is None or dt.empty or "spending_usd" not in dt:
        return None
    fig = go.Figure()
    if dt["spend_ly"].notna().any():
        fig.add_trace(go.Bar(x=dt["label"], y=dt["spend_ly"], name="Same month last year",
                             marker=dict(color=PRIOR, opacity=0.55),
                             hovertemplate="%{x}<br>Last year: $%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Bar(x=dt["label"], y=dt["spending_usd"], name="This period", marker=dict(color=CUR),
                         hovertemplate="%{x}<br>Visitor spend: $%{y:,.0f}<extra></extra>"))
    brand_fig(fig, height=height, hover="x unified", y_prefix="$")
    fig.update_layout(barmode="group")
    fig.update_yaxes(tickformat="~s")
    _thin_ticks(fig, dt["label"], max_ticks=12)
    return fig


def ranked_bars(labels, values, fmt="{:.1f}%", height: int | None = None, color: str = CUR,
                highlight: str | None = None, hover_name: str = "Share") -> go.Figure:
    labels, values = list(labels), list(values)
    colors = [DP if (highlight and lab == highlight) else color for lab in labels]
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h", marker=dict(color=colors),
        text=[fmt.format(v) for v in values], textposition="outside", cliponaxis=False,
        textfont=dict(size=11.5, color=bt.INK_2),
        hovertemplate="%{y}<br>" + hover_name + ": %{x:.1f}<extra></extra>",
    ))
    brand_fig(fig, height=height or max(200, 34 * len(labels) + 30), legend=False)
    fig.update_yaxes(autorange="reversed", showgrid=False, tickfont=dict(size=12, color=bt.INK_2))
    fig.update_xaxes(showticklabels=False, showline=False, range=[0, max(values) * 1.22 if values else 1])
    return fig


def ad_vs_spend(mix: pd.DataFrame, clean, height: int | None = None) -> go.Figure | None:
    if mix is None or mix.empty:
        return None
    labels = [clean(x) for x in mix["dma"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(y=labels, x=mix["share_pct"].fillna(0), orientation="h", name="Share of visitor spending",
                         marker=dict(color=CUR), text=[f"{v:.1f}%" if pd.notna(v) else "" for v in mix["share_pct"]],
                         textposition="outside", cliponaxis=False, textfont=dict(size=11, color=bt.INK_2),
                         hovertemplate="%{y}<br>Share of visitor spending: %{x:.1f}%<extra></extra>"))
    fig.add_trace(go.Bar(y=labels, x=mix["trip_share_pct"].fillna(0), orientation="h", name="Share of campaign trips",
                         marker=dict(color=DP), text=[f"{v:.1f}%" if pd.notna(v) else "" for v in mix["trip_share_pct"]],
                         textposition="outside", cliponaxis=False, textfont=dict(size=11, color=bt.INK_2),
                         hovertemplate="%{y}<br>Share of campaign trips: %{x:.1f}%<extra></extra>"))
    brand_fig(fig, height=height or max(260, 58 * len(labels) + 40))
    fig.update_layout(barmode="group", bargap=0.28, bargroupgap=0.08)
    fig.update_yaxes(autorange="reversed", showgrid=False, tickfont=dict(size=12, color=bt.INK_2))
    top = float(pd.concat([mix["share_pct"], mix["trip_share_pct"]]).max() or 1)
    fig.update_xaxes(showticklabels=False, showline=False, range=[0, top * 1.25])
    return fig


def place_compare(table: pd.DataFrame, highlight_col: str = "is_dp", height: int = 300) -> go.Figure | None:
    """Occupancy, ADR, and RevPAR as three small horizontal bar charts that
    share one list of places, Dana Point highlighted, each with its own scale."""
    if table is None or table.empty:
        return None
    t = table.copy()
    names = t["market"].tolist()
    colors = [DP if bool(v) else OTHER for v in (t[highlight_col] if highlight_col in t else [False] * len(t))]
    fig = make_subplots(rows=1, cols=3, shared_yaxes=True, horizontal_spacing=0.04,
                        subplot_titles=("Occupancy", "ADR", "RevPAR"))
    specs = [("occ", "{:.1f}%"), ("adr", "${:,.0f}"), ("revpar", "${:,.0f}")]
    for i, (col, fmt) in enumerate(specs, start=1):
        vals = t[col].tolist()
        # Labels sit inside the bar (white) unless the bar is short, where they
        # move just past its end. Keeping long-bar labels inside stops them from
        # spilling into the next small multiple on a phone-width screen.
        top = max(vals) if vals else 1
        inside = [v >= 0.62 * top for v in vals]
        fig.add_trace(go.Bar(
            y=names, x=vals, orientation="h", marker=dict(color=colors, cornerradius=4),
            text=[fmt.format(v) for v in vals], textangle=0,
            textposition=["inside" if i else "outside" for i in inside], insidetextanchor="end",
            textfont=dict(size=11, color=["#FFFFFF" if i else bt.INK_2 for i in inside]), cliponaxis=False,
            hovertemplate="%{y}<br>" + {"occ": "Occupancy %{x:.1f}%", "adr": "ADR $%{x:,.0f}",
                                         "revpar": "RevPAR $%{x:,.0f}"}[col] + "<extra></extra>",
            showlegend=False,
        ), row=1, col=i)
        fig.update_xaxes(showticklabels=False, showgrid=False, showline=False,
                         range=[0, max(vals) * 1.06], row=1, col=i)
    brand_fig(fig, height=height, legend=False)
    fig.update_yaxes(autorange="reversed", showgrid=False, tickfont=dict(size=12, color=bt.INK_2))
    fig.update_annotations(font=dict(size=12, color=bt.INK_3))
    fig.update_layout(margin=dict(t=28))
    return fig


def tiers(df: pd.DataFrame, height: int = 260) -> go.Figure | None:
    if df is None or df.empty:
        return None
    short = {"Luxury & Upper Upscale": "Luxury and<br>upper upscale", "Upscale & Upper Midscale": "Upscale and<br>upper midscale",
             "Midscale & Economy": "Midscale and<br>economy"}
    labels = [short.get(x, x) for x in df["report_scope"]]
    fig = go.Figure(go.Bar(
        x=labels, y=df["revpar_usd"], marker=dict(color=CUR),
        text=[f"${v:,.0f}" for v in df["revpar_usd"]], textposition="outside", cliponaxis=False,
        textfont=dict(size=11.5, color=bt.INK_2),
        customdata=df[["occupancy_pct", "adr_usd"]].values,
        hovertemplate="%{x}<br>RevPAR $%{y:,.0f}<br>Occupancy %{customdata[0]:.1f}%<br>ADR $%{customdata[1]:,.0f}<extra></extra>",
    ))
    brand_fig(fig, height=height, legend=False)
    fig.update_yaxes(showticklabels=False, showgrid=False, range=[0, float(df["revpar_usd"].max()) * 1.22])
    return fig


def room_split(rs: pd.DataFrame, height: int = 260) -> go.Figure | None:
    if rs is None or rs.empty:
        return None
    r = rs.iloc[0]
    labels = ["Luxury and upper upscale", "Upscale and upper midscale", "Midscale and economy"]
    vals = [r["luxury_upper_upscale_rooms"], r["upscale_upper_midscale_rooms"], r["midscale_economy_rooms"]]
    if not any(pd.notna(v) and v for v in vals):
        return None
    fig = go.Figure(go.Pie(
        labels=labels, values=vals, hole=0.62, sort=False, direction="clockwise",
        marker=dict(colors=[bt.TEAL, bt.MAROON, bt.SAND], line=dict(color="#FFFFFF", width=2)),
        textinfo="percent", textfont=dict(size=12, color="#FFFFFF"), insidetextorientation="horizontal",
        hovertemplate="%{label}<br>%{value:,.0f} rooms (%{percent})<extra></extra>",
    ))
    brand_fig(fig, height=height, legend=True)
    fig.update_layout(legend=dict(orientation="h", y=-0.05, yanchor="top", x=0.5, xanchor="center"),
                      margin=dict(t=6, b=6),
                      annotations=[dict(text=f"<b>{int(r['total_rooms']):,}</b><br>rooms", x=0.5, y=0.5,
                                        showarrow=False, font=dict(size=13, color=bt.INK))])
    return fig


def multiyear(my: pd.DataFrame, height: int = 300) -> go.Figure | None:
    if my is None or my.empty:
        return None
    fig = go.Figure()
    styles = {"Newport Beach/Dana Point": dict(color=bt.TEAL, width=2.5),
              "Orange County": dict(color=bt.MAROON, width=2),
              "United States": dict(color=bt.SAND, width=2, dash="dash")}
    for mkt, st in styles.items():
        d = my[my["market"] == mkt].sort_values("year_label")
        if d.empty:
            continue
        fig.add_trace(go.Scatter(
            x=d["year_label"], y=d["revpar_usd"], name=mkt, mode="lines+markers",
            line=st, marker=dict(size=6, color=st["color"]),
            hovertemplate=f"{mkt}<br>%{{x}}: $%{{y:,.0f}} RevPAR<extra></extra>",
        ))
    brand_fig(fig, height=height, hover="closest", y_prefix="$")
    return fig


def segment_mix(seg: dict | None, height: int = 280) -> go.Figure | None:
    if not seg or seg.get("monthly") is None or seg["monthly"].empty:
        return None
    mdf = seg["monthly"]
    fig = go.Figure()
    for name in ("Transient", "Group", "Contract"):
        d = mdf[mdf["segment"] == name]
        if d.empty or d["demand"].sum() == 0:
            continue
        fig.add_trace(go.Bar(x=d["month"].dt.strftime("%b %Y"), y=d["share"], name=name,
                             marker=dict(color=SEG[name]),
                             hovertemplate=f"%{{x}}<br>{name}: %{{y:.0f}}% of rooms sold<extra></extra>"))
    brand_fig(fig, height=height, hover="x unified", y_suffix="%")
    fig.update_layout(barmode="stack", bargap=0.25)
    fig.update_yaxes(range=[0, 100])
    months = [pd.Timestamp(m).strftime("%b %Y") for m in sorted(mdf["month"].unique())]
    _thin_ticks(fig, months, max_ticks=12)
    return fig


def compression(cq: pd.DataFrame, height: int = 260) -> go.Figure | None:
    if cq is None or cq.empty:
        return None
    labels = [q + ("*" if not c else "") for q, c in zip(cq["quarter"], cq["complete"])]
    fig = go.Figure()
    # "2025-Q3" becomes "Q3" with the year printed only when it changes, so
    # eight quarters fit a phone-width axis without overlapping.
    ticktext, prev = [], None
    for q in labels:
        yr, _, qq = q.partition("-")
        ticktext.append(qq if yr == prev else f"{qq}<br>{yr}")
        prev = yr
    fig.add_trace(go.Bar(x=labels, y=cq["d80"], name="Nights at 80%+", marker=dict(color=CUR),
                         text=cq["d80"], textposition="outside", cliponaxis=False,
                         textfont=dict(size=11, color=bt.INK_2),
                         hovertemplate="%{x}<br>%{y} nights at 80% or higher<extra></extra>"))
    fig.add_trace(go.Bar(x=labels, y=cq["d90"], name="Nights at 90%+", marker=dict(color=DP), width=0.28,
                         hovertemplate="%{x}<br>%{y} nights at 90% or higher<extra></extra>"))
    brand_fig(fig, height=height, hover="x unified")
    fig.update_layout(barmode="overlay")
    fig.update_xaxes(tickmode="array", tickvals=labels, ticktext=ticktext, tickangle=0)
    fig.update_yaxes(range=[0, max(1, float(cq["d80"].max())) * 1.25])
    return fig
