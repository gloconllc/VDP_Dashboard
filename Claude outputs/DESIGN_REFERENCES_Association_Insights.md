# Design References: Association Insights Dashboards

**Captured:** September 21, 2026
**Purpose:** Reference examples for a future VDP PULSE update. **Nothing in the app was changed.**
**Scope:** Structure and features only. **The Visit Dana Point palette stays as-is**, see section 7.
**Queued behind:** the CoStar Playwright build.

---

## 1. The four sources

| File | Page title | Theme name |
|---|---|---|
| `Association_Data_Report_Navy.html` | Aggregated Association Data Report | Navy |
| `Member_Pulse_Trend_Evergreen.html` | Member Pulse Survey Trend Report | Evergreen |
| `Compensation_Survey_Navy.html` | Compensation and Workplace Survey 2026 | Navy |
| `Sponsored_Survey_Peach.html` | Healthcare Workflow Software Survey 2026 | Peach |

All four carry the same footer: *"Design, features and format Copyright 2026 Association Insights, LLC. All rights reserved."*

**Worth saying once, plainly.** This is a third party's commercial template family, and the color names in the filenames (Navy, Evergreen, Peach) tell you it is sold as themed variants of one product. What is useful to us is the **information architecture and the feature set**, which are ideas rather than property. We should not reproduce their visual identity or their section wording verbatim in a client-facing product. Everything below is catalogued with that line in mind.

---

## 2. The single most transferable finding

**Two of the four are the identical template with a different palette.** `Compensation_Survey_Navy` and `Sponsored_Survey_Peach` return a byte-for-byte identical section list despite covering completely different subject matter, one a compensation survey, one healthcare workflow software.

That is the actual product insight here. They built **one dashboard grammar** and re-skin it per client and per survey. Every page is a single self-contained HTML file with no backend.

This is close to what `generate_weekly_report.py` already does for VDP, and it is the direction our own report is drifting toward anyway. Their version is just further along.

---

## 3. The shared section grammar

Identical across both survey dashboards, in this exact top-to-bottom order:

1. **At a glance**
2. **Distribution**
3. **Distribution and average by group**
4. **Satisfaction by chapter**
5. **Multi-select responses**
6. **What's linked to the main measure**
7. **Key relationships in the data**
8. **Attitudes**
9. **More from the survey**
10. **Additional questions**
11. **Categorized open-ended responses**
12. **About this dashboard**

Two things stand out about this ordering. It moves from **summary to distribution to segmentation to correlation to open text**, which is a genuine analytical progression rather than a pile of panels. And **correlation gets two full sections** (6 and 7), which most dashboards skip entirely.

---

## 4. The methodology disclosure, which is the best idea here

"About this dashboard" is not a footer note. It has its own named sub-sections:

- **The headline figures**
- **Banded questions**
- **What "clear" and "likely" mean**
- **Trim toggles**
- **Small samples**

That fourth and fifth one are the standouts. **"What 'clear' and 'likely' mean"** defines the hedging vocabulary the dashboard uses before the reader hits it. **"Small samples"** tells the reader where not to trust the numbers.

VDP PULSE has nothing equivalent. We have a glossary in the footer and a data-hierarchy rule in CLAUDE.md, but no reader-facing statement of what our confidence language means or where our samples get thin. Given how often this project has had to distinguish real data from fabricated baselines, an honest methodology panel is arguably the highest-value single addition on this list.

---

## 5. Controls and features

### On the survey dashboards
- **Segment** selector, drives the whole page
- **Average / Median** toggle, a global switch between the two central-tendency views
- **Filters panel**, with the standing instruction *"Use the filters to slice the data"*
- Explicit hover affordance text: *"Hover any bar or column for details"* and *"Hover any bar or column for the underlying numbers"*
- Header row reading *"Respondents  Median  Average"*

### On Member Pulse, which is the feature-rich one
- **Hamburger menu** for navigation
- **Collapsible Filters panel**
- **Key Measures panel**, a table of `Statistic | Average | Median`
- **Print / Save as PDF** with a real options dialog: *Print all sections* or *Print selected sections*, the latter enabling per-section checkboxes, plus *Select all*, *Clear all*, *Print selected*, and an *All sections selected* status indicator
- **Run all Chapter Reports**, opening a multi-select group dialog that generates **bundled PDFs** in one action
- **About this dashboard** info toggle

### The privacy claim
`Sponsored_Survey_Peach` states: *"No data leaves this page, and it works offline."* A self-contained file with no network dependency, said out loud as a selling point.

### The no-JavaScript fallback
Every page degrades to a written instruction rather than a blank screen: *"This report needs JavaScript to draw its charts. Open this file in a web browser (Safari, Chrome, Edge or Firefox) rather than a preview pane or email attachment viewer, and the charts will appear."*

Small, but it is the difference between a client thinking the file is broken and a client knowing what to do.

---

## 6. What maps onto VDP PULSE, and what does not

### Already have it
| Their feature | Ours |
|---|---|
| Section-by-section print selection | The section picker plus "Download Selected Sections as PDF" built 2026-08-31 |
| Per-section navigation | `render_top_nav()` / `render_sub_nav()` in `pages.py`, and the jump-to-section strip |
| Hover-for-underlying-numbers | Plotly default hover across the app |
| Self-contained distributable | `generate_weekly_report.py` producing the 10-page PDF |

### Genuinely missing, in rough order of value
1. **A real methodology panel**, on the "About this dashboard" model, including a small-samples warning and a definition of our own confidence wording. Directly relevant to our fabricated-baseline history.
2. **A global Average / Median toggle.** We report averages almost everywhere. For ADR and spend, where a few luxury properties skew hard, median is often the more honest number, and letting the reader switch is better than us picking.
3. **A correlation section.** Their sections 6 and 7 do what our `cross` audience insights do in prose, but visually. We compute the relationships already in `compute_insights.py`; we just never draw them.
4. **Bundled multi-report generation.** Their "Run all Chapter Reports" is one click to many PDFs. Our analogue would be per-property or per-segment report bundles.
5. **A swappable theme *mechanism*.** Navy, Evergreen, Peach are one layout with the palette swapped out. The transferable part is the mechanism, not their colors: our tokens currently live in three places that must be edited in lockstep, which is exactly the fragility a theme layer removes. Dana Point stays the default and only theme until GloCon actually sells this shape to a second DMO.
6. **The no-JavaScript fallback message**, for when someone opens the HTML in Outlook's preview pane.

### Deliberately not taking
- **Their colors.** Visit Dana Point palette stays, full stop.
- Their section wording verbatim
- Their visual identity generally
- The survey-specific sections (Attitudes, Multi-select responses, Categorized open-ended responses) unless VDP starts running real member or visitor surveys

---

## 7. Colors are settled, so there is no open item

**We keep the Visit Dana Point palette. Their hex values are not wanted and were not captured.** John confirmed this directly, and it is the right call: our palette is already derived from approved Dana Point photography and already validated for contrast.

The authority for it stays where it is:
- `generate_weekly_report.py` holds the constants: `TEAL`, `TEAL_DK`, `TEAL_LT`, `MAROON`, `MAROON_LT`, `SLATE`
- mirrored in `report_template.html`'s `:root` block and in `dashboard/app.py`'s inline CSS, and **all three must change together**
- `dashboard/chart_theme.py` holds the single validated chart palette; `style_fig` is imported from there and nothing gets a fourth theme
- the known constraint stands: brand teal `#0891B2` is 3.68:1 on white, fine for a chart mark, too low for text, so teal text uses `#0E7490`

So the earlier note about needing browser approval to read their palette is **dropped**. We take their information architecture and their feature set; the look stays ours. Nothing further is blocking this work.

If we ever want their exact card geometry or spacing scale, that would need a render, but layout proportions are a design decision we should make against our own content anyway, not copy.

---

## 8. Which of my skills apply when we build

- **`dataviz`** for the chart layer: the form heuristic for picking chart types, stat-tile and KPI-row specs, and the contrast floors. Used to **validate the existing Dana Point palette against any new chart type**, not to choose new colors.
- **`artifact-design`** for page structure, layout grid, and theming if any of this ships as a standalone page.
- **`anthropic-skills:bi-analytics-expert`** for the analytical framing of the correlation sections and the methodology panel.
- **`data:build-dashboard`** for the overall composition pass.
- **`anthropic-skills:visit-anaheim-data-visualization`** if any of this crosses over to the Visit Anaheim side, since it carries the Escapism palette and brand standards.

Order of operations when we start: `dataviz` before a single line of chart code, and the palette validated rather than chosen.

---

**End of reference.** Nothing in the VDP app was touched.
