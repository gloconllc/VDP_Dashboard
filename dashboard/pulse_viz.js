/* Dana Point PULSE: AntV and D3 visuals for the Insights report (added 2026-10-14).
   Runs after pulse_insights.js. Every slot already holds a working fallback
   (the hand-drawn chart, a table or a list); this file swaps in the richer
   visual only when its library loads and the render succeeds, so a blocked
   CDN or an error never leaves a gap. Libraries load lazily, when a slot first
   scrolls into view. Python computes every number; this file only draws.
   Libraries: G2 5.4.8 (charts), T8 0.3.1 (narrative text), GPT-Vis 1.0.2
   (not used: waterfall moved to G2), G6 5.1.1 (source graph), X6 3.1.8 (pipeline), AntV
   Infographic 0.2.20 (About Dana Point), D3 7.9.0 (calendar; bundled inline). */
(function () {
  "use strict";
  var VIZ = window.PULSE_VIZ || [];
  var C = { teal: "#1D6E86", tealDk: "#123C4A", tealLt: "#8FC4D6", maroon: "#A8461F", maroonLt: "#E08A54", sand: "#9C9186", ink: "#0B2530", ink3: "#64748B", grid: "#E6ECEF", mist: "#F0F7F9", mistB: "#CBE3EA" };
  // Prior-year marks use a deeper light teal (3.45:1 on white) to meet non-text contrast.
  var PRIOR = "#4C93AD";
  var REDUCE = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var KIND = { cur: C.teal, prior: PRIOR, alt: C.maroon, dp: C.maroon, teal: C.teal, maroon: C.maroon, sand: C.sand };
  var FONT = '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
  var LIBS = {
    G2: "@antv/g2@5.4.8/dist/g2.min.js",
    T8: "@antv/t8@0.3.1/dist/t8.min.js",
    GPTVis: "@antv/gpt-vis@1.0.2/dist/umd/index.min.js",
    G6: "@antv/g6@5.1.1/dist/g6.min.js",
    X6: "@antv/x6@3.1.8/dist/x6.min.js",
    AntVInfographic: "@antv/infographic@0.2.20/dist/infographic.min.js"
  };
  // Local copies ship with the app (dashboard/static/antv, served at /app/static/antv/ when
  // enableStaticServing is on), so the visuals no longer depend on a public CDN. The CDNs stay
  // as the second and third choice. (added 2026-10-15)
  var LOCAL = { G2: "g2.min.js", T8: "t8.min.js", G6: "g6.min.js", X6: "x6.min.js", AntVInfographic: "infographic.min.js" };
  var CDNS = ["https://cdn.jsdelivr.net/npm/", "https://unpkg.com/"];
  var pending = {};

  function load(name) {
    if (window[name]) return Promise.resolve(window[name]);
    if (pending[name]) return pending[name];
    pending[name] = new Promise(function (resolve, reject) {
      var urls = (LOCAL[name] ? ["/app/static/antv/" + LOCAL[name]] : []).concat(CDNS.map(function (c) { return c + LIBS[name]; }));
      var i = 0;
      (function next() {
        if (i >= urls.length) { reject(new Error(name + " unavailable")); return; }
        var s = document.createElement("script");
        s.src = urls[i++];
        s.async = true;
        s.onload = function () { window[name] ? resolve(window[name]) : next(); };
        s.onerror = function () { s.remove(); next(); };
        document.head.appendChild(s);
      })();
    });
    return pending[name];
  }

  function fmt(v, f) {
    if (v == null || isNaN(v)) return "n/a";
    if (f === "pct") return v.toFixed(1) + "%";
    if (f === "pct2") return v.toFixed(2) + "%";
    if (f === "usd") return "$" + Math.round(v).toLocaleString("en-US");
    if (f === "usdbig") { var a = Math.abs(v); return a >= 1e9 ? "$" + (v / 1e9).toFixed(2) + "B" : a >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : a >= 1e3 ? "$" + Math.round(v / 1e3) + "K" : "$" + Math.round(v); }
    if (f === "m") return v.toFixed(1) + "M";
    if (f === "num") { var b = Math.abs(v); return b >= 1e6 ? (v / 1e6).toFixed(2) + "M" : Math.round(v).toLocaleString("en-US"); }
    return Math.abs(v) >= 1000 ? Math.round(v).toLocaleString("en-US") : String(Math.round(v * 10) / 10);
  }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }

  // Swap the fallback for the rendered visual only when the render succeeds.
  // Canvas visuals keep the fallback table or list for screen readers (visually
  // hidden); text visuals (T8) replace it so nothing is read twice.
  function swap(el, build, textual) {
    var box = document.createElement("div");
    box.className = "viz-live";
    el.appendChild(box);
    return Promise.resolve().then(function () { return build(box); }).then(function () {
      var rest = Array.prototype.slice.call(el.childNodes).filter(function (n) { return n !== box; });
      if (textual) { rest.forEach(function (n) { n.remove ? n.remove() : el.removeChild(n); }); }
      else {
        var sr = document.createElement("div"); sr.className = "viz-sr";
        rest.forEach(function (n) { sr.appendChild(n); });
        el.appendChild(sr);
        box.setAttribute("aria-hidden", "true");
      }
      el.classList.add("viz-on");
    }).catch(function (err) {
      box.remove();
      el.classList.add("viz-fallback");
      if (window.console) console.warn("[pulse viz]", err && err.message ? err.message : err);
    });
  }

  /* ---------- G2: the report's charts ---------- */
  function g2(el, c) {
    return load("G2").then(function (G2) {
      return swap(el, function (box) {
        var wide = el.getAttribute("data-viz-wide") === "1";
        var chart = new G2.Chart({ container: box, autoFit: true, height: c.type === "hbar" ? Math.max(180, (c.rows || []).length * 34 + 30) : (wide ? 330 : 300), theme: "classic" });
        if (REDUCE) chart.options({ animate: false });
        var common = { axis: { x: { title: false, labelFontSize: 12, labelFill: C.ink3, labelFillOpacity: 1, line: false, tick: false }, y: { title: false, labelFontSize: 12, labelFill: C.ink3, labelFillOpacity: 1, grid: true, gridStroke: C.grid, gridLineDash: [0, 0], gridStrokeOpacity: 1 } } };
        if (c.type === "hbar") {
          var rows = (c.rows || []).map(function (r, i) { return { l: r.l, v: r.v, hi: !!r.hi, f: r.fmt, i: i }; });
          chart.options({
            type: "interval", data: rows, padding: "auto",
            coordinate: { transform: [{ type: "transpose" }] },
            encode: { x: "l", y: "v", color: function (d) { return d.hi ? "hi" : "base"; } },
            scale: { x: { domain: rows.map(function (r) { return r.l; }), paddingInner: 0.35 }, color: { domain: ["base", "hi"], range: [C.teal, C.maroon] } },
            style: { radius: 4 },
            axis: { x: { title: false, labelFontSize: 13, labelFill: C.ink, labelFillOpacity: 1, line: false, tick: false }, y: false },
            legend: false,
            labels: [{ text: function (d) { return fmt(d.v, d.f); }, position: "right", fontSize: 12.5, fontWeight: 700, fill: C.ink, dx: 6 }],
            tooltip: { title: "l", items: [{ channel: "y", name: "Value", valueFormatter: function (v) { return fmt(v, rows[0] && rows[0].f); } }] },
            interaction: { elementHighlight: { background: true } }
          });
        } else {
          var ss = (c.series || []).filter(function (s) { return s.values && s.values.length; });
          var data = [];
          ss.forEach(function (s) { s.values.forEach(function (v, i) { if (v != null) data.push({ x: (c.x || [])[i], v: v, s: s.name, k: s.kind || "cur", f: s.fmt }); }); });
          var names = ss.map(function (s) { return s.name; }), cols = ss.map(function (s) { return KIND[s.kind || "cur"] || C.teal; });
          var f0 = ss[0] ? ss[0].fmt : null;
          var opt = Object.assign({}, common, {
            data: data, padding: "auto",
            scale: { x: { domain: c.x, paddingInner: c.type === "vbar" ? 0.25 : undefined }, color: { domain: names, range: cols }, y: { nice: true } },
            legend: ss.length > 1 ? { color: { position: "top", layout: { justifyContent: "flex-start" }, itemLabelFontSize: 13 } } : false,
            tooltip: { title: "x", items: [{ channel: "y", name: "s", field: "s" }] }
          });
          opt.axis.y.labelFormatter = function (v) { return fmt(v, f0); };
          if (c.type === "vbar") {
            Object.assign(opt, { type: "interval", encode: { x: "x", y: "v", color: "s" }, transform: ss.length > 1 ? [{ type: "dodgeX" }] : [], style: { radius: 3 },
              tooltip: { title: "x", items: [{ channel: "y", name: "Value", valueFormatter: function (v) { return fmt(v, f0); } }] }, interaction: { elementHighlight: { background: true } } });
          } else {
            Object.assign(opt, { type: "view", children: [
              { type: "line", encode: { x: "x", y: "v", color: "s", series: "s" }, style: { lineWidth: 3, lineDash: function (d) { return d[0] && d[0].k === "prior" ? [6, 4] : null; } },
                tooltip: { title: "x", items: [{ channel: "y", name: "Value", valueFormatter: function (v) { return fmt(v, f0); } }] } },
              { type: "point", encode: { x: "x", y: "v", color: "s", shape: "point" }, style: { r: 3.5 }, tooltip: false }
            ], interaction: { tooltip: { crosshairs: true } } });
          }
          chart.options(opt);
        }
        return chart.render();
      });
    });
  }

  /* ---------- T8: section ledes with every figure annotated ---------- */
  var MONTH = "(?:Jan\\.|Feb\\.|Aug\\.|Sept\\.|Oct\\.|Nov\\.|Dec\\.|January|February|March|April|May|June|July|August|September|October|November|December)";
  function t8Syntax(text) {
    var out = text.replace(/[\[\]]/g, "");
    var m = out.match(/^(DANA POINT, Calif\. \([^)]*\):)\s*/);
    var head = "";
    if (m) { head = "**" + m[1] + "** "; out = out.slice(m[0].length); }
    var spans = [];
    function mark(re, fn) { out = out.replace(re, function (all) { spans.push(fn.apply(null, arguments)); return "\u0000" + (spans.length - 1) + "\u0001"; }); }
    mark(new RegExp(MONTH + "(?: \\d{1,2}(?!\\d))?(?:,? \\d{4})?(?: to " + MONTH + "(?: \\d{1,2}(?!\\d))?(?:,? \\d{4})?)?", "g"), function (s) { return "[" + s + "](time_desc)"; });
    mark(/[+−-]\d[\d,.]*%/g, function (s) { var n = parseFloat(s.replace("−", "-")); return "[" + s + "](ratio_value, origin=" + (n / 100) + ', assessment="' + (n > 0 ? "positive" : n < 0 ? "negative" : "equal") + '")'; });
    mark(/[+−-]\d[\d.]* points/g, function (s) { var n = parseFloat(s.replace("−", "-")); return "[" + s + "](delta_value, assessment=\"" + (n > 0 ? "positive" : n < 0 ? "negative" : "equal") + "\")"; });
    mark(/\$[\d,.]+[MBK]?/g, function (s) { return "[" + s + "](metric_value)"; });
    mark(/\b\d[\d,.]*%/g, function (s) { return "[" + s + "](contribute_ratio, origin=" + (parseFloat(s) / 100) + ")"; });
    mark(/\b\d{1,3}(?:,\d{3})+\b|\b\d+(?:\.\d+)?(?: million| billion)\b/g, function (s) { return "[" + s + "](metric_value)"; });
    out = out.replace(/\u0000(\d+)\u0001/g, function (_, i) { return spans[+i]; });
    return head + out;
  }
  function t8(el, text) {
    return load("T8").then(function (T8) {
      return swap(el, function (box) { new T8.Text(box).theme("light").render(t8Syntax(text)); }, true);
    });
  }

  /* ---------- Waterfall: what moved room revenue ----------
     Drawn with G2 rather than GPT-Vis: GPT-Vis's built-in labels were too
     small to read and dropped the smallest step. Same waterfall spec shape. */
  function waterfall(el, w) {
    var SHORT = { "More rooms sold": "More rooms", "Higher nightly rate": "Higher rate" };
    function narrowLab(c) { return el.clientWidth < 520 && SHORT[c] ? SHORT[c] : c; }
    return load("G2").then(function (G2) {
      return swap(el, function (box) {
        var run = 0, rows = w.data.map(function (d) {
          var start = d.isTotal || run === 0 ? 0 : run, end = d.isTotal ? d.value : run + d.value;
          if (!d.isTotal) run = end;
          var step = !d.isTotal && start !== 0;
          return { c: narrowLab(d.category), y: [start / 1e6, end / 1e6], v: d.value, t: d.isTotal ? "total" : step ? (d.value >= 0 ? "up" : "down") : "base",
            lab: (step && d.value >= 0 ? "+" : "") + fmt(d.value, "usdbig") };
        });
        var narrow = el.clientWidth < 520;
        var chart = new G2.Chart({ container: box, autoFit: true, height: 320, theme: "classic" });
        var top = Math.max.apply(null, rows.map(function (r) { return r.y[1]; }));
        chart.options({
          type: "interval", data: rows, padding: "auto", animate: REDUCE ? false : undefined,
          encode: { x: "c", y: "y", color: "t" }, style: { radius: 4, maxWidth: 120 },
          scale: { x: { domain: rows.map(function (r) { return r.c; }), paddingInner: 0.45 }, y: { domain: [0, Math.ceil((top + 20) / 25) * 25], nice: false },
            color: { domain: ["base", "up", "down", "total"], range: [C.teal, C.teal, C.maroon, C.tealDk] } },
          axis: { x: { title: false, labelFontSize: narrow ? 12 : 13, labelFill: C.ink, labelFillOpacity: 1, line: false, tick: false, labelAutoRotate: false, labelAutoHide: false, labelAutoEllipsis: false, transform: [] },
            y: narrow ? false : { title: "Room revenue ($ millions)", titleFontSize: 12, titleFill: C.ink3, titleFillOpacity: 1, labelFontSize: 12, labelFill: C.ink3, labelFillOpacity: 1, grid: true, gridStroke: C.grid, gridStrokeOpacity: 1, gridLineDash: [0, 0], line: false, tick: false } },
          legend: false,
          labels: [{ text: "lab", position: "top", textBaseline: "bottom", dy: -6, fontSize: narrow ? 12 : 13.5, fontWeight: 800, fill: C.ink, fillOpacity: 1 }],
          tooltip: { title: "c", items: [{ field: "lab", name: "Room revenue" }] }
        });
        return chart.render();
      });
    });
  }

  /* ---------- D3: every night of the year ---------- */
  function calendar(el, cal) {
    var d3 = window.d3;
    if (!d3 || !cal.days || !cal.days.length) return Promise.resolve();
    el.innerHTML = "";
    var bins = [60, 70, 80, 90], fills = ["#F8FAFC", "#CBE3EA", "#8FC4D6", "#1D6E86", "#123C4A"];
    var color = d3.scaleThreshold().domain(bins).range(fills);
    var parse = d3.utcParse("%Y-%m-%d");
    var days = cal.days.map(function (d) { return { date: parse(d[0]), occ: d[1], adr: d[2] }; }).sort(function (a, b) { return a.date - b.date; });
    var start = d3.utcSunday.floor(days[0].date), cell = 15, gap = 2, top = 22, left = 30;
    var weeks = d3.utcSunday.count(start, days[days.length - 1].date) + 1;
    var W = left + weeks * (cell + gap) + 4, H = top + 7 * (cell + gap) + 34;
    var wrap = d3.select(el).append("div").attr("class", "cal-wrap");
    var svg = wrap.append("svg").attr("viewBox", "0 0 " + W + " " + H).attr("class", "cal").attr("role", "img")
      .attr("aria-label", "Daily hotel occupancy calendar, " + days.length + " nights");
    ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].forEach(function (lab, i) {
      if (i % 2) svg.append("text").attr("x", left - 6).attr("y", top + i * (cell + gap) + cell - 3).attr("text-anchor", "end").attr("class", "cal-ax").text(lab);
    });
    d3.utcMonths(d3.utcMonth.floor(days[0].date), days[days.length - 1].date).forEach(function (m) {
      var wk = d3.utcSunday.count(start, m < days[0].date ? days[0].date : m);
      svg.append("text").attr("x", left + wk * (cell + gap)).attr("y", top - 8).attr("class", "cal-ax").text(d3.utcFormat("%b")(m));
    });
    var fmtDate = d3.utcFormat("%a, %b %-d, %Y");
    var tip = document.getElementById("vtip");
    svg.append("g").selectAll("rect").data(days).join("rect")
      .attr("x", function (d) { return left + d3.utcSunday.count(start, d.date) * (cell + gap); })
      .attr("y", function (d) { return top + d.date.getUTCDay() * (cell + gap); })
      .attr("width", cell).attr("height", cell).attr("rx", 3)
      .attr("fill", function (d) { return color(d.occ); })
      .attr("stroke", function (d) { return d.occ < 60 ? "#E2E8F0" : "none"; })
      .attr("tabindex", 0)
      .attr("data-tip", function (d) { return fmtDate(d.date) + ": " + d.occ.toFixed(1) + "% occupancy" + (d.adr ? ", $" + Math.round(d.adr) + " average rate" : ""); });
    var lg = svg.append("g").attr("transform", "translate(" + left + "," + (H - 18) + ")");
    ["Under 60%", "60 to 70%", "70 to 80%", "80 to 90%", "90% and up"].forEach(function (lab, i) {
      var g = lg.append("g").attr("transform", "translate(" + i * 104 + ",0)");
      g.append("rect").attr("width", 12).attr("height", 12).attr("rx", 3).attr("fill", fills[i]).attr("stroke", i ? "none" : "#E2E8F0");
      g.append("text").attr("x", 17).attr("y", 10.5).attr("class", "cal-ax").text(lab);
    });
    el.classList.add("viz-on");
    return Promise.resolve();
  }

  /* ---------- G6: which source feeds which section ---------- */
  function graph(el, gdata) {
    // 22 labeled nodes stay legible only on a wide screen; phones keep the table.
    if (el.clientWidth < 600) { el.classList.add("viz-fallback"); return Promise.resolve(); }
    return load("G6").then(function (G6) {
      return swap(el, function (box) {
        box.style.height = "460px";
        var nodes = gdata.nodes.map(function (n) {
          return { id: n.id, data: { kind: n.kind, label: n.label }, style: { labelText: n.label } };
        });
        var g = new G6.Graph({
          container: box, padding: 24,
          data: { nodes: nodes, edges: gdata.edges.map(function (e, i) { return { id: "e" + i, source: e.source, target: e.target }; }) },
          node: { type: "rect", style: function (d) {
            var src = d.data.kind === "source";
            return { size: [src ? 150 : 170, 34], radius: 8, fill: src ? C.tealDk : C.mist, stroke: src ? C.tealDk : C.mistB, lineWidth: 1,
              labelText: d.data.label, labelPlacement: "center", labelFill: src ? "#FFFFFF" : C.ink, labelFontSize: 12.5, labelFontWeight: 700, labelFontFamily: FONT, cursor: "pointer" };
          }, state: { active: { lineWidth: 3, stroke: C.maroon }, inactive: { opacity: 0.35 } } },
          edge: { type: "cubic-horizontal", style: { stroke: PRIOR, lineWidth: 1.5, endArrow: true, endArrowSize: 6 }, state: { active: { stroke: C.maroon, lineWidth: 2.5 }, inactive: { opacity: 0.15 } } },
          layout: { type: "antv-dagre", rankdir: "LR", nodesep: 12, ranksep: 220 },
          behaviors: ["drag-element", { type: "hover-activate", degree: 1, state: "active", inactiveState: "inactive" }]
        });
        // Full-size type when it fits; shrink only on narrow screens.
        return g.render().then(function () { return g.fitCenter(); }).then(function () { return g.fitView({ when: "overflow" }); });
      });
    });
  }

  /* ---------- X6: how data reaches PULSE ---------- */
  function pipeline(el, p) {
    return load("X6").then(function (X6) {
      return swap(el, function (box) {
        var colW = 210, nodeW = 176, nodeH = 52, gapY = 16;
        var byCol = p.columns.map(function (_, ci) { return p.nodes.filter(function (n) { return n.col === ci; }); });
        var maxN = Math.max.apply(null, byCol.map(function (c) { return c.length; }));
        var H = 40 + maxN * (nodeH + gapY);
        box.style.height = Math.max(300, H + 20) + "px";
        var g = new X6.Graph({ container: box, autoResize: true, background: { color: "#FFFFFF" }, interacting: false, panning: false, mousewheel: false });
        p.columns.forEach(function (col, ci) {
          g.addNode({ shape: "rect", x: ci * colW, y: 0, width: nodeW, height: 24, label: col,
            attrs: { body: { fill: "transparent", stroke: "transparent" }, label: { fill: C.teal, fontSize: 11.5, fontWeight: 800, fontFamily: FONT, letterSpacing: 1.2, textTransform: "uppercase" } } });
          var nodes = byCol[ci], offset = (maxN - nodes.length) * (nodeH + gapY) / 2;
          nodes.forEach(function (n, ri) {
            var dark = n.id === "db";
            g.addNode({ id: n.id, shape: "rect", x: ci * colW, y: 34 + offset + ri * (nodeH + gapY), width: nodeW, height: nodeH,
              label: n.label,
              attrs: { body: { fill: dark ? C.tealDk : C.mist, stroke: dark ? C.tealDk : C.mistB, strokeWidth: 1, rx: 8, ry: 8 },
                label: { fill: dark ? "#FFFFFF" : C.ink, fontSize: 12, fontWeight: 600, fontFamily: FONT, textWrap: { width: nodeW - 16, height: nodeH - 8, ellipsis: true } } } });
          });
        });
        p.edges.forEach(function (e) {
          g.addEdge({ source: e[0], target: e[1], router: { name: "er", args: { offset: "center", direction: "H" } }, connector: { name: "rounded" },
            attrs: { line: { stroke: PRIOR, strokeWidth: 1.5, targetMarker: { name: "block", width: 7, height: 6 } } } });
        });
        g.zoomToFit({ padding: 12, maxScale: 1 });
      });
    });
  }

  /* ---------- AntV Infographic: About Dana Point ---------- */
  function infographic(el, facts) {
    var labels = ["Location", "Dolphin and Whale Watching Capital", "Whale watching every day", "Best time to visit", "A seaside state of mind"];
    var lines = facts.map(function (f, i) { return "    - label " + (labels[i] || "Dana Point") + "\n      desc " + f.text.replace(/\n/g, " "); }).join("\n");
    var syntax = "infographic list-grid-candy-card-lite\ndata\n  title About Dana Point\n  desc In Visit Dana Point's own words\n  lists\n" + lines + "\ntheme\n  palette #1D6E86 #123C4A #A8461F #8FC4D6 #9C9186";
    return load("AntVInfographic").then(function (lib) {
      return swap(el, function (box) {
        box.style.minHeight = "320px";
        var info = new lib.Infographic({ container: box, width: "100%", height: "100%" });
        info.render(syntax);
        var list = el.parentNode && el.parentNode.querySelector(".dest-list");
        if (list) list.classList.add("dest-compact");
        return new Promise(function (r) { setTimeout(r, 400); });
      });
    });
  }

  var RENDER = { g2: g2, t8: t8, waterfall: waterfall, calendar: calendar, graph: graph, pipeline: pipeline, info: infographic };
  function run(el) {
    if (el.getAttribute("data-viz-done")) return;
    el.setAttribute("data-viz-done", "1");
    var item = VIZ[+el.getAttribute("data-viz")];
    if (!item || !RENDER[item.kind]) return;
    // A visual switched off in the app keeps its original chart, table or text;
    // a slot with nothing underneath (the calendar) is hidden with its figure.
    var on = window.PULSE_VIZ_ON;
    if (on && on.indexOf(item.kind) < 0) {
      if (!el.children.length && !el.textContent.trim()) { var fig = el.closest("figure"); if (fig) fig.style.display = "none"; }
      return;
    }
    try { RENDER[item.kind](el, item.spec); } catch (err) { if (window.console) console.warn("[pulse viz]", err); }
  }
  function observe() {
    var els = document.querySelectorAll("[data-viz]:not([data-viz-done])");
    if (!("IntersectionObserver" in window)) { els.forEach(run); return; }
    var io = new IntersectionObserver(function (es) { es.forEach(function (e) { if (e.isIntersecting && e.target.offsetWidth > 0) { io.unobserve(e.target); run(e.target); } }); }, { rootMargin: "200px" });
    els.forEach(function (e) { io.observe(e); });
  }
  // Pages are drawn on navigation, so watch for new slots as well as the first ones.
  observe();
  new MutationObserver(function () { observe(); }).observe(document.getElementById("app") || document.body, { childList: true, subtree: true });
})();
