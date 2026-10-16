/* Dana Point PULSE Insights: page renderer.
   Reads window.PULSE_DATA (built by pulse_insights.py) and draws every page.
   No numbers are computed here; Python computes, JS draws. */
(function () {
  "use strict";
  var D = window.PULSE_DATA, GEO = window.PULSE_GEO || { states: {} };
  var app = document.getElementById("app");
  var LOGO = app.getAttribute("data-logo");
  var PHOTO = app.getAttribute("data-photo") || "", PHOTO_SITE = app.getAttribute("data-photo-site") || "", PHOTO_CREDIT = app.getAttribute("data-photo-credit") || "";
  var RM = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  /* ---------- helpers ---------- */
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]; }); }
  function fmt(v, f) {
    if (v == null || isNaN(v)) return "n/a";
    if (f === "pct") return v.toFixed(1) + "%";
    if (f === "pct2") return v.toFixed(2) + "%";
    if (f === "usd") return "$" + Math.round(v).toLocaleString("en-US");
    if (f === "usdbig") { var a = Math.abs(v); return a >= 1e9 ? "$" + (v / 1e9).toFixed(2) + "B" : a >= 1e6 ? "$" + (v / 1e6).toFixed(1) + "M" : a >= 1e3 ? "$" + Math.round(v / 1e3) + "K" : "$" + Math.round(v); }
    if (f === "m") return v.toFixed(1) + "M";
    if (f === "num") { var b = Math.abs(v); return b >= 1e6 ? (v / 1e6).toFixed(2) + "M" : Math.round(v).toLocaleString("en-US"); }
    return String(v);
  }
  function deltaCls(s) { s = String(s || ""); return s.indexOf("▲") === 0 ? "up" : s.indexOf("▼") === 0 ? "dn" : ""; }
  function bold(s) {
    // bold the statistics in prose: money, percents, points, big counts
    return esc(s).replace(/(\$[\d,.]+[MBK]?|[+−-]?\d[\d,.]*%|[+−-]\d[\d.]* points|\b\d{1,3}(?:,\d{3})+\b)/g, '<b class="kv">$1</b>');
  }
  function dateline(s) { return bold(s).replace(/^(DANA POINT, Calif\. \([^)]*\):)/, '<span class="dateline">$1</span>'); }
  function el(tag, cls, html) { var e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; }
  function toast(msg) { var t = el("div", "toast", esc(msg)); t.setAttribute("role", "status"); document.body.appendChild(t); setTimeout(function () { t.remove(); }, 2600); }

  /* ---------- skeleton ---------- */
  var sections = D.sectionList; // [{id, tab}]
  var state = { cur: sections[0].id, all: false };
  var h = [];
  h.push('<a class="skip" href="#main">Skip to the report</a>');
  h.push('<div class="wrap"><header class="hdr" role="banner"><div class="hdr-l">' +
    '<div class="badge-logo"><img src="' + LOGO + '" alt="Visit Dana Point"></div><div>' +
    '<div class="eyebrow">Visit Dana Point · PULSE</div><h1 class="title">' + esc(D.title) + '</h1>' +
    '<div class="sub"><span>' + esc(D.subtitle) + '</span><span class="dot">•</span><span class="pill-status"><i></i>' + esc(D.period) + '</span></div></div></div>' +
    '<div class="ctrls" role="toolbar" aria-label="Report controls">' +
    '<button class="ctl" id="prev" aria-label="Previous section">‹ Previous</button><button class="ctl" id="next" aria-label="Next section">Next ›</button>' +
    '<div class="seg" role="group" aria-label="View"><button id="one" aria-pressed="true">One page</button><button id="allp" aria-pressed="false">All pages</button></div>' +
    '<button class="ctl" id="fs">Full screen</button><button class="ctl primary" id="exp">Export</button></div></header></div>');
  h.push('<div class="tabs-bar"><div class="wrap"><nav class="tabs" id="tabs" aria-label="Sections">' +
    sections.map(function (s) { return '<button class="tab" data-s="' + s.id + '">' + esc(s.tab) + "</button>"; }).join("") + "</nav></div></div>");
  h.push('<main id="main" class="wrap" tabindex="-1"><div class="pageline"><span id="pl"></span><span>' + esc(D.period_detail) + "</span></div><div id=\"pages\"></div></main>");
  h.push('<div class="wrap"><footer class="foot"><span>Sources: STR; CoStar; Datafy; Later; Visit California; BLS; Visit Dana Point events. ' + esc(D.contact) + '. Media contact: <a class="contact-link" href="mailto:john.picou@gloconsolutions.com">GloCon Solutions LLC, john.picou@gloconsolutions.com</a></span><button class="totop" id="totop">↑ Go to top</button></footer></div>');
  h.push('<div id="vtip" hidden></div>');
  app.innerHTML = h.join("");
  var pagesEl = document.getElementById("pages");

  /* ---------- charts ---------- */
  var uid = 0;
  function chartFrame(c, inner) {
    return '<figure class="cf reveal"><h3>' + esc(c.title) + '</h3><p class="cs">' + esc(c.sub || "") + '</p><div class="body">' + inner + '</div><p class="cap">Source: ' + esc(c.src) + "</p></figure>";
  }
  function hbars(rows, opts) {
    opts = opts || {};
    if (!rows || !rows.length) return '<p class="cap">No data for this period.</p>';
    var mx = Math.max.apply(null, rows.map(function (r) { return Math.abs(r.v); })) || 1;
    return '<div class="hbars" role="img" aria-label="' + esc(rows.map(function (r) { return r.l + " " + fmt(r.v, r.fmt); }).join("; ")) + '">' + rows.map(function (r) {
      var w = Math.abs(r.v) / mx * 100;
      return '<div class="hb' + (r.hi ? " hi" : "") + '" data-tip="' + esc(r.l + ": " + fmt(r.v, r.fmt)) + '" tabindex="0"><span class="lab">' + esc(r.l) + '</span><span class="t"><span class="f' + (r.v < 0 ? " neg" : "") + '" data-w="' + w.toFixed(1) + '"></span></span><span class="val">' + (opts.signed ? (r.v >= 0 ? "▲ +" : "▼ −") + Math.abs(r.v).toFixed(1) + "%" : fmt(r.v, r.fmt)) + "</span></div>";
    }).join("") + "</div>";
  }
  var COL = { cur: "#1D6E86", prior: "#8FC4D6", alt: "#A8461F" };
  function axisMax(v) { if (v <= 0) return 1; var p = Math.pow(10, Math.floor(Math.log10(v))); var n = v / p; return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * p; }
  function seriesChart(c, wide) {
    var W = wide ? 1100 : 720, H = wide ? 330 : 300, L = 56, R = 16, T = 14, B = 34, x = c.x || [], ss = (c.series || []).filter(function (s) { return s.values && s.values.length; });
    if (!x.length || !ss.length) return '<p class="cap">No data for this period.</p>';
    var all = []; ss.forEach(function (s) { s.values.forEach(function (v) { if (v != null) all.push(v); }); });
    var mx = axisMax(Math.max.apply(null, all)), mn = 0;
    if (c.type === "line") { var lo = Math.min.apply(null, all); if (lo > mx * 0.5) mn = Math.floor(lo * 0.85 / (mx / 10)) * (mx / 10); }
    var iw = W - L - R, ih = H - T - B, n = x.length;
    function Y(v) { return T + ih - (v - mn) / (mx - mn) * ih; }
    var f0 = ss[0].fmt, s = ['<svg class="vz" viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="' + esc(c.title) + '">'];
    for (var g = 0; g <= 5; g++) { var gv = mn + (mx - mn) * g / 5, gy = Y(gv); s.push('<line class="gl" x1="' + L + '" x2="' + (W - R) + '" y1="' + gy + '" y2="' + gy + '"/><text class="ax" x="' + (L - 8) + '" y="' + (gy + 4) + '" text-anchor="end">' + fmt(gv, f0 === "pct" ? "pct" : f0 === "usd" ? "usd" : f0 === "usdbig" ? "usdbig" : f0 === "m" ? "m" : "num") + "</text>"); }
    var step = iw / n, every = n > 14 ? Math.ceil(n / 12) : 1;
    x.forEach(function (lab, i) { if (i % every === 0) s.push('<text class="ax" x="' + (L + step * (i + 0.5)) + '" y="' + (H - 10) + '" text-anchor="middle">' + esc(lab) + "</text>"); });
    if (c.type === "vbar") {
      var bw = step * 0.78 / ss.length;
      ss.forEach(function (se, k) {
        se.values.forEach(function (v, i) {
          if (v == null) return;
          var bx = L + step * i + step * 0.11 + bw * k, by = Y(v);
          s.push('<rect class="bar" x="' + bx.toFixed(1) + '" y="' + by.toFixed(1) + '" width="' + Math.max(bw - 2, 1).toFixed(1) + '" height="' + (T + ih - by).toFixed(1) + '" rx="2" fill="' + COL[se.kind || "cur"] + '" data-tip="' + esc(x[i] + " · " + se.name + ": " + fmt(v, se.fmt)) + '" tabindex="0"/>');
        });
      });
    } else {
      ss.forEach(function (se) {
        var d = "", pts = [];
        se.values.forEach(function (v, i) { if (v == null) return; var px = L + step * (i + 0.5), py = Y(v); d += (d ? "L" : "M") + px.toFixed(1) + "," + py.toFixed(1); pts.push([px, py, v, i]); });
        s.push('<path class="ln" d="' + d + '" stroke="' + COL[se.kind || "cur"] + '" stroke-width="' + (se.kind === "prior" ? 2.5 : 3.5) + '"' + (se.kind === "prior" ? ' stroke-dasharray="6 4"' : "") + "/>");
        pts.forEach(function (p) { s.push('<circle cx="' + p[0].toFixed(1) + '" cy="' + p[1].toFixed(1) + '" r="3.5" fill="' + COL[se.kind || "cur"] + '"/><circle class="hit" cx="' + p[0].toFixed(1) + '" cy="' + p[1].toFixed(1) + '" r="11" data-tip="' + esc(x[p[3]] + " · " + se.name + ": " + fmt(p[2], se.fmt)) + '" tabindex="0"/>'); });
        var last = pts[pts.length - 1];
        if (last) s.push('<text class="dl" x="' + Math.min(last[0] + 6, W - 4) + '" y="' + (last[1] - 8) + '" text-anchor="end" fill="' + COL[se.kind || "cur"] + '">' + esc(se.name) + "</text>");
      });
    }
    s.push("</svg>");
    var leg = ss.length > 1 ? '<div class="legend">' + ss.map(function (se) { return '<span><i style="background:' + COL[se.kind || "cur"] + '"></i>' + esc(se.name) + "</span>"; }).join("") + "</div>" : "";
    return leg + s.join("");
  }
  window.PULSE_VIZ = window.PULSE_VIZ || [];
  function vizSlot(kind, spec) { window.PULSE_VIZ.push({ kind: kind, spec: spec }); return ' data-viz="' + (window.PULSE_VIZ.length - 1) + '"'; }
  function chart(c, wide) {
    if (!c) return "";
    var vb = ' data-viz-wide="' + (wide ? 1 : 0) + '"' + vizSlot("g2", c);
    var out = chart0(c, wide);
    return out.replace('<div class="body">', '<div class="body"' + vb + '>');
  }
  function chart0(c, wide) {
    if (!c) return "";
    if (c.type === "hbar") return chartFrame(c, hbars(c.rows));
    if (c.type === "line" || c.type === "vbar") return chartFrame(c, seriesChart(c, wide));
    return "";
  }

  /* ---------- infographics ---------- */
  function ig(g) {
    var b = "";
    if (g.type === "chain") {
      b = '<div class="chain">' + g.steps.map(function (s, i) { return (i ? '<span class="ar" aria-hidden="true">→</span>' : "") + '<div class="st" data-tip="' + esc(s.tip || s.l) + '" tabindex="0"><div class="v">' + esc(s.v) + '</div><div class="l">' + esc(s.l) + "</div></div>"; }).join("") + "</div>";
    } else if (g.type === "waffle") {
      var a = Math.round(g.parts[0].v), cells = "";
      for (var i = 0; i < 100; i++) cells += '<i class="' + (i < a ? "a" : "b") + '"></i>';
      b = '<div class="ig-row"><div class="waffle" role="img" aria-label="' + esc(g.parts.map(function (p) { return p.v.toFixed(1) + "% " + p.l; }).join(", ")) + '" data-tip="' + esc(g.parts[0].v.toFixed(1) + "% " + g.parts[0].l) + '" tabindex="0">' + cells + '</div><div class="ig-leg">' +
        g.parts.map(function (p, k) { return '<div><div class="v" style="color:' + (k ? "#0B2530" : "#123C4A") + '">' + p.v.toFixed(1) + '%</div><div>' + esc(p.l) + "</div></div>"; }).join("") + "</div></div>";
    } else if (g.type === "share") {
      b = '<div class="sharebar" role="img" aria-label="' + esc(g.parts.map(function (p) { return p.v.toFixed(1) + "% " + p.l; }).join(", ")) + '">' + g.parts.map(function (p, k) { return '<div class="' + (k ? "b" : "a") + '" data-w="' + p.v + '" data-tip="' + esc(p.l + ": " + p.v.toFixed(1) + "%") + '" tabindex="0">' + p.v.toFixed(1) + "%</div>"; }).join("") + '</div><div class="legend" style="margin-top:10px">' +
        g.parts.map(function (p, k) { return '<span><i style="background:' + (k ? "#8FC4D6" : "#123C4A") + '"></i>' + esc(p.l) + "</span>"; }).join("") + "</div>";
    } else if (g.type === "receipt") {
      var tot = g.items.reduce(function (s, x) { return s + x.v; }, 0), cols = ["#123C4A", "#1D6E86", "#8FC4D6", "#A8461F", "#E08A54", "#9C9186", "#CBD9DE"];
      b = '<div class="receipt">' + g.items.map(function (x) { return '<div class="rr" data-tip="' + esc(x.l + ": " + x.v.toFixed(1) + "¢ of every $1") + '" tabindex="0"><span>' + esc(x.l) + "</span><b>" + x.v.toFixed(1) + "¢</b></div>"; }).join("") +
        '<div class="tot"><span>Every visitor dollar</span><span>' + tot.toFixed(0) + '¢</span></div><div class="dollar" aria-hidden="true">' + g.items.map(function (x, k) { return '<span style="width:' + x.v + "%;background:" + cols[k % cols.length] + '"></span>'; }).join("") + "</div></div>";
    } else if (g.type === "towers") {
      var mx = Math.max.apply(null, g.rows.map(function (r) { return r.v; })) || 1;
      b = '<div class="towers" role="img" aria-label="' + esc(g.rows.map(function (r) { return r.l + " " + r.v.toFixed(1) + "%"; }).join(", ")) + '">' + g.rows.map(function (r) { return '<div class="tw' + (r.hi ? " hi" : "") + '" data-tip="' + esc(r.l + ": " + r.v.toFixed(1) + "% occupancy") + '" tabindex="0"><span class="v">' + r.v.toFixed(1) + '%</span><span class="bx" data-h="' + (r.v / mx * 100).toFixed(1) + '"></span><span class="d">' + esc(r.l.toUpperCase()) + "</span></div>"; }).join("") + "</div>";
    } else if (g.type === "versus") {
      var m2 = Math.max(g.a.v, g.b.v) || 1;
      b = '<div class="vs">' + [["a", g.a], ["b", g.b]].map(function (p) { return '<div class="row ' + p[0] + '" data-tip="' + esc(p[1].l + ": " + fmt(p[1].v, p[1].fmt)) + '" tabindex="0"><span>' + esc(p[1].l) + '</span><span class="t"><span class="f" data-w="' + (p[1].v / m2 * 100).toFixed(1) + '"></span></span><span class="v">' + fmt(p[1].v, p[1].fmt) + "</span></div>"; }).join("") + "</div>";
    } else if (g.type === "funnel") {
      var m3 = g.stages.length ? g.stages[0].v || 1 : 1;
      b = '<div class="funnel">' + g.stages.map(function (s) { var w = Math.max(30, Math.sqrt(s.v / m3) * 100); return '<div class="fn" style="width:' + w.toFixed(0) + '%" data-tip="' + esc(s.l + ": " + fmt(s.v, s.fmt)) + '" tabindex="0"><span>' + esc(s.l) + "</span><span>" + fmt(s.v, s.fmt) + "</span></div>"; }).join("") + "</div>";
    } else if (g.type === "board") {
      b = '<div class="board" role="table" aria-label="Origin airports">' + g.rows.map(function (r) { return '<div class="br" role="row" data-tip="' + esc(r.name + ": " + r.v.toFixed(2) + "%") + '" tabindex="0"><span class="cd">' + esc(r.code) + "</span><span>" + esc(r.name) + '</span><span class="pc">' + r.v.toFixed(2) + "%</span></div>"; }).join("") + "</div>";
    }
    return '<div class="ig reveal"><h3>' + esc(g.title) + '</h3><p class="cs">' + esc(g.sub || "") + "</p>" + b + '<p class="cap">Source: ' + esc(g.src) + "</p></div>";
  }

  /* ---------- map ---------- */
  function map(mp) {
    var vals = mp.states || {}, list = Object.keys(vals).map(function (k) { return vals[k]; }).sort(function (a, b) { return b - a; });
    var t1 = list[Math.floor(list.length * 0.1)] || 0, t2 = list[Math.floor(list.length * 0.35)] || 0;
    var s = ['<svg class="vz map" viewBox="0 0 975 610" role="img" aria-label="' + esc(mp.title) + '">'];
    Object.keys(GEO.states || {}).forEach(function (nm) {
      var v = vals[nm], cls = v == null ? "" : v >= t1 ? " b3" : v >= t2 ? " b2" : " b1";
      s.push('<path class="st' + cls + '" d="' + GEO.states[nm] + '"' + (v != null ? ' data-tip="' + esc(nm + ": " + v.toFixed(2) + "% of trips") + '" tabindex="0"' : "") + "/>");
    });
    var pins = (mp.pins || []).slice().sort(function (a, b) { return b.v - a.v; }), mx = pins.length ? pins[0].v : 1, boxes = [];
    pins.forEach(function (p, i) {
      var r = 6 + 26 * Math.sqrt(p.v / mx);
      s.push('<circle class="pin" cx="' + p.x + '" cy="' + p.y + '" r="' + r.toFixed(1) + '" data-r="' + r.toFixed(1) + '" data-tip="' + esc(p.name + ": " + p.v.toFixed(1) + "% of visitor spending") + '" tabindex="0"/>');
      var lx = p.x + r + 3, ly = p.y + 4, lw = (p.name.length + 6) * 6.6, clash = boxes.some(function (b) { return lx < b[2] && lx + lw > b[0] && ly - 12 < b[3] && ly + 3 > b[1]; });
      if (i < 8 && !clash) { boxes.push([lx, ly - 12, lx + lw, ly + 3]); s.push('<text class="pl" x="' + lx + '" y="' + ly + '">' + esc(p.name) + " " + p.v.toFixed(1) + "%</text>"); }
    });
    if (mp.home) s.push('<path class="home" d="M' + mp.home.x + "," + (mp.home.y - 9) + "l9,9l-9,9l-9,-9z\" data-tip=\"Dana Point\" tabindex=\"0\"/>");
    s.push("</svg>");
    var leg = '<div class="map-leg"><div class="lbl">States by share of trips</div><div class="bins"><span><i style="background:#1D6E86"></i>Top 10% of states</span><span><i style="background:#8FC4D6"></i>Next 25%</span><span><i style="background:#CBE3EA"></i>Others with trips</span><span><i style="background:#A8461F"></i>Dana Point</span></div>' +
      '<div class="lbl" style="margin-top:14px">Top markets by visitor spending</div><ol>' + pins.slice(0, 8).map(function (p) { return "<li>" + esc(p.name) + " <b>" + p.v.toFixed(1) + "%</b></li>"; }).join("") + "</ol></div>";
    return '<figure class="cf reveal"><h3>' + esc(mp.title) + '</h3><p class="cs">' + esc(mp.sub) + '</p><div class="map-grid"><div>' + s.join("") + "</div>" + leg + '</div><p class="cap">Source: ' + esc(mp.src) + "</p></figure>";
  }

  /* ---------- tables ---------- */
  function table(t, cls) {
    if (!t || !t.rows || !t.rows.length) return "";
    var num = t.num || [];
    return '<div class="block reveal"><h3 class="h2">' + esc(t.title) + '</h3><p class="cap" style="margin:0 0 10px">' + esc(t.sub || "") + '</p><div class="tbl-wrap" tabindex="0" role="region" aria-label="' + esc(t.title) + '"><table class="t ' + (cls || "") + '"><thead><tr>' +
      t.cols.map(function (c, j) { return '<th scope="col" class="' + (num.indexOf(j) >= 0 ? "n" : "") + '">' + esc(c) + "</th>"; }).join("") + "</tr></thead><tbody>" +
      t.rows.map(function (r, i) { return '<tr class="' + (t.hl === i ? "hl" : "") + '">' + r.map(function (v, j) { var tag = j ? "td" : "th"; return "<" + tag + (j ? "" : ' scope="row"') + ' class="' + (num.indexOf(j) >= 0 ? "n " : "") + deltaCls(v) + '">' + esc(v) + "</" + tag + ">"; }).join("") + "</tr>"; }).join("") +
      '</tbody></table></div><p class="cap">Source: ' + esc(t.src) + "</p></div>";
  }
  function heat(hm) {
    if (!hm || !hm.rows || !hm.rows.length) return "";
    function bin(v) { return v == null ? "" : v < 60 ? "h1c" : v < 70 ? "h2c" : v < 80 ? "h3c" : v < 90 ? "h4c" : "h5c"; }
    return '<div class="block reveal"><h3 class="h2">' + esc(hm.title) + '</h3><p class="cap" style="margin:0 0 10px">' + esc(hm.sub) + '</p><div class="tbl-wrap" tabindex="0" role="region" aria-label="' + esc(hm.title) + '"><table class="t heat"><thead><tr><th>Month</th>' + hm.cols.map(function (c) { return '<th class="n">' + esc(c) + "</th>"; }).join("") + '<th>Nights at 90% or higher</th></tr></thead><tbody>' +
      hm.rows.map(function (r) { return '<tr><th scope="row">' + esc(r.m) + (r.partial ? " (partial)" : "") + "</th>" + [r.wn, r.we, r.all].map(function (v, k) { return '<td class="c ' + bin(v) + '" data-tip="' + esc(r.m + " · " + hm.cols[k] + ": " + fmt(v, "pct")) + '" tabindex="0">' + fmt(v, "pct") + "</td>"; }).join("") + "<td>" + r.n90 + "</td></tr>"; }).join("") +
      '</tbody></table></div><div class="heat-lg"><span><i class="h1c"></i>Under 60%</span><span><i class="h2c"></i>60–70%</span><span><i class="h3c"></i>70–80%</span><span><i class="h4c"></i>80–90%</span><span><i class="h5c"></i>90% and up</span></div><p class="cap">Source: ' + esc(hm.src) + "</p></div>";
  }

  /* ---------- pages ---------- */
  function overview() {
    var S = D.summary, p = [];
    // Photo banner: visitdanapoint.com photo on top, bundled photo underneath as the fallback
    var layers = [PHOTO_SITE, PHOTO].filter(Boolean).map(function (u) { return "url('" + u + "')"; }).join(", ");
    if (layers) p.push('<div class="hero-photo ov-a" role="img" aria-label="Aerial view of the Dana Point headlands and harbor" style="background-image:' + layers + '"><div class="hero-scrim"></div><div class="hero-in"><div class="hero-k">Dana Point PULSE</div><div class="hero-t">' + esc(D.period) + '</div></div>' + (PHOTO_CREDIT ? '<span class="hero-credit">' + esc(PHOTO_CREDIT) + "</span>" : "") + "</div>");
    // PULSE Summary announcement
    p.push('<section class="block reveal ov-a" aria-labelledby="ps-h">' +
      '<article class="paper" id="paper"><div class="nameplate"><span class="np" id="ps-h">PULSE Summary</span><span class="cap" style="margin:0">Dana Point PULSE · ' + esc(D.period) + " · Prepared " + esc(D.prepared) + "</span></div>" +
      '<div class="release"><span>For immediate release</span></div>' +
      '<div class="hl-label">Highlight: ' + esc(S.highlight) + '</div><h2 class="headline">' + esc(S.headline) + '</h2><p class="subhead">' + bold(S.sub) + "</p>" +
      '<div class="lbl">By the numbers</div><div class="btn-row">' + S.stats.map(function (s) { return '<div class="btn-stat" data-tip="' + esc(s.calc || s.src) + '" tabindex="0"><div class="v" data-count="' + esc(s.v) + '">' + esc(s.v) + '</div><div class="l">' + esc(s.l) + '</div><div class="cap">' + esc(s.src) + "</div></div>"; }).join("") + "</div>" +
      '<div class="cols3">' + S.paras.map(function (x, i) { return "<p>" + (i ? bold(x) : dateline(x)) + "</p>"; }).join("") + "</div>" +
      '<div class="block"><div class="lbl">Growth vs. a year earlier</div>' + hbars(S.growth.map(function (g) { return { l: g.l, v: g.v, fmt: "pct", hi: false }; }), { signed: true }) + "</div>" +
      '<p class="about">' + esc(S.about) + '</p><div class="endmark">###</div></article></section>');
    p.push(qaBlock({ q: "What does the board need to know this period?", a: S.sub, src: "PULSE Summary, " + D.period, v: (S.stats && S.stats[0]) ? S.stats[0].v : "", l: (S.stats && S.stats[0]) ? S.stats[0].l : "" }, "ov-a"));
    // The overview follows the summary
    p.push('<div class="sec-h ov-b" style="margin-top:28px"><h2 class="h1">Overview</h2><span class="chip">' + esc(D.period) + "</span></div>");
    // Key takeaways
    p.push('<section class="block reveal ov-b" aria-labelledby="tk-h"><h3 class="h2" id="tk-h">Key takeaways</h3><p class="cap" style="margin:0 0 12px">The headline number from each section, with its period. Select one to read the full story.</p><div class="tk-grid">' +
      D.takeaways.map(function (t) { return '<button class="tk" data-go="' + t.id + '"><span class="k">' + esc(t.tab) + '</span><span class="v">' + esc(t.v) + '</span><span class="l">' + esc(t.l) + '</span><span class="hd">' + esc(t.head) + '</span><span class="p">' + esc(t.src) + '</span><span class="go">Read the section →</span></button>'; }).join("") + "</div></section>");
    // About Dana Point sits behind the "i" (collapsed), as clean aligned cards in Visit Dana Point's own words
    if (D.destination && D.destination.length) {
      p.push('<details class="dest-i ov-i"><summary><span class="ic" aria-hidden="true">i</span><span>About Dana Point</span></summary><p class="cap" style="margin:10px 0 12px">In Visit Dana Point\'s own words, from <a class="contact-link" href="https://visitdanapoint.com/" target="_blank" rel="noopener">visitdanapoint.com</a>.</p><ul class="dest-list">' +
        D.destination.map(function (d) { return '<li><span class="dest-t">' + esc(d.text) + '</span><a class="src dest-src" href="' + esc(d.url) + '" target="_blank" rel="noopener">Source: ' + esc(d.src) + "</a></li>"; }).join("") + "</ul></details>");
    }
    // Questions and short answers: the full list stays on the Overview page, and each section repeats its own in news-report style.
    p.push('<section class="block reveal ov-b" aria-labelledby="qa-h"><h3 class="h2" id="qa-h">Questions and short answers</h3><p class="cap" style="margin:0 0 12px">Each question with a short answer. Select a question to see the details.</p><div class="qa">' +
      D.qa.concat([{ id: "appx", q: D.appendix.question, a: "A plain-language guide: " + D.appendix.counts.terms + " terms, " + D.appendix.counts.methods + " methods and " + D.appendix.counts.formulas + " formulas, so every figure in this report can be traced and repeated.", src: "PULSE", v: String(D.appendix.counts.formulas), l: "formulas worked through" }]).map(function (q, i) {
        return '<div class="qa-i' + (i === 0 ? " open" : "") + '"><button class="qa-q" aria-expanded="' + (i === 0) + '"><span>' + esc(q.q) + '</span><span class="ic" aria-hidden="true">+</span></button><div class="qa-a"><div><p style="margin:0 0 6px">' + bold(q.a) + '</p><p class="cap">Source: ' + esc(q.src) + '</p><button class="ctl" data-go="' + q.id + '" style="margin-top:8px">Read the section →</button></div><div class="ak"><div class="v">' + esc(q.v) + '</div><div class="l">' + esc(q.l) + "</div></div></div></div>";
      }).join("") + "</div></section>");
    return p.join("");
  }

  function qaBlock(q, extra) {
    return '<section class="qa-news reveal ' + (extra || "") + '" aria-label="Question and short answer"><div class="kick"><span class="nr">Question and short answer</span></div>' +
      '<h3 class="qa-head">' + esc(q.q) + '</h3><div class="lede-grid"><div class="card"><p class="lede">' + bold(q.a) + '</p><p class="cap">Source: ' + esc(q.src) + '</p></div>' +
      '<div class="keynum" tabindex="0"><span class="v">' + esc(q.v) + '</span><span class="l">' + esc(q.l) + "</span></div></div></section>";
  }
  function qaNews(id, question) {
    var q = (D.qa || []).filter(function (x) { return x.id === id; })[0];
    if (!q) return '<p class="question">The question: <b>' + esc(question) + "</b></p>";
    return '<section class="qa-news reveal" aria-label="Question and short answer"><div class="kick"><span class="nr">Question and short answer</span></div>' +
      '<h3 class="qa-head">' + esc(q.q) + '</h3><div class="lede-grid"><div class="card"><p class="lede">' + bold(q.a) + '</p><p class="cap">Source: ' + esc(q.src) + '</p></div>' +
      '<div class="keynum" tabindex="0"><span class="v">' + esc(q.v) + '</span><span class="l">' + esc(q.l) + "</span></div></div></section>";
  }

  function sectionPage(s, idx, total) {
    var p = [];
    p.push('<div class="kick"><span class="nr">News report</span><span class="sn">' + esc(s.tab) + "</span></div>");
    p.push('<h2 class="art-head">' + esc(s.headline) + '</h2><p class="art-dek">' + bold(s.dek) + "</p>");
    p.push('<div class="byline"><span>By Dana Point PULSE · ' + esc(D.prepared) + '</span><span class="chip">' + esc(s.byline_src) + "</span></div>");
    p.push('<div class="lede-grid reveal"><div class="card"><div class="lbl">The lede</div><p class="lede"' + vizSlot("t8", s.lede) + '>' + dateline(s.lede) + '</p><p class="cap">' + esc(s.lede_src) + '</p></div><div class="keynum" data-tip="' + esc(s.key.l) + '" tabindex="0"><span class="v">' + esc(s.key.v) + '</span><span class="l">' + esc(s.key.l) + "</span></div></div>");
    p.push('<div class="mist why reveal"><div class="lbl">Why it matters</div><p>' + bold(s.why) + "</p>" + (s.why_calc ? '<span class="calc-line">Calculation: ' + esc(s.why_calc) + "</span>" : "") + "</div>");
    if (s.glance && s.glance.length) p.push('<div class="block"><h3 class="h2">At a glance</h3><div class="' + (s.glance.length > 1 ? "grid2" : "") + '">' + s.glance.map(ig).join("") + "</div></div>");
    p.push('<div class="block"><h3 class="h2">By the numbers</h3><div class="stats">' + s.numbers.map(function (n) { return '<div class="stat reveal" data-tip="' + esc(n.src) + '" tabindex="0"><span class="k">' + esc(n.k) + '</span><span class="v">' + esc(n.v) + '</span><span class="l">' + esc(n.l) + '</span><span class="p"><span class="' + deltaCls(n.p) + '">' + esc(n.p) + '</span></span><span class="cap" style="margin-top:4px">Source: ' + esc(n.src) + "</span></div>"; }).join("") + "</div></div>");
    p.push(table({ title: "How the key insight is calculated", cols: ["Step", "Calculation", "Result", "Source"], rows: s.calc, num: [], src: "PULSE calculation from the sources named" }));
    var dk = ["who", "what", "when", "where", "why", "how"];
    p.push('<div class="block"><h3 class="h2">The details</h3><p class="cap" style="margin:0 0 12px">Who, what, when, where, why and how, each with its figure, source and date.</p><div class="det">' + dk.map(function (k) { var d = s.details[k]; return '<div class="dc reveal"><span class="k">' + k + '</span><span class="f">' + esc(d.fig) + '</span><span class="fl">' + esc(d.fig_l) + '</span><span class="tx">' + bold(d.text) + '</span><span class="s">Source: ' + esc(d.src) + "</span></div>"; }).join("") + "</div></div>");
    p.push('<div class="band"><span class="bl">Analysis</span><hr></div>' + qaNews(s.id, s.question));
    if (s.map) p.push(map(s.map));
    if (s.charts && s.charts.length) p.push('<div class="block ' + (s.charts.length > 1 ? "grid2" : "") + '">' + s.charts.map(function (c) { return chart(c, s.charts.length === 1); }).join("") + "</div>");
    if (s.waterfall) p.push('<figure class="cf reveal"><h3>' + esc(s.waterfall.title) + '</h3><p class="cs">' + esc(s.waterfall.sub) + '</p><div class="body viz-box"' + vizSlot("waterfall", s.waterfall) + '>' + table({ cols: ["Step", "Room revenue"], rows: s.waterfall.data.map(function (d) { return [d.category, fmt(d.value, "usd")]; }), num: [1] }) + '</div><p class="calc-line">Calculation: ' + esc(s.waterfall.calc) + '</p><p class="cap">Source: ' + esc(s.waterfall.src) + "</p></figure>");
    if (s.calendar) p.push('<figure class="cf reveal"><h3>' + esc(s.calendar.title) + '</h3><p class="cs">' + esc(s.calendar.sub) + '</p><div class="body viz-cal"' + vizSlot("calendar", s.calendar) + '></div><p class="cap">Source: ' + esc(s.calendar.src) + "</p></figure>");
    if (s.heat) p.push(heat(s.heat));
    p.push(table(s.table));
    p.push('<div class="block reveal"><h3 class="h2">What it means for Dana Point</h3><div class="grid2"><div class="means"><h4>What it enhances</h4><ul>' + s.means.enh.map(function (x) { return '<li><span class="otag">' + esc(x.tag) + "</span>" + bold(x.text) + '<span class="src">Source: ' + esc(x.src) + "</span></li>"; }).join("") + '</ul></div><div class="means"><h4>What Dana Point needs</h4><ul>' + s.means.needs.map(function (x) { return '<li><span class="otag">' + esc(x.tag) + "</span>" + bold(x.text) + '<span class="src">Source: ' + esc(x.src) + "</span></li>"; }).join("") + "</ul></div></div></div>");
    p.push('<div class="block reveal"><h3 class="h2">Compare and contrast: findings only</h3><div class="tbl-wrap"><table class="t cc"><thead><tr><th>Type</th><th>Finding</th><th>How to read it</th><th>Source and date</th></tr></thead><tbody>' +
      s.cc.map(function (c) { return '<tr class="cc-' + c.type.toLowerCase().replace(/ /g, "-") + '"><th scope="row">' + esc(c.type) + "</th><td>" + bold(c.point) + (c.calc ? '<span class="calc-line">Calculation: ' + esc(c.calc) + "</span>" : "") + "</td><td>" + esc(c.solution) + '</td><td class="cap" style="margin:0">' + esc(c.src) + "</td></tr>"; }).join("") + "</tbody></table></div></div>");
    if (s.advice && s.advice.length) {
      p.push('<div class="block reveal adv-block"><h3 class="h2">Compare and contrast, with solutions</h3><p class="adv-note"><span class="adv-flag">GloCon Solutions perspective</span> The full compare-and-contrast view with suggested solutions. Points come from the sourced data; solutions are judgment, not data.</p><div class="tbl-wrap"><table class="t cc adv"><thead><tr><th>Type</th><th>Point</th><th>Solution</th><th>Source and date</th></tr></thead><tbody>' +
        s.advice.map(function (c) { return '<tr class="cc-' + c.type.toLowerCase().replace(/ /g, "-") + '"><th scope="row">' + esc(c.type) + "</th><td>" + bold(c.point) + (c.calc ? '<span class="calc-line">Calculation: ' + esc(c.calc) + "</span>" : "") + "</td><td>" + esc(c.action) + '</td><td class="cap" style="margin:0">' + esc(c.src) + "</td></tr>"; }).join("") + "</tbody></table></div></div>");
    }
    var more = (s.more || []).map(function (m) { return m.kind === "table" ? table(m) : chart(m.chart); }).join("");
    var nMore = (s.more || []).length;
    p.push('<details class="more pulse"><summary><span class="plus" aria-hidden="true">+</span><span><span class="mt">More data and notes</span><br><span class="ms">' + nMore + " more " + (nMore === 1 ? "chart or table" : "charts and tables") + ", plus the notes and data caveats behind this section. Don't skip these before you quote a number.</span></span><span class=\"sh\">Show</span></summary><div class=\"in\">" + more + '<div><div class="lbl">Notes</div><ul class="notes">' + (s.notes || []).map(function (n) { return "<li>" + esc(n) + "</li>"; }).join("") + "</ul></div></div></details>");
    var nx = sections[idx + 1];
    if (nx) p.push('<div class="upnext"><div><div class="un-k">Up next · section ' + (idx + 1) + " of " + (total - 1) + '</div><div class="un-h">' + esc(nx.tab) + ' →</div><p class="un-p">' + esc(s.bridge || "") + '</p></div><button data-go="' + nx.id + '">Continue</button></div>');
    return p.join("");
  }

  function appendix() {
    var A = D.appendix, p = [];
    p.push('<div class="kick"><span class="nr">Reference</span><span class="sn">Appendix</span></div><h2 class="art-head">' + esc(A.headline) + '</h2><p class="art-dek">' + esc(A.dek) + "</p>");
    p.push(qaBlock({ q: A.question, a: "A plain-language guide: " + A.counts.terms + " terms, " + A.counts.methods + " methods and " + A.counts.formulas + " formulas, so every figure in this report can be traced and repeated.", src: "PULSE", v: String(A.counts.formulas), l: "formulas worked through" }));
    p.push('<div class="block stats">' + [["Glossary", A.counts.terms, "Terms defined"], ["Methods", A.counts.methods, "Methods explained"], ["Formulas", A.counts.formulas, "Formulas with worked examples"]].map(function (x) { return '<div class="stat"><span class="k">' + x[0] + '</span><span class="v">' + x[1] + '</span><span class="l">' + x[2] + "</span></div>"; }).join("") + "</div>");
    p.push(table({ title: "Formulas", sub: "Each formula with this report's inputs", cols: ["Measure", "Formula", "Worked example"], rows: A.formulas, src: "PULSE calculations from source data" }));
    p.push(table({ title: "Glossary", cols: ["Term", "Definition"], rows: A.glossary, src: "Dana Point PULSE" }));
    p.push(table({ title: "Methods", cols: ["Method", "How each number was produced"], rows: A.methods, src: "Dana Point PULSE" }));
    if (A.pipeline) p.push('<figure class="cf reveal"><h3>How data reaches PULSE</h3><p class="cs">From source files to the board, the report and the weekly PDF</p><div class="body viz-box viz-pipe"' + vizSlot("pipeline", A.pipeline) + '><ol class="pipe-list">' + A.pipeline.columns.map(function (col, ci) { return "<li><b>" + esc(col) + ":</b> " + A.pipeline.nodes.filter(function (n) { return n.col === ci; }).map(function (n) { return esc(n.label); }).join("; ") + "</li>"; }).join("") + '</ol></div><p class="cap">Source: Dana Point PULSE pipeline (CLAUDE.md, Standard Process)</p></figure>');
    if (A.photos && A.photos.length) p.push(table({ title: "Photo credits", sub: "Every photo shown in PULSE, where it appears and where it comes from", cols: ["Photo", "Where it appears", "Source", "Link"], rows: A.photos, src: "Dana Point PULSE; photos from visitdanapoint.com remain Visit Dana Point's" }));
    p.push('<div class="upnext"><div><div class="un-k">Up next</div><div class="un-h">Sources →</div><p class="un-p">The Sources page lists every source with its coverage, period, pull date and how PULSE checked it.</p></div><button data-go="sources">Continue</button></div>');
    return p.join("");
  }
  function sourcesPage() {
    var S = D.sources;
    var sq = qaBlock({ q: "Where does every figure in this report come from?", a: "Every figure traces to a named source in the register below, with its period, and the known data gaps are listed with it.", src: "Dana Point PULSE", v: String((S.register || []).length), l: "sources in the register" });
    return sq + '<div class="kick"><span class="nr">Reference</span><span class="sn">Sources</span></div><h2 class="art-head">Sources</h2><p class="art-dek">Prepared ' + esc(S.prepared) + ". Every figure in this report traces to one of these sources.</p>" +
      (S.graph ? '<figure class="cf reveal"><h3>Which source feeds which section</h3><p class="cs">Each line links a source to a report section that cites it. Drag to explore; hover for names.</p><div class="body viz-box viz-graph"' + vizSlot("graph", S.graph) + '>' + table({ cols: ["Source", "Sections"], rows: S.graph.nodes.filter(function (n) { return n.kind === "source"; }).map(function (n) { return [n.label, S.graph.edges.filter(function (e) { return e.source === n.id; }).map(function (e) { var t = S.graph.nodes.filter(function (m) { return m.id === e.target; })[0]; return t ? t.label : ""; }).join(", ")]; }) }) + '</div><p class="cap">Source: Dana Point PULSE, from each section\'s source lines</p></figure>' : "") +
      table({ title: "Source register", cols: S.cols, rows: S.register, src: "Dana Point PULSE" }) +
      '<div class="block mist"><div class="lbl">Known data gaps</div><ul class="notes">' + S.recs.map(function (r) { return "<li>" + esc(r) + "</li>"; }).join("") + "</ul></div>" +
      ((S.recs_advice && S.recs_advice.length) ? '<div class="block mist adv-block"><div class="lbl">Recommendations for the data</div><p class="adv-note"><span class="adv-flag">GloCon Solutions perspective</span> Steps that would close the gaps listed above.</p><ul class="notes">' + S.recs_advice.map(function (r) { return "<li>" + esc(r) + "</li>"; }).join("") + "</ul></div>" : "") +
      '<div class="block card"><div class="lbl">Licensed data</div><p style="margin:0">STR, CoStar and Datafy are licensed. This report shares summaries, not raw exports.</p></div>';
  }

  var html = [];
  sections.forEach(function (s, i) {
    var body;
    if (s.id === "overview") body = overview();
    else if (s.id === "appx") body = appendix();
    else if (s.id === "sources") body = sourcesPage();
    else body = sectionPage(D.sections.filter(function (x) { return x.id === s.id; })[0], i, sections.length - 2);
    html.push('<section class="page" id="' + s.id + '" aria-label="' + esc(s.tab) + '">' + body + "</section>");
  });
  pagesEl.innerHTML = html.join("");

  /* ---------- motion ---------- */
  function grow(root) {
    root.querySelectorAll("[data-w]").forEach(function (e) { e.style.width = e.getAttribute("data-w") + "%"; });
    root.querySelectorAll("[data-h]").forEach(function (e) { e.style.height = e.getAttribute("data-h") + "%"; });
  }
  if (!RM && "IntersectionObserver" in window) {
    document.body.classList.add("anim");
    var io = new IntersectionObserver(function (es) { es.forEach(function (e) { if (e.isIntersecting) { e.target.classList.add("in"); grow(e.target); io.unobserve(e.target); } }); }, { threshold: 0.12 });
    document.querySelectorAll(".reveal").forEach(function (e) { io.observe(e); });
    setTimeout(function () { grow(document); }, 60);
  } else { grow(document); }

  /* ---------- navigation ---------- */
  var tabs = document.querySelectorAll(".tab");
  function idxOf(id) { for (var i = 0; i < sections.length; i++) if (sections[i].id === id) return i; return 0; }
  function show(id, scroll) {
    state.cur = id;
    tabs.forEach(function (t) { t.setAttribute("aria-current", t.getAttribute("data-s") === id ? "true" : "false"); });
    var on = document.querySelector('.tab[aria-current="true"]'), tb = document.getElementById("tabs"); if (on && tb) tb.scrollLeft = Math.max(on.offsetLeft - tb.clientWidth / 2 + on.offsetWidth / 2, 0);
    document.querySelectorAll(".page").forEach(function (p) { p.classList.toggle("on", p.id === id); });
    var i = idxOf(id);
    document.getElementById("pl").textContent = "Page " + (i + 1) + " of " + sections.length + " · " + sections[i].tab;
    document.getElementById("prev").disabled = i === 0; document.getElementById("next").disabled = i === sections.length - 1;
    try { if (history.replaceState) history.replaceState(null, "", "#" + id); } catch (e) { /* blocked inside the app frame; harmless */ }
    var pg = document.getElementById(id);
    if (pg) grow(pg);
    if (scroll !== false) { var tgt = state.all ? pg : document.getElementById("main"); if (tgt) tgt.scrollIntoView({ behavior: RM ? "auto" : "smooth", block: "start" }); }
  }
  tabs.forEach(function (t) { t.addEventListener("click", function () { show(t.getAttribute("data-s")); }); });
  document.getElementById("prev").onclick = function () { show(sections[Math.max(idxOf(state.cur) - 1, 0)].id); };
  document.getElementById("next").onclick = function () { show(sections[Math.min(idxOf(state.cur) + 1, sections.length - 1)].id); };
  function setAll(v) {
    state.all = v; document.body.classList.toggle("all", v);
    document.getElementById("one").setAttribute("aria-pressed", String(!v)); document.getElementById("allp").setAttribute("aria-pressed", String(v));
    grow(document);
  }
  document.getElementById("one").onclick = function () { setAll(false); show(state.cur); };
  document.getElementById("allp").onclick = function () { setAll(true); show(state.cur); };
  document.getElementById("fs").onclick = function () {
    var de = document.documentElement;
    if (!document.fullscreenElement && de.requestFullscreen) { de.requestFullscreen().then(function () { document.body.classList.add("fs"); }).catch(function () { toast("Full screen is not available in this frame. Use Download the full report, then open it in your browser."); }); }
    else if (document.exitFullscreen) { document.exitFullscreen(); document.body.classList.remove("fs"); }
  };
  document.addEventListener("fullscreenchange", function () { document.getElementById("fs").textContent = document.fullscreenElement ? "Exit full screen" : "Full screen"; });
  document.getElementById("totop").onclick = function () { window.scrollTo({ top: 0, behavior: RM ? "auto" : "smooth" }); };
  app.addEventListener("click", function (e) {
    var g = e.target.closest("[data-go]"); if (g) { e.preventDefault(); show(g.getAttribute("data-go")); return; }
    var q = e.target.closest(".qa-q"); if (q) { var it = q.parentNode; it.classList.toggle("open"); q.setAttribute("aria-expanded", it.classList.contains("open")); }
  });
  document.querySelectorAll("details.more").forEach(function (d) { d.addEventListener("toggle", function () { d.querySelector(".sh").textContent = d.open ? "Hide" : "Show"; d.classList.remove("pulse"); }); });
  document.addEventListener("keydown", function (e) {
    if (e.target.closest && e.target.closest("input,textarea,select,dialog")) return;
    if (e.key === "ArrowRight" && e.altKey) document.getElementById("next").click();
    if (e.key === "ArrowLeft" && e.altKey) document.getElementById("prev").click();
  });
  var start = (location.hash || "").replace("#", "");
  show(sections.some(function (x) { return x.id === start; }) ? start : sections[0].id, false);

  /* ---------- tooltips and highlight ---------- */
  var tip = document.getElementById("vtip");
  function hot(t, on) {
    t.classList.toggle("is-hot", on);
    var grp = t.closest(".hbars, svg.vz");
    if (grp) grp.classList.toggle("hovering", on);
  }
  function showTip(t, x, y) { tip.textContent = t.getAttribute("data-tip"); tip.hidden = false; var r = tip.getBoundingClientRect(); tip.style.left = Math.min(x + 14, window.innerWidth - r.width - 8) + "px"; tip.style.top = Math.max(y - r.height - 12, 8) + "px"; }
  app.addEventListener("mouseover", function (e) { var t = e.target.closest("[data-tip]"); if (!t) return; hot(t, true); showTip(t, e.clientX, e.clientY); });
  app.addEventListener("mousemove", function (e) { var t = e.target.closest("[data-tip]"); if (t && !tip.hidden) showTip(t, e.clientX, e.clientY); });
  app.addEventListener("mouseout", function (e) { var t = e.target.closest("[data-tip]"); if (!t) return; hot(t, false); tip.hidden = true; });
  app.addEventListener("focusin", function (e) { var t = e.target.closest("[data-tip]"); if (!t) return; var r = t.getBoundingClientRect(); hot(t, true); showTip(t, r.left + r.width / 2, r.top); });
  app.addEventListener("focusout", function (e) { var t = e.target.closest("[data-tip]"); if (!t) return; hot(t, false); tip.hidden = true; });

  /* ---------- copy the announcement ---------- */
  function copy(rich) {
    var paper = document.getElementById("paper");
    var txt = "DRAFT: requires approval before release.\n\n" + paper.innerText;
    var htm = "<p><b>DRAFT: requires approval before release.</b></p>" + paper.outerHTML;
    if (rich && window.ClipboardItem && navigator.clipboard && navigator.clipboard.write) {
      navigator.clipboard.write([new ClipboardItem({ "text/html": new Blob([htm], { type: "text/html" }), "text/plain": new Blob([txt], { type: "text/plain" }) })]).then(function () { toast("Copied with formatting"); }, function () { fallback(txt); });
    } else if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(txt).then(function () { toast("Copied"); }, function () { fallback(txt); }); }
    else fallback(txt);
  }
  function fallback(txt) { var ta = document.createElement("textarea"); ta.value = txt; document.body.appendChild(ta); ta.select(); try { document.execCommand("copy"); toast("Copied"); } catch (e) { toast("Copy is blocked here; select the text and copy it."); } ta.remove(); }
  var cpt = document.getElementById("cpt"), cpf = document.getElementById("cpf");
  if (cpt) cpt.onclick = function () { copy(false); };
  if (cpf) cpf.onclick = function () { copy(true); };

  /* ---------- export ---------- */
  var dlg = el("dialog", "exp");
  dlg.setAttribute("aria-labelledby", "exp-h");
  dlg.innerHTML = '<form method="dialog" id="expf"><h3 id="exp-h">Export</h3><p class="cap" style="margin:0 0 14px">Choose sections, then a format.</p>' +
    '<fieldset><legend>Format</legend><div class="fmt">' + [["pdf", "PDF"], ["pptx", "PowerPoint"], ["xlsx", "Excel"], ["csv", "CSV"], ["link", "Share link"]].map(function (f, i) { return '<label><input type="radio" name="fmt" value="' + f[0] + '"' + (i ? "" : " checked") + "> " + f[1] + "</label>"; }).join("") + "</div></fieldset>" +
    '<fieldset><legend>Sections</legend><div class="exp-quick"><button type="button" class="ctl" id="xa">Select all</button><button type="button" class="ctl" id="xc">Clear</button><button type="button" class="ctl" id="xcur">Current section only</button></div><div class="exp-list">' +
    sections.map(function (s) { return '<label class="exp-item"><input type="checkbox" name="sec" value="' + s.id + '" checked> ' + esc(s.tab) + "</label>"; }).join("") + '</div><p class="cap" id="xcount"></p></fieldset>' +
    '<div class="exp-acts"><button type="button" class="ctl" id="xcancel">Cancel</button><button type="button" class="ctl primary" id="xgo">Download PDF</button></div></form>';
  document.body.appendChild(dlg);
  function chosen() { return Array.prototype.map.call(dlg.querySelectorAll('input[name=sec]:checked'), function (i) { return i.value; }); }
  function fmtSel() { return dlg.querySelector('input[name=fmt]:checked').value; }
  function sync() {
    var n = chosen().length, f = fmtSel();
    document.getElementById("xcount").textContent = n + " of " + sections.length + " sections selected";
    var g = document.getElementById("xgo");
    g.textContent = { pdf: "Download PDF", pptx: "Download PowerPoint", xlsx: "Download Excel", csv: "Download CSV", link: "Copy link" }[f];
    g.disabled = n === 0;
  }
  dlg.addEventListener("change", sync);
  document.getElementById("exp").onclick = function () { sync(); if (dlg.showModal) dlg.showModal(); else dlg.setAttribute("open", ""); };
  document.getElementById("xcancel").onclick = function () { dlg.close(); };
  document.getElementById("xa").onclick = function () { dlg.querySelectorAll("input[name=sec]").forEach(function (i) { i.checked = true; }); sync(); };
  document.getElementById("xc").onclick = function () { dlg.querySelectorAll("input[name=sec]").forEach(function (i) { i.checked = false; }); sync(); };
  document.getElementById("xcur").onclick = function () { dlg.querySelectorAll("input[name=sec]").forEach(function (i) { i.checked = i.value === state.cur; }); sync(); };
  function save(blob, name) { var a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = name; document.body.appendChild(a); a.click(); setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1500); }
  var stamp = (D.prepared || "").replace(/[^A-Za-z0-9]+/g, "-");
  document.getElementById("xgo").onclick = function () {
    var ids = chosen(), f = fmtSel();
    if (!ids.length) return;
    dlg.close();
    if (f === "pdf") {
      document.querySelectorAll(".page").forEach(function (p) { p.classList.toggle("pr-hide", ids.indexOf(p.id) < 0); });
      var opened = []; document.querySelectorAll("details.more").forEach(function (d) { if (!d.open) { d.open = true; opened.push(d); } });
      grow(document); document.querySelectorAll(".reveal").forEach(function (r) { r.classList.add("in"); });
      setTimeout(function () { window.print(); opened.forEach(function (d) { d.open = false; }); document.querySelectorAll(".pr-hide").forEach(function (p) { p.classList.remove("pr-hide"); }); }, 120);
    } else if (f === "csv") {
      var parts = ids.map(function (i, k) { var c = (D.csvBySection || {})[i] || ""; return k ? c.split("\n").slice(1).join("\n") : c; });
      save(new Blob(["\ufeff" + parts.join("")], { type: "text/csv" }), "Dana-Point-PULSE-Insights-" + stamp + ".csv");
    } else if (f === "xlsx") { xlsx(ids); }
    else if (f === "pptx") { pptx(ids); }
    else if (f === "link") {
      var url = D.publicUrl + (ids.length === 1 ? "#" + ids[0] : "");
      if (navigator.share) navigator.share({ title: D.title, url: url }).catch(function () {});
      if (navigator.clipboard) navigator.clipboard.writeText(url).then(function () { toast("Link copied"); }, function () { fallback(url); }); else fallback(url);
    }
  };

  // Excel: a real .xlsx, About sheet plus one sheet per section, numbers stored as numbers
  function xmlEsc(s) { return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); }
  function parseNum(s) {
    if (typeof s !== "string") return null;
    var t = s.replace(/[▲▼▶]/g, "").replace("−", "-").trim();
    if (/^[+-]?[\d,]+(\.\d+)?%$/.test(t)) return { v: parseFloat(t.replace(/[,%]/g, "")) / 100, s: 1 };
    if (/^\$[\d,]+(\.\d+)?$/.test(t)) return { v: parseFloat(t.replace(/[$,]/g, "")), s: 2 };
    if (/^[+-]?[\d,]+(\.\d+)?$/.test(t)) return { v: parseFloat(t.replace(/,/g, "")), s: 3 };
    return null;
  }
  function colName(n) { var s = ""; n++; while (n > 0) { var m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); } return s; }
  function sheetXml(rows) {
    var out = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cols><col min="1" max="1" width="38" customWidth="1"/><col min="2" max="8" width="24" customWidth="1"/></cols><sheetData>'];
    rows.forEach(function (r, i) {
      out.push('<row r="' + (i + 1) + '">');
      r.cells.forEach(function (v, j) {
        var ref = colName(j) + (i + 1), p = r.raw ? null : parseNum(v), st = r.style || 0;
        if (p) out.push('<c r="' + ref + '" s="' + (p.s + 3) + '"><v>' + p.v + "</v></c>");
        else if (v !== "" && v != null) out.push('<c r="' + ref + '" t="inlineStr" s="' + st + '"><is><t xml:space="preserve">' + xmlEsc(v) + "</t></is></c>");
      });
      out.push("</row>");
    });
    out.push("</sheetData></worksheet>");
    return out.join("");
  }
  function xlsx(ids) {
    if (!window.JSZip) { toast("Excel export is unavailable here."); return; }
    var z = new JSZip(), names = [];
    var about = [{ cells: [D.title], style: 1, raw: true }, { cells: [""], raw: true }, { cells: ["Prepared", D.prepared], raw: true }, { cells: ["Period", D.period], raw: true }, { cells: ["Detail", D.period_detail], raw: true }, { cells: ["Prepared by", "GloCon Solutions LLC for Visit Dana Point"] }, { cells: ["Contact", "John Picou, GloCon Solutions LLC, john.picou@gloconsolutions.com"], raw: true }, { cells: ["Note", "Licensed data (STR, CoStar, Datafy) appears as summary tables only."], raw: true }];
    var sheets = [["About", about]];
    ids.forEach(function (id) {
      var nm = sections[idxOf(id)].tab.slice(0, 31).replace(/[\\\/?*\[\]:]/g, "-"), rows = [{ cells: [sections[idxOf(id)].tab], style: 1, raw: true }];
      ((D.tablesBySection || {})[id] || []).forEach(function (t) {
        rows.push({ cells: [""], raw: true }); rows.push({ cells: [t.title], style: 2, raw: true }); rows.push({ cells: t.cols, style: 2, raw: true });
        t.rows.forEach(function (r) { rows.push({ cells: r }); });
        rows.push({ cells: ["Source: " + t.src], style: 3, raw: true });
      });
      sheets.push([nm, rows]);
    });
    sheets.forEach(function (s, i) { names.push(s[0]); z.file("xl/worksheets/sheet" + (i + 1) + ".xml", sheetXml(s[1])); });
    z.file("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>' + names.map(function (n, i) { return '<Override PartName="/xl/worksheets/sheet' + (i + 1) + '.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'; }).join("") + "</Types>");
    z.file("_rels/.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>');
    z.file("xl/workbook.xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>' + names.map(function (n, i) { return '<sheet name="' + xmlEsc(n) + '" sheetId="' + (i + 1) + '" r:id="rId' + (i + 1) + '"/>'; }).join("") + "</sheets></workbook>");
    z.file("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + names.map(function (n, i) { return '<Relationship Id="rId' + (i + 1) + '" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet' + (i + 1) + '.xml"/>'; }).join("") + '<Relationship Id="rId' + (names.length + 1) + '" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>');
    z.file("xl/styles.xml", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><numFmts count="2"><numFmt numFmtId="164" formatCode="0.0%"/><numFmt numFmtId="165" formatCode="&quot;$&quot;#,##0"/></numFmts><fonts count="4"><font><sz val="11"/><name val="Calibri"/></font><font><b/><i/><sz val="16"/><color rgb="FF0B2530"/><name val="Georgia"/></font><font><b/><sz val="11"/><color rgb="FF1D6E86"/><name val="Calibri"/></font><font><i/><sz val="9"/><color rgb="FF64748B"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="7"><xf/><xf fontId="1" applyFont="1"/><xf fontId="2" applyFont="1"/><xf fontId="3" applyFont="1"/><xf numFmtId="164" applyNumberFormat="1"/><xf numFmtId="165" applyNumberFormat="1"/><xf numFmtId="3" applyNumberFormat="1"/></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>');
    z.generateAsync({ type: "blob", mimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" }).then(function (b) { save(b, "Dana-Point-PULSE-Insights-" + stamp + ".xlsx"); });
  }

  // PowerPoint: keep the title slide plus the chosen sections from the full editable deck
  function pptx(ids) {
    if (!D.deck) { toast("The PowerPoint deck is not available in this build."); return; }
    var bin = atob(D.deck), bytes = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    var all = sections.every(function (s) { return ids.indexOf(s.id) >= 0; });
    if (all || !window.JSZip) { save(new Blob([bytes], { type: "application/vnd.openxmlformats-officedocument.presentationml.presentation" }), "Dana-Point-PULSE-Insights-" + stamp + ".pptx"); return; }
    var keep = { 1: true };
    ids.forEach(function (id) { var r = (D.deckMap || {})[id]; if (r) for (var k = r[0]; k <= r[1]; k++) keep[k] = true; });
    JSZip.loadAsync(bytes).then(function (z) {
      return Promise.all([z.file("ppt/presentation.xml").async("string"), z.file("ppt/_rels/presentation.xml.rels").async("string"), z.file("[Content_Types].xml").async("string")]).then(function (x) {
        var pres = x[0], rels = x[1], ct = x[2], ridToSlide = {}, drop = [];
        rels.replace(/<Relationship [^>]*Id="(rId\d+)"[^>]*Target="slides\/slide(\d+)\.xml"[^>]*\/>/g, function (m, rid, n) { ridToSlide[rid] = +n; return m; });
        rels.replace(/<Relationship [^>]*Target="slides\/slide(\d+)\.xml"[^>]*Id="(rId\d+)"[^>]*\/>/g, function (m, n, rid) { ridToSlide[rid] = +n; return m; });
        pres = pres.replace(/<p:sldId [^>]*r:id="(rId\d+)"[^>]*\/>/g, function (m, rid) { var n = ridToSlide[rid]; if (n && !keep[n]) { drop.push(n); return ""; } return m; });
        var dropRid = Object.keys(ridToSlide).filter(function (r) { return drop.indexOf(ridToSlide[r]) >= 0; });
        dropRid.forEach(function (r) { rels = rels.replace(new RegExp('<Relationship [^>]*Id="' + r + '"[^>]*/>'), ""); });
        var chartDrops = [];
        return Promise.all(drop.map(function (n) {
          var rf = z.file("ppt/slides/_rels/slide" + n + ".xml.rels");
          return (rf ? rf.async("string") : Promise.resolve("")).then(function (sr) {
            sr.replace(/Target="\.\.\/charts\/(chart\d+)\.xml"/g, function (m, c) { chartDrops.push(c); return m; });
            z.remove("ppt/slides/slide" + n + ".xml"); z.remove("ppt/slides/_rels/slide" + n + ".xml.rels");
            ct = ct.replace(new RegExp('<Override PartName="/ppt/slides/slide' + n + '\\.xml"[^>]*/>'), "");
          });
        })).then(function () {
          return Promise.all(chartDrops.map(function (c) {
            var cr = z.file("ppt/charts/_rels/" + c + ".xml.rels");
            return (cr ? cr.async("string") : Promise.resolve("")).then(function (s) {
              s.replace(/Target="\.\.\/embeddings\/([^"]+)"/g, function (m, e) { z.remove("ppt/embeddings/" + e); return m; });
              z.remove("ppt/charts/" + c + ".xml"); z.remove("ppt/charts/_rels/" + c + ".xml.rels");
              ct = ct.replace(new RegExp('<Override PartName="/ppt/charts/' + c + '\\.xml"[^>]*/>'), "");
            });
          }));
        }).then(function () {
          z.file("ppt/presentation.xml", pres); z.file("ppt/_rels/presentation.xml.rels", rels); z.file("[Content_Types].xml", ct);
          return z.generateAsync({ type: "blob", mimeType: "application/vnd.openxmlformats-officedocument.presentationml.presentation" });
        });
      });
    }).then(function (b) { save(b, "Dana-Point-PULSE-Insights-" + stamp + ".pptx"); }).catch(function () { toast("PowerPoint subset failed; downloading the full deck."); save(new Blob([bytes]), "Dana-Point-PULSE-Insights-" + stamp + ".pptx"); });
  }
})();
