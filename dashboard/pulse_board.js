// pulse_board.js: the Dana Point PULSE board view, drawn by hand.
//
// Streamlit mounts this as an st.components.v2 component. Python builds every
// number, label, and sentence (pulse_data.py and pulse_story.py); this file
// only draws them. Charts are plain SVG and HTML sized to the real width of
// their card, so text stays the same size on a phone and on a wide monitor,
// and every chart has a hover (or tap) layer. No external libraries.

const C = {
  ink: '#0B2530', ink2: '#334155', ink3: '#64748B', ink4: '#94A3B8',
  teal: '#1D6E86', tealDk: '#123C4A', tealLt: '#8FC4D6', tealText: '#0E7490',
  maroon: '#A8461F', sand: '#9C9186', grid: '#E6ECEF', mist: '#F0F7F9', white: '#FFFFFF',
};
const COLOR = { cur: C.teal, prior: C.sand, dp: C.maroon, teal: C.teal, maroon: C.maroon, sand: C.sand, tealDk: C.tealDk, tealLt: C.tealLt };
const REDUCE = typeof window !== 'undefined' && window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
  { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Bold the figures inside a written answer so the eye can scan them.
const emph = (s) => esc(s).replace(
  /([+\-−]?\$[\d,]+(?:\.\d+)?[KMB]?|[+\-−]?\d+(?:\.\d+)?%|[+\-−]?\d+(?:\.\d+)?\s?pts)/g, '<b>$1</b>');

const isNum = (v) => v !== null && v !== undefined && isFinite(v);

const FMT = {
  pct1: (v) => (isNum(v) ? v.toFixed(1) + '%' : 'n/a'),
  pct0: (v) => (isNum(v) ? Math.round(v) + '%' : 'n/a'),
  usd0: (v) => (isNum(v) ? '$' + Math.round(v).toLocaleString('en-US') : 'n/a'),
  usd2: (v) => (isNum(v) ? '$' + v.toFixed(2) : 'n/a'),
  num0: (v) => (isNum(v) ? Math.round(v).toLocaleString('en-US') : 'n/a'),
  usdbig: (v) => {
    if (!isNum(v)) return 'n/a';
    const a = Math.abs(v);
    if (a >= 1e9) return '$' + (v / 1e9).toFixed(1) + 'B';
    if (a >= 1e6) return '$' + (v / 1e6).toFixed(1) + 'M';
    if (a >= 1e3) return '$' + Math.round(v / 1e3) + 'K';
    return '$' + Math.round(v);
  },
  axUsd: (v) => {
    const a = Math.abs(v);
    if (a >= 1e6) return '$' + (v / 1e6).toFixed(a >= 1e7 ? 0 : 1).replace(/\.0$/, '') + 'M';
    if (a >= 1e3) return '$' + Math.round(v / 1e3) + 'K';
    return '$' + Math.round(v);
  },
};
const fmtOf = (k) => FMT[k] || FMT.num0;
const axisFmt = (k) => (k === 'pct1' || k === 'pct0' ? (v) => Math.round(v) + '%'
  : k === 'usdbig' ? FMT.axUsd : k === 'usd0' || k === 'usd2' ? FMT.axUsd : FMT.num0);

function changeText(kind, cur, prior) {
  if (!isNum(cur) || !isNum(prior)) return '';
  if (kind === 'pct1' || kind === 'pct0') {
    const d = cur - prior;
    return (d >= 0 ? '+' : '') + d.toFixed(1) + ' pts';
  }
  if (!prior) return '';
  const d = (cur / prior - 1) * 100;
  return (d >= 0 ? '+' : '') + d.toFixed(1) + '%';
}

function niceTicks(lo, hi, count) {
  if (!isNum(lo) || !isNum(hi)) return [0, 1];
  if (lo === hi) { lo -= 1; hi += 1; }
  const step0 = (hi - lo) / Math.max(1, count);
  const mag = Math.pow(10, Math.floor(Math.log10(step0)));
  const r = step0 / mag;
  const step = mag * (r >= 7.5 ? 10 : r >= 3.5 ? 5 : r >= 1.5 ? 2 : 1);
  const start = Math.floor(lo / step) * step;
  const end = Math.ceil(hi / step) * step;
  const out = [];
  for (let v = start; v <= end + step * 1e-9; v += step) out.push(+v.toFixed(10));
  return out;
}

// Category ticks for trends: at most maxN labels, with a month's year printed
// only when it changes ("Sep / 2025", "Nov", "Jan / 2026").
function xTicks(labels, maxN) {
  const n = labels.length;
  const step = Math.max(1, Math.ceil(n / Math.max(1, maxN)));
  const out = [];
  let prevYear = null;
  for (let i = 0; i < n; i += step) {
    const parts = String(labels[i]).split(' ');
    let a = String(labels[i]);
    let b = '';
    if (parts.length === 2 && /^\d{4}$/.test(parts[1])) {
      a = parts[0];
      b = parts[1] === prevYear ? '' : parts[1];
      prevYear = parts[1];
    }
    out.push({ i, a, b });
  }
  return out;
}

function roundTopRect(x, y, w, h, r) {
  if (h <= 0 || w <= 0) return '';
  r = Math.max(0, Math.min(r, w / 2, h));
  const y0 = y + h;
  return `M${x},${y0}L${x},${y + r}Q${x},${y} ${x + r},${y}L${x + w - r},${y}Q${x + w},${y} ${x + w},${y + r}L${x + w},${y0}Z`;
}

function linePath(pts) {
  let d = '';
  let pen = false;
  pts.forEach((p) => {
    if (!p) { pen = false; return; }
    d += (pen ? 'L' : 'M') + p[0].toFixed(1) + ',' + p[1].toFixed(1);
    pen = true;
  });
  return d;
}

function deltaChip(d, dark) {
  if (!d) return '';
  const tone = d.tone || 'none';
  const arrow = tone === 'up' ? '▲' : tone === 'down' ? '▼' : tone === 'flat' ? '●' : '';
  return `<span class="pb-delta ${tone}${dark ? ' dark' : ''}">${arrow ? `<i>${arrow}</i>` : ''}${esc(d.text)}</span>`;
}

function spark(values, w, h, color, area) {
  const v = (values || []).filter(isNum);
  if (v.length < 3) return '';
  const lo = Math.min(...v); const hi = Math.max(...v); const rng = hi - lo || 1;
  const pts = v.map((x, i) => [2 + (i * (w - 4)) / (v.length - 1), h - 3 - ((x - lo) / rng) * (h - 6)]);
  const d = linePath(pts);
  const last = pts[pts.length - 1];
  const fill = area ? `<path d="${d}L${last[0]},${h}L${pts[0][0]},${h}Z" fill="${color}" opacity=".16"/>` : '';
  return `<svg class="pb-spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true">${fill}` +
    `<path d="${d}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"/>` +
    `</svg>`;
}

// ---------------------------------------------------------------------------
// Tooltip, one per board chunk
// ---------------------------------------------------------------------------

function makeTip(root) {
  const tip = document.createElement('div');
  tip.className = 'pb-tip';
  root.appendChild(tip);
  return {
    show(ev, html) {
      tip.innerHTML = html;
      tip.style.opacity = '1';
      const rr = root.getBoundingClientRect();
      const tw = tip.offsetWidth; const th = tip.offsetHeight;
      let x = ev.clientX - rr.left + 16;
      let y = ev.clientY - rr.top - th - 12;
      if (x + tw > rr.width - 6) x = ev.clientX - rr.left - tw - 16;
      if (x < 6) x = 6;
      if (y < 6) y = ev.clientY - rr.top + 18;
      tip.style.transform = `translate(${Math.round(x)}px, ${Math.round(y)}px)`;
    },
    hide() { tip.style.opacity = '0'; },
  };
}

const tipRows = (title, rows) => `<div class="pb-tip-t">${esc(title)}</div>` + rows.map((r) =>
  `<div class="pb-tip-r">${r.color ? `<span class="sw" style="background:${r.color}${r.dash ? ';opacity:.7' : ''}"></span>` : ''}` +
  `<span class="k">${esc(r.k)}</span><span class="v">${esc(r.v)}</span></div>`).join('');

function legendHTML(items) {
  if (!items || items.length < 2) return '';
  return '<div class="pb-legend top">' + items.map((it) => `<span><i class="${it.dash ? 'dash' : ''}" style="background:${it.color}"></i>${esc(it.name)}</span>`).join('') + '</div>';
}

// ---------------------------------------------------------------------------
// Charts
// ---------------------------------------------------------------------------

function empty(msg) { return `<div class="pb-empty">${esc(msg || 'Not available for this window yet.')}</div>`; }

function animateLines(host, anim) {
  if (!anim || REDUCE) return;
  host.querySelectorAll('path.pb-draw').forEach((p) => {
    try {
      const L = p.getTotalLength();
      p.style.strokeDasharray = `${L}`;
      p.style.strokeDashoffset = `${L}`;
      p.getBoundingClientRect();
      p.style.transition = 'stroke-dashoffset 1100ms cubic-bezier(.2,.7,.2,1)';
      p.style.strokeDashoffset = '0';
      setTimeout(() => { p.style.strokeDasharray = ''; }, 1200);
    } catch (e) { /* no-op */ }
  });
  host.querySelectorAll('.pb-fade').forEach((el, i) => {
    el.style.opacity = '0';
    el.getBoundingClientRect();
    el.style.transition = `opacity 600ms ease ${300 + i * 20}ms`;
    el.style.opacity = '1';
  });
}

function animateBars(host, anim) {
  if (!anim || REDUCE) return;
  host.querySelectorAll('.pb-grow').forEach((el, i) => {
    el.style.transform = 'scaleY(0)';
    el.getBoundingClientRect();
    el.style.transition = `transform 700ms cubic-bezier(.2,.7,.2,1) ${Math.min(i * 25, 400)}ms`;
    el.style.transform = 'scaleY(1)';
  });
}

// Line chart: any number of series over shared categories, one y axis.
function lineChart(host, w, anim, spec, tip) {
  const labels = spec.labels || [];
  const series = (spec.series || []).filter((s) => (s.values || []).some(isNum));
  const H = spec.height || (w < 480 ? 230 : 260);
  if (!labels.length || !series.length) { host.innerHTML = empty(); return; }
  const m = { l: 48, r: 18, t: 16, b: 42 };
  const iw = Math.max(20, w - m.l - m.r); const ih = H - m.t - m.b;
  const all = series.flatMap((s) => s.values).filter(isNum);
  let lo = Math.min(...all); let hi = Math.max(...all);
  const pad = (hi - lo) * 0.08 || 1;
  const ticks = niceTicks(lo - pad, hi + pad, w < 480 ? 3 : 4);
  lo = ticks[0]; hi = ticks[ticks.length - 1];
  const n = labels.length;
  const X = (i) => m.l + (n <= 1 ? iw / 2 : (i * iw) / (n - 1));
  const Y = (v) => m.t + ih - ((v - lo) / (hi - lo)) * ih;
  const yf = axisFmt(spec.fmt);
  const vf = fmtOf(spec.fmt);
  const gid = 'g' + Math.random().toString(36).slice(2, 8);
  let s = `<svg width="${w}" height="${H}" viewBox="0 0 ${w} ${H}" role="img" aria-label="${esc(spec.aria || '')}">`;
  s += `<defs><linearGradient id="${gid}" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="${C.teal}" stop-opacity=".20"/><stop offset="1" stop-color="${C.teal}" stop-opacity="0"/></linearGradient></defs>`;
  ticks.forEach((t) => {
    s += `<line x1="${m.l}" x2="${w - m.r}" y1="${Y(t)}" y2="${Y(t)}" class="pb-gl"/>`;
    s += `<text x="${m.l - 10}" y="${Y(t) + 4}" class="pb-yl" text-anchor="end">${esc(yf(t))}</text>`;
  });
  xTicks(labels, Math.max(2, Math.floor(iw / 58))).forEach((t) => {
    s += `<text x="${X(t.i)}" y="${H - m.b + 18}" class="pb-xl" text-anchor="middle">${esc(t.a)}` +
      (t.b ? `<tspan x="${X(t.i)}" dy="13" class="pb-xl2">${esc(t.b)}</tspan>` : '') + '</text>';
  });
  series.forEach((sr, k) => {
    const pts = sr.values.map((v, i) => (isNum(v) ? [X(i), Y(v)] : null));
    const col = COLOR[sr.color] || sr.color || C.teal;
    if (sr.area) {
      const valid = pts.filter(Boolean);
      if (valid.length > 1) {
        s += `<path d="${linePath(pts)}L${valid[valid.length - 1][0]},${m.t + ih}L${valid[0][0]},${m.t + ih}Z" fill="url(#${gid})" class="pb-fade"/>`;
      }
    }
    s += `<path d="${linePath(pts)}" fill="none" stroke="${col}" stroke-width="${sr.width || 2.4}" stroke-linejoin="round" stroke-linecap="round"` +
      (sr.dash ? ` stroke-dasharray="5 5" class="pb-fade"` : ' class="pb-draw"') + '/>';
    if (sr.dots) {
      pts.forEach((p) => { if (p) s += `<circle cx="${p[0]}" cy="${p[1]}" r="3.4" fill="${col}" stroke="#fff" stroke-width="1.6" class="pb-fade"/>`; });
    }
    if (sr.endLabel) {
      let li = -1; sr.values.forEach((v, i) => { if (isNum(v)) li = i; });
      if (li >= 0) {
        const px = X(li); const py = Y(sr.values[li]);
        const anchor = px > w - 60 ? 'end' : 'middle';
        s += `<text x="${anchor === 'end' ? px + 4 : px}" y="${py - 11}" class="pb-endl" text-anchor="${anchor}" fill="${col}">${esc(vf(sr.values[li]))}</text>`;
      }
    }
  });
  s += `<line class="pb-cross" x1="0" x2="0" y1="${m.t}" y2="${m.t + ih}" opacity="0"/>`;
  s += `<g class="pb-hdots"></g>`;
  s += `<rect class="pb-hit" x="${m.l - 8}" y="${m.t}" width="${iw + 16}" height="${ih}" fill="transparent"/>`;
  s += '</svg>';
  host.innerHTML = legendHTML(series.map((sr) => ({ name: sr.name, color: COLOR[sr.color] || sr.color || C.teal, dash: sr.dash }))) + s;
  const svg = host.querySelector('svg');
  const cross = svg.querySelector('.pb-cross');
  const hd = svg.querySelector('.pb-hdots');
  const hit = svg.querySelector('.pb-hit');
  const move = (ev) => {
    const r = svg.getBoundingClientRect();
    const px = ev.clientX - r.left;
    const i = Math.max(0, Math.min(n - 1, Math.round(((px - m.l) / iw) * (n - 1))));
    cross.setAttribute('x1', X(i)); cross.setAttribute('x2', X(i)); cross.setAttribute('opacity', '1');
    let dots = '';
    const rows = [];
    series.forEach((sr) => {
      const v = sr.values[i];
      const col = COLOR[sr.color] || sr.color || C.teal;
      if (isNum(v)) dots += `<circle cx="${X(i)}" cy="${Y(v)}" r="5" fill="${col}" stroke="#fff" stroke-width="2"/>`;
      rows.push({ k: sr.name, v: vf(v), color: col, dash: sr.dash });
    });
    if (spec.compare && series.length >= 2) {
      const ch = changeText(spec.fmt, series[0].values[i], series[1].values[i]);
      if (ch) rows.push({ k: 'Change', v: ch });
    }
    hd.innerHTML = dots;
    tip.show(ev, tipRows(labels[i] + (spec.partial && spec.partial[i] ? ' (partial month)' : ''), rows));
  };
  hit.addEventListener('pointermove', move);
  hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', () => { cross.setAttribute('opacity', '0'); hd.innerHTML = ''; tip.hide(); });
  animateLines(host, anim);
}

// Vertical bars: one or two series (current and prior), or highlighted singles.
function barChart(host, w, anim, spec, tip) {
  const labels = spec.labels || [];
  const series = (spec.series || []).filter((s) => (s.values || []).some(isNum));
  const H = spec.height || (w < 480 ? 230 : 260);
  if (!labels.length || !series.length) { host.innerHTML = empty(); return; }
  const showAxis = spec.axis !== false;
  const m = { l: showAxis ? 50 : 8, r: 10, t: spec.valueLabels ? 26 : 14, b: 42 };
  const iw = Math.max(20, w - m.l - m.r); const ih = H - m.t - m.b;
  const all = series.flatMap((s) => s.values).filter(isNum);
  const ticks = niceTicks(0, Math.max(...all) * 1.04, w < 480 ? 3 : 4);
  const hi = spec.max || ticks[ticks.length - 1];
  const n = labels.length;
  const band = iw / n;
  const gw = band * (series.length > 1 ? 0.76 : 0.62);
  const bw = Math.max(2, gw / series.length - (series.length > 1 ? 2 : 0));
  const Y = (v) => m.t + ih - (v / hi) * ih;
  const yf = axisFmt(spec.fmt); const vf = fmtOf(spec.fmt);
  let s = `<svg width="${w}" height="${H}" viewBox="0 0 ${w} ${H}" role="img" aria-label="${esc(spec.aria || '')}">`;
  if (showAxis) {
    ticks.forEach((t) => {
      if (t > hi + 1e-9) return;
      s += `<line x1="${m.l}" x2="${w - m.r}" y1="${Y(t)}" y2="${Y(t)}" class="pb-gl"/>`;
      s += `<text x="${m.l - 10}" y="${Y(t) + 4}" class="pb-yl" text-anchor="end">${esc(yf(t))}</text>`;
    });
  }
  s += `<line x1="${m.l}" x2="${w - m.r}" y1="${m.t + ih}" y2="${m.t + ih}" class="pb-base"/>`;
  const tickList = spec.ticktext ? labels.map((_, i) => ({ i, a: spec.ticktext[i][0], b: spec.ticktext[i][1] || '' }))
    : xTicks(labels, Math.max(2, Math.floor(iw / 44)));
  tickList.forEach((t) => {
    const cx = m.l + band * t.i + band / 2;
    s += `<text x="${cx}" y="${H - m.b + 18}" class="pb-xl" text-anchor="middle">${esc(t.a)}` +
      (t.b ? `<tspan x="${cx}" dy="13" class="pb-xl2">${esc(t.b)}</tspan>` : '') + '</text>';
  });
  for (let i = 0; i < n; i++) {
    const gx = m.l + band * i + (band - gw) / 2;
    series.forEach((sr, k) => {
      const v = sr.values[i];
      if (!isNum(v)) return;
      const hl = sr.highlight && sr.highlight[i];
      const col = hl ? (COLOR[sr.hiColor] || C.teal) : (COLOR[sr.color] || sr.color || C.teal);
      const x = gx + k * (bw + (series.length > 1 ? 2 : 0));
      const y = Y(v);
      s += `<path d="${roundTopRect(x, y, bw, m.t + ih - y, 4)}" fill="${col}"${sr.opacity ? ` opacity="${sr.opacity}"` : ''} class="pb-grow" style="transform-origin:${x + bw / 2}px ${m.t + ih}px"/>`;
      if (spec.valueLabels && k === series.length - 1) {
        s += `<text x="${x + bw / 2}" y="${y - 8}" class="pb-vl" text-anchor="middle">${esc(vf(v))}</text>`;
      }
    });
  }
  s += `<rect class="pb-band" x="0" y="${m.t}" width="0" height="${ih}" opacity="0"/>`;
  s += `<rect class="pb-hit" x="${m.l}" y="${m.t - 10}" width="${iw}" height="${ih + 10}" fill="transparent"/>`;
  s += '</svg>';
  host.innerHTML = legendHTML(series.map((sr) => ({ name: sr.name, color: COLOR[sr.color] || sr.color || C.teal }))) + s;
  const svg = host.querySelector('svg');
  const hit = svg.querySelector('.pb-hit');
  const bandEl = svg.querySelector('.pb-band');
  const move = (ev) => {
    const r = svg.getBoundingClientRect();
    const i = Math.max(0, Math.min(n - 1, Math.floor((ev.clientX - r.left - m.l) / band)));
    bandEl.setAttribute('x', m.l + band * i); bandEl.setAttribute('width', band); bandEl.setAttribute('opacity', '1');
    const rows = series.map((sr) => ({ k: sr.name, v: vf(sr.values[i]), color: sr.highlight && sr.highlight[i] ? (COLOR[sr.hiColor] || C.teal) : (COLOR[sr.color] || sr.color) }));
    if (spec.extra) spec.extra.forEach((ex) => rows.push({ k: ex.name, v: fmtOf(ex.fmt)(ex.values[i]) }));
    if (spec.compare && series.length >= 2) {
      const ch = changeText(spec.fmt, series[1].values[i], series[0].values[i]);
      if (ch) rows.push({ k: 'Change', v: ch });
    }
    tip.show(ev, tipRows((spec.tipLabels && spec.tipLabels[i]) || labels[i], rows));
  };
  hit.addEventListener('pointermove', move);
  hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', () => { bandEl.setAttribute('opacity', '0'); tip.hide(); });
  animateBars(host, anim);
}

// Compression nights: 80%+ bars with the 90%+ subset drawn as a narrow inset.
function compressionChart(host, w, anim, spec, tip) {
  const labels = spec.labels || [];
  if (!labels.length) { host.innerHTML = empty(); return; }
  const H = w < 480 ? 230 : 250;
  const m = { l: 36, r: 8, t: 26, b: 44 };
  const iw = Math.max(20, w - m.l - m.r); const ih = H - m.t - m.b;
  const hi = niceTicks(0, Math.max(...spec.d80, 1) * 1.1, 4);
  const top = hi[hi.length - 1];
  const n = labels.length; const band = iw / n;
  const Y = (v) => m.t + ih - (v / top) * ih;
  let s = `<svg width="${w}" height="${H}" viewBox="0 0 ${w} ${H}" role="img" aria-label="Compression nights by quarter">`;
  hi.forEach((t) => {
    s += `<line x1="${m.l}" x2="${w - m.r}" y1="${Y(t)}" y2="${Y(t)}" class="pb-gl"/>`;
    s += `<text x="${m.l - 8}" y="${Y(t) + 4}" class="pb-yl" text-anchor="end">${t}</text>`;
  });
  s += `<line x1="${m.l}" x2="${w - m.r}" y1="${m.t + ih}" y2="${m.t + ih}" class="pb-base"/>`;
  let prevYear = null;
  labels.forEach((q, i) => {
    const [yr, qq] = String(q).split('-');
    const cx = m.l + band * i + band / 2;
    const bw = Math.min(46, band * 0.62);
    const iw2 = Math.max(4, bw * 0.38);
    const v80 = spec.d80[i]; const v90 = spec.d90[i];
    const inProg = spec.complete && !spec.complete[i];
    s += `<path d="${roundTopRect(cx - bw / 2, Y(v80), bw, m.t + ih - Y(v80), 4)}" fill="${C.teal}"${inProg ? ' opacity=".78"' : ''} class="pb-grow" style="transform-origin:${cx}px ${m.t + ih}px"/>`;
    if (v90 > 0) s += `<path d="${roundTopRect(cx - iw2 / 2, Y(v90), iw2, m.t + ih - Y(v90), 2)}" fill="${C.maroon}" class="pb-grow" style="transform-origin:${cx}px ${m.t + ih}px"/>`;
    s += `<text x="${cx}" y="${Y(v80) - 8}" class="pb-vl" text-anchor="middle">${v80}</text>`;
    s += `<text x="${cx}" y="${H - m.b + 18}" class="pb-xl" text-anchor="middle">${esc((qq || q) + (inProg ? '*' : ''))}` +
      (yr !== prevYear ? `<tspan x="${cx}" dy="13" class="pb-xl2">${esc(yr)}</tspan>` : '') + '</text>';
    prevYear = yr;
  });
  s += `<rect class="pb-band" x="0" y="${m.t}" width="0" height="${ih}" opacity="0"/>`;
  s += `<rect class="pb-hit" x="${m.l}" y="${m.t - 16}" width="${iw}" height="${ih + 16}" fill="transparent"/></svg>`;
  host.innerHTML = legendHTML([{ name: 'Nights at 80% or higher', color: C.teal }, { name: 'Nights at 90% or higher', color: C.maroon }]) + s;
  const svg = host.querySelector('svg'); const hit = svg.querySelector('.pb-hit'); const bandEl = svg.querySelector('.pb-band');
  const move = (ev) => {
    const r = svg.getBoundingClientRect();
    const i = Math.max(0, Math.min(n - 1, Math.floor((ev.clientX - r.left - m.l) / band)));
    bandEl.setAttribute('x', m.l + band * i); bandEl.setAttribute('width', band); bandEl.setAttribute('opacity', '1');
    tip.show(ev, tipRows(labels[i] + (spec.complete && !spec.complete[i] ? ', in progress' : ''), [
      { k: 'Nights at 80% or higher', v: String(spec.d80[i]), color: C.teal },
      { k: 'Of those, at 90% or higher', v: String(spec.d90[i]), color: C.maroon },
    ]));
  };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', () => { bandEl.setAttribute('opacity', '0'); tip.hide(); });
  animateBars(host, anim);
}

// 100% stacked monthly bars (transient, group, contract).
function stackChart(host, w, anim, spec, tip) {
  const labels = spec.labels || [];
  const series = spec.series || [];
  if (!labels.length || !series.length) { host.innerHTML = empty(); return; }
  const H = w < 480 ? 230 : 260;
  const m = { l: 44, r: 8, t: 12, b: 42 };
  const iw = Math.max(20, w - m.l - m.r); const ih = H - m.t - m.b;
  const n = labels.length; const band = iw / n; const bw = Math.min(40, band * 0.7);
  const Y = (v) => m.t + ih - (v / 100) * ih;
  let s = `<svg width="${w}" height="${H}" viewBox="0 0 ${w} ${H}" role="img" aria-label="Business mix by month">`;
  [0, 25, 50, 75, 100].forEach((t) => {
    s += `<line x1="${m.l}" x2="${w - m.r}" y1="${Y(t)}" y2="${Y(t)}" class="pb-gl"/>`;
    s += `<text x="${m.l - 8}" y="${Y(t) + 4}" class="pb-yl" text-anchor="end">${t}%</text>`;
  });
  xTicks(labels, Math.max(2, Math.floor(iw / 40))).forEach((t) => {
    const cx = m.l + band * t.i + band / 2;
    s += `<text x="${cx}" y="${H - m.b + 18}" class="pb-xl" text-anchor="middle">${esc(t.a)}` +
      (t.b ? `<tspan x="${cx}" dy="13" class="pb-xl2">${esc(t.b)}</tspan>` : '') + '</text>';
  });
  for (let i = 0; i < n; i++) {
    const cx = m.l + band * i + band / 2;
    let acc = 0;
    series.forEach((sr) => {
      const v = sr.values[i] || 0;
      if (v <= 0) return;
      const y1 = Y(acc + v); const y0 = Y(acc);
      const hgt = Math.max(0, y0 - y1 - 2);
      s += `<rect x="${cx - bw / 2}" y="${y1}" width="${bw}" height="${hgt}" rx="2" fill="${COLOR[sr.color] || sr.color}" class="pb-grow" style="transform-origin:${cx}px ${m.t + ih}px"/>`;
      acc += v;
    });
  }
  s += `<rect class="pb-band" x="0" y="${m.t}" width="0" height="${ih}" opacity="0"/>`;
  s += `<rect class="pb-hit" x="${m.l}" y="${m.t}" width="${iw}" height="${ih}" fill="transparent"/></svg>`;
  host.innerHTML = s;
  const svg = host.querySelector('svg'); const hit = svg.querySelector('.pb-hit'); const bandEl = svg.querySelector('.pb-band');
  const move = (ev) => {
    const r = svg.getBoundingClientRect();
    const i = Math.max(0, Math.min(n - 1, Math.floor((ev.clientX - r.left - m.l) / band)));
    bandEl.setAttribute('x', m.l + band * i); bandEl.setAttribute('width', band); bandEl.setAttribute('opacity', '1');
    tip.show(ev, tipRows(labels[i], series.map((sr) => ({ k: sr.name, v: FMT.pct0(sr.values[i]), color: COLOR[sr.color] || sr.color }))));
  };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerdown', move);
  hit.addEventListener('pointerleave', () => { bandEl.setAttribute('opacity', '0'); tip.hide(); });
  animateBars(host, anim);
}

// Donut for the submarket room split.
function donut(host, w, anim, spec, tip) {
  const parts = (spec.parts || []).filter((p) => isNum(p.value) && p.value > 0);
  if (!parts.length) { host.innerHTML = ''; return; }
  const size = Math.min(210, Math.max(150, w * 0.5));
  const r = size / 2 - 16; const sw = 22; const circ = 2 * Math.PI * r;
  const total = parts.reduce((a, p) => a + p.value, 0);
  let off = 0;
  let s = `<div class="pb-donut"><svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" role="img" aria-label="Rooms by tier">`;
  s += `<g transform="rotate(-90 ${size / 2} ${size / 2})">`;
  parts.forEach((p, i) => {
    const len = (p.value / total) * circ;
    s += `<circle class="pb-seg" data-i="${i}" cx="${size / 2}" cy="${size / 2}" r="${r}" fill="none" stroke="${COLOR[p.color] || p.color}" stroke-width="${sw}"` +
      ` stroke-dasharray="${Math.max(0, len - 2)} ${circ}" stroke-dashoffset="${-off}"/>`;
    off += len;
  });
  s += `</g><text x="${size / 2}" y="${size / 2 - 2}" text-anchor="middle" class="pb-dn-v">${esc(spec.center || '')}</text>` +
    `<text x="${size / 2}" y="${size / 2 + 16}" text-anchor="middle" class="pb-dn-l">${esc(spec.centerLabel || '')}</text></svg>`;
  s += '<div class="pb-legend col">' + parts.map((p) => `<span><i style="background:${COLOR[p.color] || p.color}"></i>${esc(p.label)}` +
    ` <b>${Math.round((p.value / total) * 100)}%</b></span>`).join('') + '</div></div>';
  host.innerHTML = s;
  host.querySelectorAll('.pb-seg').forEach((el) => {
    const p = parts[+el.dataset.i];
    const f = (ev) => tip.show(ev, tipRows(p.label, [{ k: 'Rooms', v: FMT.num0(p.value), color: COLOR[p.color] || p.color },
      { k: 'Share of tiered rooms', v: FMT.pct0((p.value / total) * 100) }]));
    el.addEventListener('pointermove', f); el.addEventListener('pointerdown', f);
    el.addEventListener('pointerleave', () => tip.hide());
  });
}

// Where visitors come from: each origin market placed by compass bearing and
// distance from Dana Point (log-scaled rings), sized by share of spending.
function polarChart(host, w, anim, spec, tip) {
  const pts = (spec.points || []).filter((p) => isNum(p.miles) && isNum(p.bearing));
  if (!pts.length) { host.innerHTML = empty('Origin market locations are not available.'); return; }
  const H = Math.min(w, 420);
  const cx = w / 2; const cy = H / 2; const R = Math.min(w, H) / 2 - 26;
  const maxMi = Math.max(3000, ...pts.map((p) => p.miles));
  const rOf = (mi) => (Math.log10(1 + mi / 40) / Math.log10(1 + maxMi / 40)) * R;
  const maxShare = Math.max(...pts.map((p) => p.share || 0), 1);
  let s = `<svg width="${w}" height="${H}" viewBox="0 0 ${w} ${H}" role="img" aria-label="Origin markets by distance and direction">`;
  s += `<defs><radialGradient id="pbglow"><stop offset="0" stop-color="${C.tealLt}" stop-opacity=".35"/><stop offset="1" stop-color="${C.tealLt}" stop-opacity="0"/></radialGradient></defs>`;
  s += `<circle cx="${cx}" cy="${cy}" r="${R}" fill="${C.mist}"/>`;
  [100, 500, 1000, 2500].forEach((mi) => {
    const rr = rOf(mi);
    s += `<circle cx="${cx}" cy="${cy}" r="${rr}" fill="none" class="pb-ring"/>`;
    s += `<text x="${cx + 4}" y="${cy - rr - 3}" class="pb-ringl">${mi.toLocaleString('en-US')} mi</text>`;
  });
  s += `<line x1="${cx - R}" x2="${cx + R}" y1="${cy}" y2="${cy}" class="pb-axis"/><line x1="${cx}" x2="${cx}" y1="${cy - R}" y2="${cy + R}" class="pb-axis"/>`;
  [['N', cx, cy - R - 8], ['S', cx, cy + R + 16], ['E', cx + R + 10, cy + 4], ['W', cx - R - 10, cy + 4]].forEach(([t, x, y]) => {
    s += `<text x="${x}" y="${y}" class="pb-compass" text-anchor="middle">${t}</text>`;
  });
  const placed = pts.map((p) => {
    const th = (p.bearing * Math.PI) / 180; const rr = rOf(p.miles);
    return { ...p, x: cx + rr * Math.sin(th), y: cy - rr * Math.cos(th), r: 5 + Math.sqrt((p.share || 0) / maxShare) * 17 };
  });
  placed.forEach((p) => {
    s += `<line x1="${cx}" y1="${cy}" x2="${p.x}" y2="${p.y}" stroke="${C.teal}" stroke-opacity=".28" stroke-width="${1 + ((p.share || 0) / maxShare) * 3}" class="pb-fade"/>`;
  });
  placed.slice().sort((a, b) => b.r - a.r).forEach((p) => {
    s += `<circle class="pb-bub pb-fade" data-n="${esc(p.name)}" cx="${p.x}" cy="${p.y}" r="${p.r}" fill="${C.teal}" fill-opacity=".78" stroke="#fff" stroke-width="1.5"/>`;
  });
  s += `<circle cx="${cx}" cy="${cy}" r="26" fill="url(#pbglow)"/><circle cx="${cx}" cy="${cy}" r="7" fill="${C.maroon}" stroke="#fff" stroke-width="2.5"/>`;
  // Dana Point's label sits to the west, over the ocean, where no market is.
  s += `<text x="${cx - 12}" y="${cy + 4}" class="pb-dp" text-anchor="end">Dana Point</text>`;
  const boxes = [[cx - 12 - 70, cy - 8, cx - 12, cy + 8]];
  placed.forEach((p) => boxes.push([p.x - p.r, p.y - p.r, p.x + p.r, p.y + p.r]));
  const hitAny = (b, skip) => boxes.some((q, qi) => qi !== skip && b[0] < q[2] && b[2] > q[0] && b[1] < q[3] && b[3] > q[1]);
  const maxLabels = w < 420 ? 5 : 10;
  let shown = 0;
  placed.forEach((p, pi) => {
    if (shown >= maxLabels) return;
    const txt = p.label || p.name; const tw = txt.length * 6.1 + 4;
    const right = p.x >= cx;
    const x0 = right ? p.x + p.r + 4 : p.x - p.r - 4 - tw;
    const cand = [x0, p.y - 7, x0 + tw, p.y + 7];
    if (hitAny(cand, pi + 1) || cand[0] < 2 || cand[2] > w - 2) return;
    boxes.push(cand); shown += 1;
    s += `<text x="${right ? p.x + p.r + 4 : p.x - p.r - 4}" y="${p.y + 4}" class="pb-bubl" text-anchor="${right ? 'start' : 'end'}">${esc(txt)}</text>`;
  });
  s += '</svg>';
  host.innerHTML = s;
  host.querySelectorAll('.pb-bub').forEach((el) => {
    const p = placed.find((q) => q.name === el.dataset.n);
    const f = (ev) => tip.show(ev, tipRows(p.label || p.name, [
      { k: 'Share of visitor spending', v: FMT.pct1(p.share), color: C.teal },
      { k: 'Distance from Dana Point', v: FMT.num0(p.miles) + ' mi' }]));
    el.addEventListener('pointermove', f); el.addEventListener('pointerdown', f);
    el.addEventListener('pointerleave', () => tip.hide());
  });
  animateLines(host, anim);
}

// ---------------------------------------------------------------------------
// HTML-based visuals (ranked bars, comparisons, splits, events)
// ---------------------------------------------------------------------------

function hbars(spec) {
  const rows = spec.rows || [];
  if (!rows.length) return empty();
  const max = Math.max(...rows.map((r) => r.value || 0)) || 1;
  const vf = fmtOf(spec.fmt || 'pct1');
  return '<div class="pb-hb">' + rows.map((r) => `<div class="pb-hb-r${r.hi ? ' hi' : ''}"><span class="n">${esc(r.label)}</span>` +
    `<span class="t"><span class="b" style="width:${Math.max(1.5, (r.value / max) * 100)}%"></span></span><span class="v">${esc(vf(r.value))}</span></div>`).join('') + '</div>';
}

function pairBars(spec) {
  const rows = spec.rows || [];
  if (!rows.length) return '';
  const max = Math.max(...rows.flatMap((r) => [r.a || 0, r.b || 0])) || 1;
  const leg = `<div class="pb-legend"><span><i style="background:${C.teal}"></i>${esc(spec.aName)}</span><span><i style="background:${C.maroon}"></i>${esc(spec.bName)}</span></div>`;
  return leg + '<div class="pb-pb">' + rows.map((r) => `<div class="pb-pb-r"><span class="n">${esc(r.label)}</span><span class="bars">` +
    `<span class="row"><span class="tk"><span class="b a" style="width:${Math.max(1, ((r.a || 0) / max) * 100)}%"></span></span><span class="v">${esc(FMT.pct1(r.a))}</span></span>` +
    `<span class="row"><span class="tk"><span class="b bb" style="width:${Math.max(1, ((r.b || 0) / max) * 100)}%"></span></span><span class="v">${esc(FMT.pct1(r.b))}</span></span>` +
    '</span></div>').join('') + '</div>';
}

function compareTable(spec) {
  const rows = spec.rows || []; const cols = spec.cols || [];
  if (!rows.length) return empty();
  const max = {}; cols.forEach((c) => { max[c.key] = Math.max(...rows.map((r) => r[c.key] || 0)) || 1; });
  let s = `<div class="pb-cmp" style="--cols:${cols.length}"><div class="pb-cmp-h"><span></span>` + cols.map((c) => `<span>${esc(c.label)}</span>`).join('') + '</div>';
  rows.forEach((r) => {
    s += `<div class="pb-cmp-r${r.hi ? ' hi' : ''}"><span class="n">${esc(r.name)}</span>`;
    cols.forEach((c) => {
      const v = r[c.key];
      s += `<span class="c"><span class="v">${esc(fmtOf(c.fmt)(v))}</span><span class="t"><span class="b" style="width:${isNum(v) ? Math.max(2, (v / max[c.key]) * 100) : 0}%"></span></span></span>`;
    });
    s += '</div>';
  });
  return s + '</div>';
}

function splitBar(parts, opts) {
  const ps = (parts || []).filter((p) => isNum(p.value) && p.value > 0);
  if (!ps.length) return '';
  const tot = ps.reduce((a, p) => a + p.value, 0);
  return `<div class="pb-split${opts && opts.big ? ' big' : ''}">` + ps.map((p) => `<span style="flex:${p.value};background:${COLOR[p.color] || p.color}" title="${esc(p.label)}"></span>`).join('') +
    '</div><div class="pb-legend">' + ps.map((p) => `<span><i style="background:${COLOR[p.color] || p.color}"></i>${esc(p.label)} <b>${esc(p.text || FMT.pct0((p.value / tot) * 100))}</b></span>`).join('') + '</div>';
}

function eventsList(spec) {
  const items = spec.items || [];
  if (!items.length) return empty('No events are on the calendar for the next 120 days.');
  return '<ol class="pb-ev">' + items.map((e) => `<li><div class="d"><span class="mo">${esc(e.mon)}</span><span class="dy">${esc(e.day)}</span></div>` +
    `<div class="b"><div class="nm">${esc(e.name)}</div><div class="ly">${esc(e.ly)}</div>` +
    (isNum(e.ly_occ) ? `<div class="occ"><span style="width:${Math.min(100, e.ly_occ)}%"></span></div>` : '') + '</div>' +
    `<span class="pill${e.soon ? ' soon' : ''}">${esc(e.when)}</span></li>`).join('') + '</ol>';
}

function miniKpis(tiles) {
  return '<div class="pb-mk">' + (tiles || []).map((t) => `<div><span class="l">${esc(t.label)}</span><span class="v">${esc(t.value)}</span><span class="f">${esc(t.foot || '')}</span></div>`).join('') + '</div>';
}

// ---------------------------------------------------------------------------
// Layout pieces
// ---------------------------------------------------------------------------

function kpiTiles(kpis) {
  return '<div class="pb-kpis">' + (kpis || []).map((k) => `<div class="pb-kpi"><span class="l">${esc(k.label)}</span>` +
    `<div class="v" data-num="${isNum(k.num) ? k.num : ''}" data-fmt="${esc(k.fmt || '')}">${esc(k.value)}</div>` +
    `<div class="d">${deltaChip(k.delta)}${k.spark ? spark(k.spark, 72, 24, C.teal, true) : ''}</div><div class="f">${esc(k.foot || '')}</div></div>`).join('') + '</div>';
}

function answerBox(text, label) {
  if (!text) return '';
  return `<div class="pb-ans"><div class="lab">${esc(label || 'What the data says')}</div><p>${emph(text)}</p></div>`;
}

function cardHTML(c, idx) {
  const cls = ['pb-card', c.wide ? 'wide' : '', c.side ? 'side' : ''].filter(Boolean).join(' ');
  const body = `<div class="pb-chart" data-i="${idx}"></div>`;
  const text = (c.a ? `<p class="pb-ca">${emph(c.a)}</p>` : '') +
    (c.how ? `<details class="pb-how"><summary>How to read this</summary><div>${esc(c.how)}</div></details>` : '');
  const head = `<h3 class="pb-cq">${esc(c.q || '')}</h3>` + (c.src ? `<div class="pb-src">${esc(c.src)}</div>` : '');
  if (c.side) return `<article class="${cls}">${head}<div class="pb-side"><div>${body}</div><div class="pb-side-t">${text}</div></div></article>`;
  return `<article class="${cls}">${head}${body}${text}</article>`;
}

function drawCard(host, c, w, anim, tip) {
  switch (c.type) {
    case 'line': return lineChart(host, w, anim, c, tip);
    case 'bars': return barChart(host, w, anim, c, tip);
    case 'compression': return compressionChart(host, w, anim, c, tip);
    case 'stack': {
      stackChart(host, w, anim, c, tip);
      if (c.totals) host.insertAdjacentHTML('beforeend', '<div class="pb-sub">Whole window</div>' + splitBar(c.totals));
      return;
    }
    case 'polar': return polarChart(host, w, anim, c, tip);
    case 'hbars': host.innerHTML = hbars(c); return;
    case 'compare': host.innerHTML = compareTable(c); return;
    case 'events': host.innerHTML = eventsList(c); return;
    case 'split': host.innerHTML = splitBar(c.parts, { big: true }); return;
    case 'ads': host.innerHTML = miniKpis(c.tiles) + pairBars(c); return;
    case 'adr': {
      lineChart(host, w, anim, c, tip);
      if (c.split) host.insertAdjacentHTML('beforeend', `<div class="pb-sub">What moved RevPAR (${esc(c.split.total)})</div>` + splitBar(c.split.parts));
      return;
    }
    case 'tiers': {
      const bars = document.createElement('div'); host.appendChild(bars);
      barChart(bars, w, anim, c, tip);
      if (c.tierText) host.insertAdjacentHTML('beforeend', `<p class="pb-ca">${emph(c.tierText)}</p>`);
      const d = document.createElement('div'); host.appendChild(d);
      donut(d, w, anim, c.donut || {}, tip);
      return;
    }
    default: host.innerHTML = '';
  }
}

function mountCharts(root, cards, tip) {
  root.querySelectorAll('.pb-chart').forEach((host) => {
    const c = cards[+host.dataset.i];
    if (!c) return;
    let lastW = 0; let anim = true;
    const run = () => {
      const w = Math.round(host.clientWidth);
      if (!w || Math.abs(w - lastW) < 3) return;
      lastW = w;
      host.innerHTML = '';
      try { drawCard(host, c, w, anim, tip); } catch (e) { host.innerHTML = empty('This chart could not be drawn.'); console.error('[pulse_board]', e); }
      anim = false;
    };
    if (typeof ResizeObserver !== 'undefined') {
      const ro = new ResizeObserver(() => run());
      ro.observe(host);
      root._cleanup.push(() => ro.disconnect());
    }
    run();
  });
}

function countUp(root) {
  if (REDUCE) return;
  root.querySelectorAll('[data-num]').forEach((el) => {
    const target = parseFloat(el.dataset.num); const f = FMT[el.dataset.fmt];
    if (!isFinite(target) || !f) return;
    const final = el.textContent; const t0 = performance.now(); const dur = 900;
    const tick = (t) => {
      const p = Math.min(1, (t - t0) / dur); const e = 1 - Math.pow(1 - p, 3);
      el.textContent = p < 1 ? f(target * e) : final;
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}

const ICON = {
  hotel: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 20V7l9-4 9 4v13" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"/><path d="M8 20v-5h8v5M8 10h2M14 10h2" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
  visitors: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="9" cy="8" r="3.2" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M3 20c.6-3.6 3-5.5 6-5.5s5.4 1.9 6 5.5M16 5.2a3 3 0 010 5.6M18 14.8c1.7.7 2.8 2.4 3 5.2" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
  market: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 20V11M10 20V5M16 20v-7M22 20H2" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
  calendar: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3.5" y="5" width="17" height="15" rx="2.5" fill="none" stroke="currentColor" stroke-width="1.8"/><path d="M3.5 10h17M8 3v4M16 3v4" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>',
};

const WAVES = '<svg class="pb-waves" viewBox="0 0 1200 220" preserveAspectRatio="none" aria-hidden="true">' +
  [0, 1, 2, 3, 4, 5].map((k) => `<path d="M0 ${150 + k * 12} C 200 ${120 + k * 12}, 400 ${180 + k * 12}, 600 ${150 + k * 12} S 1000 ${120 + k * 12}, 1200 ${150 + k * 12}" fill="none" stroke="#fff" stroke-opacity="${0.10 - k * 0.012}" stroke-width="1.2"/>`).join('') + '</svg>';

function renderBrief(root, d) {
  const hero = (d.kpis || []).map((k) => `<div class="pb-hk"><div class="l">${esc(k.label)}</div>` +
    `<div class="v" data-num="${isNum(k.num) ? k.num : ''}" data-fmt="${esc(k.fmt || '')}">${esc(k.value)}</div>` +
    `<div class="d">${deltaChip(k.delta, true)}</div>` + (k.spark ? spark(k.spark, 120, 30, C.tealLt, true) : '') +
    `<div class="f">${esc(k.foot || '')}</div></div>`).join('');
  const fresh = (d.fresh || []).map((f) => `<span class="pb-fc"><i class="${f.ok ? 'ok' : 'late'}"></i><b>${esc(f.label)}</b> ${esc(f.text)}</span>`).join('');
  const cards = (d.cards || []).map((c, i) => `<button class="pb-gc ${esc(c.icon)}" data-anchor="${esc(c.anchor)}">` +
    `<span class="ic">${ICON[c.icon] || ''}</span><span class="no">0${i + 1}</span><span class="k">${esc(c.kicker)}</span>` +
    `<span class="t">${esc(c.title)}</span><span class="b">${emph(c.body)}</span><span class="go">See the detail <i>→</i></span></button>`).join('');
  root.insertAdjacentHTML('beforeend', `
    <section class="pb-brief"${d.photo ? ` style="--photo:url('${d.photo}')"` : ''}>
      <div class="pb-brief-photo"></div><div class="pb-brief-scrim"></div>${WAVES}
      <div class="pb-brief-in">
        <div class="pb-logo" role="img" aria-label="Visit Dana Point"></div>
        <div class="pb-brief-top"><span class="pb-kick">${esc(d.eyebrow)}</span><span class="pb-span">${esc(d.span)}</span></div>
        <h1 class="pb-hl">${esc(d.headline)}</h1>
        <p class="pb-dek">${emph(d.dek)}</p>
        <div class="pb-hks">${hero}</div>
        <div class="pb-fresh">${fresh}</div>
      </div>
    </section>
    <section class="pb-glance">
      <div class="pb-glance-h"><h2>${esc(d.cardsTitle || 'Four answers for the board')}</h2><p>${esc(d.cardsSub || '')}</p></div>
      <div class="pb-gcs">${cards}</div>
    </section>`);
  root.querySelectorAll('.pb-gc').forEach((b) => b.addEventListener('click', () => {
    const t = document.getElementById(b.dataset.anchor);
    if (t) t.scrollIntoView({ behavior: REDUCE ? 'auto' : 'smooth', block: 'start' });
  }));
}

function renderSection(root, d, tip) {
  const cards = d.cards || [];
  root.insertAdjacentHTML('beforeend', `
    <section class="pb-sec">
      <header class="pb-sh"><span class="pb-no">0${esc(d.n)}</span><div><div class="pb-eb">${esc(d.eyebrow)}</div><h2 class="pb-st">${esc(d.title)}</h2></div></header>
      <p class="pb-q"><span>Q</span>${esc(d.question)}</p>
      ${d.empty ? `<div class="pb-empty big">${esc(d.empty)}</div>` : ''}
      ${d.kpis && d.kpis.length ? kpiTiles(d.kpis) : ''}
      ${answerBox(d.answer)}
      <div class="pb-grid">${cards.map((c, i) => cardHTML(c, i)).join('')}</div>
      ${d.note ? answerBox(d.note.text, d.note.label).replace('pb-ans', 'pb-ans note') : ''}
    </section>`);
  mountCharts(root, cards, tip);
}

export default function (component) {
  const { data, parentElement } = component;
  let root = parentElement.querySelector('.pb');
  if (!root) {
    root = document.createElement('div');
    root.className = 'pb';
    parentElement.appendChild(root);
  }
  (root._cleanup || []).forEach((f) => { try { f(); } catch (e) { /* no-op */ } });
  root._cleanup = [];
  root.innerHTML = '';
  const d = data || {};
  try {
    const tip = makeTip(root);
    if (d.kind === 'brief') renderBrief(root, d);
    else renderSection(root, d, tip);
    countUp(root);
  } catch (err) {
    root.innerHTML = '<div class="pb-empty big">This part of the board could not be drawn.</div>';
    console.error('[pulse_board]', err);
  }
  return () => (root._cleanup || []).forEach((f) => { try { f(); } catch (e) { /* no-op */ } });
}
