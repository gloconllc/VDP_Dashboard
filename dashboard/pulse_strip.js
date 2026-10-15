// pulse_strip.js: "At a glance" infographic strips and the PULSE Summary
// announcement, drawn under the existing board sections (added 2026-10-14).
//
// Mounted as an st.components.v2 component. Python (pulse_strip.py) passes
// figures already computed by pulse_insights.build_report; this file only
// draws them: infographics with value tooltips, clean data cards, quiet
// entrance motion (off under reduced motion), keyboard focus on every mark.

const REDUCE = typeof window !== 'undefined' && window.matchMedia
  && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
  { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const bold = (s) => esc(s).replace(
  /(\$[\d,.]+[MBK]?|[+−-]?\d[\d,.]*%|[+−-]\d[\d.]* points|\b\d{1,3}(?:,\d{3})+\b)/g, '<b>$1</b>');

function fmt(v, f) {
  if (v == null || isNaN(v)) return 'n/a';
  if (f === 'pct') return v.toFixed(1) + '%';
  if (f === 'usd') return '$' + Math.round(v).toLocaleString('en-US');
  if (f === 'usdbig') {
    const a = Math.abs(v);
    return a >= 1e9 ? '$' + (v / 1e9).toFixed(2) + 'B' : a >= 1e6 ? '$' + (v / 1e6).toFixed(1) + 'M'
      : a >= 1e3 ? '$' + Math.round(v / 1e3) + 'K' : '$' + Math.round(v);
  }
  if (f === 'num') { const b = Math.abs(v); return b >= 1e6 ? (v / 1e6).toFixed(2) + 'M' : Math.round(v).toLocaleString('en-US'); }
  return String(v);
}

// Same infographic vocabulary as the Insights report.
function ig(g) {
  let b = '';
  if (g.type === 'chain') {
    b = '<div class="chain">' + g.steps.map((s, i) => (i ? '<span class="ar" aria-hidden="true">→</span>' : '')
      + '<div class="st" data-tip="' + esc(s.tip || s.l) + '" tabindex="0"><div class="v">' + esc(s.v) + '</div><div class="l">' + esc(s.l) + '</div></div>').join('') + '</div>';
  } else if (g.type === 'waffle') {
    const a = Math.round(g.parts[0].v); let cells = '';
    for (let i = 0; i < 100; i++) cells += '<i class="' + (i < a ? 'a' : 'b') + '"></i>';
    b = '<div class="ig-row"><div class="waffle" role="img" aria-label="' + esc(g.parts.map((p) => p.v.toFixed(1) + '% ' + p.l).join(', '))
      + '" data-tip="' + esc(g.parts[0].v.toFixed(1) + '% ' + g.parts[0].l) + '" tabindex="0">' + cells + '</div><div class="ig-leg">'
      + g.parts.map((p, k) => '<div><div class="v" style="color:' + (k ? '#0B2530' : '#123C4A') + '">' + p.v.toFixed(1) + '%</div><div>' + esc(p.l) + '</div></div>').join('') + '</div></div>';
  } else if (g.type === 'share') {
    b = '<div class="sharebar" role="img" aria-label="' + esc(g.parts.map((p) => p.v.toFixed(1) + '% ' + p.l).join(', ')) + '">'
      + g.parts.map((p, k) => '<div class="' + (k ? 'b' : 'a') + '" data-w="' + p.v + '" data-tip="' + esc(p.l + ': ' + p.v.toFixed(1) + '%') + '" tabindex="0">' + p.v.toFixed(1) + '%</div>').join('')
      + '</div><div class="legend" style="margin-top:10px">' + g.parts.map((p, k) => '<span><i style="background:' + (k ? '#8FC4D6' : '#123C4A') + '"></i>' + esc(p.l) + '</span>').join('') + '</div>';
  } else if (g.type === 'receipt') {
    const tot = g.items.reduce((s, x) => s + x.v, 0);
    const cols = ['#123C4A', '#1D6E86', '#8FC4D6', '#A8461F', '#E08A54', '#9C9186', '#CBD9DE'];
    b = '<div class="receipt">' + g.items.map((x) => '<div class="rr" data-tip="' + esc(x.l + ': ' + x.v.toFixed(1) + '¢ of every $1') + '" tabindex="0"><span>' + esc(x.l) + '</span><b>' + x.v.toFixed(1) + '¢</b></div>').join('')
      + '<div class="tot"><span>Every visitor dollar</span><span>' + tot.toFixed(0) + '¢</span></div><div class="dollar" aria-hidden="true">'
      + g.items.map((x, k) => '<span style="width:' + x.v + '%;background:' + cols[k % cols.length] + '"></span>').join('') + '</div></div>';
  } else if (g.type === 'towers') {
    const mx = Math.max.apply(null, g.rows.map((r) => r.v)) || 1;
    b = '<div class="towers" role="img" aria-label="' + esc(g.rows.map((r) => r.l + ' ' + r.v.toFixed(1) + '%').join(', ')) + '">'
      + g.rows.map((r) => '<div class="tw' + (r.hi ? ' hi' : '') + '" data-tip="' + esc(r.l + ': ' + r.v.toFixed(1) + '% occupancy') + '" tabindex="0"><span class="v">' + r.v.toFixed(1)
      + '%</span><span class="bx" data-h="' + (r.v / mx * 100).toFixed(1) + '"></span><span class="d">' + esc(r.l) + '</span></div>').join('') + '</div>';
  } else if (g.type === 'versus') {
    const m2 = Math.max(g.a.v, g.b.v) || 1;
    b = '<div class="vs">' + [['a', g.a], ['b', g.b]].map((p) => '<div class="row ' + p[0] + '" data-tip="' + esc(p[1].l + ': ' + fmt(p[1].v, p[1].fmt)) + '" tabindex="0"><span>' + esc(p[1].l)
      + '</span><span class="t"><span class="f" data-w="' + (p[1].v / m2 * 100).toFixed(1) + '"></span></span><span class="v">' + fmt(p[1].v, p[1].fmt) + '</span></div>').join('') + '</div>';
  } else if (g.type === 'funnel') {
    const m3 = g.stages.length ? g.stages[0].v || 1 : 1;
    b = '<div class="funnel">' + g.stages.map((s) => {
      const w = Math.max(30, Math.sqrt(s.v / m3) * 100);
      return '<div class="fn" style="width:' + w.toFixed(0) + '%" data-tip="' + esc(s.l + ': ' + fmt(s.v, s.fmt)) + '" tabindex="0"><span>' + esc(s.l) + '</span><span>' + fmt(s.v, s.fmt) + '</span></div>';
    }).join('') + '</div>';
  } else if (g.type === 'board') {
    b = '<div class="board" role="table" aria-label="Origin airports">' + g.rows.map((r) => '<div class="br" role="row" data-tip="' + esc(r.name + ': ' + r.v.toFixed(2) + '%') + '" tabindex="0"><span class="cd">'
      + esc(r.code) + '</span><span>' + esc(r.name) + '</span><span class="pc">' + r.v.toFixed(2) + '%</span></div>').join('') + '</div>';
  }
  return '<div class="ig reveal"><h3>' + esc(g.title) + '</h3><p class="cs">' + esc(g.sub || '') + '</p>' + b + '<p class="cap">Source: ' + esc(g.src) + '</p></div>';
}

function card(n) {
  const cls = String(n.p || '').indexOf('▲') === 0 ? ' up' : String(n.p || '').indexOf('▼') === 0 ? ' dn' : '';
  return '<div class="ps-card reveal" tabindex="0" data-tip="' + esc(n.l + ': ' + n.v + (n.p ? ' (' + n.p + ')' : '')) + '">'
    + '<div class="ps-k">' + esc(n.k) + '</div><div class="ps-v">' + esc(n.v) + '</div><div class="ps-l">' + esc(n.l) + '</div>'
    + (n.p ? '<div class="ps-p' + cls + '">' + esc(n.p) + '</div>' : '') + '<div class="ps-src">Source: ' + esc(n.src) + '</div></div>';
}

function renderGlance(root, d) {
  root.innerHTML = '<section class="ps-strip" aria-label="' + esc(d.title) + '">'
    + '<div class="ps-head"><div><div class="ps-eyebrow">At a glance · ' + esc(d.period) + '</div><h2 class="ps-title">' + esc(d.title) + '</h2></div>'
    + '<a class="ps-more" href="#pulse-insights">Read the full story in the Insights report →</a></div>'
    + (d.cards && d.cards.length ? '<div class="ps-cards">' + d.cards.map(card).join('') + '</div>' : '')
    + '<div class="ps-grid">' + (d.glance || []).map(ig).join('') + '</div></section>';
}

function renderAnnounce(root, d) {
  // One slim line: the PULSE Summary headline and a link to the full report.
  root.innerHTML = '<a class="ps-ann reveal" href="#pulse-insights" aria-label="PULSE Summary: ' + esc(d.headline) + '. Read the Insights report.">'
    + '<span class="ps-flag">PULSE Summary</span><span class="ps-ann-h">' + esc(d.headline) + '</span>'
    + '<span class="ps-date">' + esc(d.period) + '</span><span class="ps-go">Read the report →</span></a>';
}

function grow(el) {
  el.querySelectorAll('[data-w]').forEach((e) => { e.style.width = e.getAttribute('data-w') + '%'; });
  el.querySelectorAll('[data-h]').forEach((e) => { e.style.height = e.getAttribute('data-h') + '%'; });
}

function wire(root) {
  // Value tooltip and highlight: the hovered or focused mark is outlined, the rest dim.
  const tip = document.createElement('div');
  tip.className = 'ps-tip'; tip.hidden = true; root.appendChild(tip);
  const show = (e) => {
    const t = e.target.closest('[data-tip]');
    if (!t || !root.contains(t)) return;
    tip.textContent = t.getAttribute('data-tip'); tip.hidden = false;
    const r = t.getBoundingClientRect(), rr = root.getBoundingClientRect();
    tip.style.left = Math.max(4, Math.min(r.left - rr.left + r.width / 2 - 140, rr.width - 284)) + 'px';
    tip.style.top = (r.top - rr.top - 8) + 'px';
    const grp = t.parentElement;
    if (grp) { grp.classList.add('dim'); t.classList.add('sel'); }
  };
  const hide = (e) => {
    const t = e.target.closest('[data-tip]');
    tip.hidden = true;
    if (t) { t.classList.remove('sel'); if (t.parentElement) t.parentElement.classList.remove('dim'); }
  };
  root.addEventListener('mouseover', show); root.addEventListener('focusin', show);
  root.addEventListener('mouseout', hide); root.addEventListener('focusout', hide);

  // Quiet entrance: reveal and grow bars as each block scrolls into view.
  const blocks = root.querySelectorAll('.reveal');
  if (REDUCE || !('IntersectionObserver' in window)) {
    blocks.forEach((b) => b.classList.add('in')); grow(root); return () => {};
  }
  const io = new IntersectionObserver((es) => es.forEach((e) => {
    if (e.isIntersecting) { e.target.classList.add('in'); grow(e.target); io.unobserve(e.target); }
  }), { threshold: 0.15 });
  blocks.forEach((b) => io.observe(b));
  return () => io.disconnect();
}

export default function (component) {
  const { data, parentElement } = component;
  let root = parentElement.querySelector('.ps');
  if (!root) { root = document.createElement('div'); root.className = 'ps'; parentElement.appendChild(root); }
  if (root._cleanup) { try { root._cleanup(); } catch (e) { /* no-op */ } }
  const d = data || {};
  try {
    if (d.kind === 'announce') renderAnnounce(root, d); else renderGlance(root, d);
    root._cleanup = wire(root);
  } catch (err) {
    root.innerHTML = '';
    console.error('[pulse_strip]', err);
  }
  return () => { if (root._cleanup) { try { root._cleanup(); } catch (e) { /* no-op */ } } };
}
