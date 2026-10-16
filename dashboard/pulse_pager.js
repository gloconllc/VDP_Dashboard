/* Dana Point PULSE: one section per page (added 2026-10-15).
   Runs inside a zero-height component and works on the Streamlit page around it.
   Nothing is removed or rewritten: sections that are not on the current page are
   hidden with a class, and "Show all sections" puts the long scroll back. If the
   page layout is not recognised (fewer than three sections found) it does nothing,
   so the page stays exactly as it was. Config is injected as window.PZ_CFG. */
(function () {
  "use strict";
  var P, D;
  try { P = window.parent; D = P.document; D.body; } catch (e) { return; }
  var CFG = window.PZ_CFG || { pages: [], alias: {} };
  if (P.__pzPager && P.__pzPager.destroy) { try { P.__pzPager.destroy(); } catch (e) {} }

  var pages = CFG.pages.map(function (p) { return { id: p[0], label: p[1] }; });
  var alias = CFG.alias || {};
  var known = {}; pages.forEach(function (p) { known[p.id] = true; });
  function pageOf(id) { var t = alias[id] || id; return known[t] ? t : null; }
  var REDUCE = P.matchMedia && P.matchMedia("(prefers-reduced-motion: reduce)").matches;

  var active = null, all = false, items = [], bar = null, timer = null, enabled = false;
  try { all = P.sessionStorage.getItem("pzAll") === "1"; } catch (e) {}

  var css = D.createElement("style");
  css.id = "pz-pager-css";
  css.textContent =
    ".pz-off{display:none !important}" +
    "#pz-bar{position:fixed;left:50%;transform:translateX(-50%);bottom:16px;z-index:999;display:flex;align-items:center;gap:10px;" +
    "background:#fff;border:1px solid #cbe3ea;border-radius:14px;padding:8px 10px;box-shadow:0 6px 24px rgba(11,37,48,.16);" +
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0b2530;max-width:calc(100vw - 24px)}" +
    "#pz-bar button{font:inherit;font-weight:700;font-size:14px;min-height:44px;min-width:44px;padding:0 16px;border-radius:10px;cursor:pointer;" +
    "background:#1d6e86;color:#fff;border:1px solid #123c4a}" +
    "#pz-bar button:hover:not(:disabled){background:#123c4a}" +
    "#pz-bar button:disabled{opacity:.4;cursor:default}" +
    "#pz-bar button.pz-q{background:transparent;color:#0e7490;border-color:#cbe3ea;font-weight:600;font-size:13px;padding:0 12px}" +
    "#pz-bar button:focus-visible{outline:2px solid #1d6e86;outline-offset:2px}" +
    "#pz-bar .pz-l{display:flex;flex-direction:column;line-height:1.2;min-width:0;text-align:center}" +
    "#pz-bar .pz-n{font-size:11px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;color:#6b7280}" +
    "#pz-bar .pz-t{font-family:Georgia,serif;font-style:italic;font-weight:700;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:34vw}" +
    "@media (max-width:640px){#pz-bar{gap:6px;padding:6px}#pz-bar .pz-t{max-width:30vw;font-size:14px}#pz-bar button.pz-q{display:none}}";
  D.head.appendChild(css);

  function mainBlock() {
    var c = D.querySelector('[data-testid="stMainBlockContainer"]') || D.querySelector('[data-testid="stMain"]');
    return c ? c.querySelector('[data-testid="stVerticalBlock"]') : null;
  }

  function scan() {
    var block = mainBlock();
    if (!block) return false;
    var kids = Array.prototype.slice.call(block.children), cur = null, found = {};
    items = [];
    kids.forEach(function (it) {
      var own = false;
      try { own = window.frameElement && it.contains(window.frameElement); } catch (e) {}
      var a = it.querySelector(".pulse-anchor[id]");
      if (a && !own) { var t = pageOf(a.id); if (t) { cur = t; found[t] = true; } }
      items.push({ el: it, page: own ? null : cur });
    });
    return Object.keys(found).length >= 3;
  }

  function apply() {
    timer = null;
    if (!scan()) { enabled = false; items.forEach(function (x) { x.el.classList.remove("pz-off"); }); if (bar) bar.style.display = "none"; return; }
    enabled = true;
    if (!active || !known[active]) active = (hashPage() || pages[0].id);
    items.forEach(function (x) {
      var off = !all && x.page && x.page !== active;
      x.el.classList.toggle("pz-off", !!off);
    });
    drawBar();
  }
  function schedule() { if (timer) return; timer = setTimeout(apply, 120); }

  function hashPage() {
    var h = (P.location.hash || "").replace("#", "");
    return h ? pageOf(h) : null;
  }
  function idx() { for (var i = 0; i < pages.length; i++) if (pages[i].id === active) return i; return 0; }

  function drawBar() {
    if (!bar) {
      bar = D.createElement("nav");
      bar.id = "pz-bar";
      bar.setAttribute("aria-label", "Page navigation");
      bar.innerHTML = '<button type="button" class="pz-b" aria-label="Previous page">&#8249; Back</button>' +
        '<div class="pz-l" aria-live="polite"><span class="pz-n"></span><span class="pz-t"></span></div>' +
        '<button type="button" class="pz-f" aria-label="Next page">Next &#8250;</button>' +
        '<button type="button" class="pz-q"></button>';
      D.body.appendChild(bar);
      bar.querySelector(".pz-b").onclick = function () { go(pages[Math.max(idx() - 1, 0)].id); };
      bar.querySelector(".pz-f").onclick = function () { go(pages[Math.min(idx() + 1, pages.length - 1)].id); };
      bar.querySelector(".pz-q").onclick = function () {
        all = !all; try { P.sessionStorage.setItem("pzAll", all ? "1" : "0"); } catch (e) {}
        apply(); if (!all) go(active);
        else P.dispatchEvent(new P.Event("resize"));
      };
    }
    bar.style.display = "flex";
    var i = idx();
    bar.querySelector(".pz-n").textContent = all ? "All sections" : "Page " + (i + 1) + " of " + pages.length;
    bar.querySelector(".pz-t").textContent = all ? "Long scroll" : pages[i].label;
    bar.querySelector(".pz-b").disabled = all || i === 0;
    bar.querySelector(".pz-f").disabled = all || i === pages.length - 1;
    bar.querySelector(".pz-q").textContent = all ? "One page at a time" : "Show all sections";
  }

  function go(id, noScroll) {
    var t = pageOf(id); if (!t) return;
    if (all) { all = false; try { P.sessionStorage.setItem("pzAll", "0"); } catch (e) {} }
    active = t;
    try { P.history.replaceState(null, "", "#" + t); } catch (e) {}
    apply();
    if (!noScroll) {
      var first = null;
      for (var i = 0; i < items.length; i++) if (items[i].page === t) { first = items[i].el; break; }
      if (first) first.scrollIntoView({ behavior: REDUCE ? "auto" : "smooth", block: "start" });
    }
    // charts that were hidden need a fresh measure once they are visible
    setTimeout(function () { P.dispatchEvent(new P.Event("resize")); }, 60);
    setTimeout(function () { P.dispatchEvent(new P.Event("resize")); }, 400);
  }

  function onClick(e) {
    var a = e.target && e.target.closest ? e.target.closest('a[href^="#pulse-"]') : null;
    if (!a || !enabled) return;
    var t = pageOf(a.getAttribute("href").slice(1));
    if (t) { e.preventDefault(); e.stopPropagation(); go(t); }
  }
  function onMsg(e) {
    var d = e.data;
    if (d && typeof d.pzGo === "string" && enabled) go(d.pzGo);
  }
  function onKey(e) {
    if (!enabled || !e.altKey) return;
    if (e.key === "ArrowRight") go(pages[Math.min(idx() + 1, pages.length - 1)].id);
    if (e.key === "ArrowLeft") go(pages[Math.max(idx() - 1, 0)].id);
  }
  function onHash() { var t = hashPage(); if (t && enabled && t !== active) go(t); }

  D.addEventListener("click", onClick, true);
  P.addEventListener("message", onMsg);
  P.addEventListener("keydown", onKey);
  P.addEventListener("hashchange", onHash);
  var host = D.querySelector('[data-testid="stMainBlockContainer"]') || D.body;
  var mo = new P.MutationObserver(schedule);
  mo.observe(host, { childList: true, subtree: true });
  apply();
  var first = hashPage();
  if (first && enabled) go(first);

  P.__pzPager = {
    go: go,
    destroy: function () {
      D.removeEventListener("click", onClick, true);
      P.removeEventListener("message", onMsg); P.removeEventListener("keydown", onKey); P.removeEventListener("hashchange", onHash);
      mo.disconnect();
      items.forEach(function (x) { x.el.classList.remove("pz-off"); });
      if (bar && bar.parentNode) bar.parentNode.removeChild(bar);
      if (css.parentNode) css.parentNode.removeChild(css);
    }
  };
})();
