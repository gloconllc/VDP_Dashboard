/* Dana Point PULSE: one topic per page (pages added 2026-10-15, topics 2026-10-16).
   Runs inside a zero-height component and works on the Streamlit page around it.
   Nothing is removed or rewritten: whatever is not on the current page is hidden with
   a class, and "Show all sections" puts the long scroll back. If the page layout is
   not recognised (fewer than three sections found) it does nothing, so the page stays
   exactly as it was. Config is injected as window.PZ_CFG.

   Levels: a page is a section (At a glance, Performance, ...). Inside a board section,
   each question card is its own topic, with an overview first and the extras (At a glance
   strip, Ask about this section, map) last.

   The date window bar is kept at the top, tucks away while reading down the page, and
   returns when you scroll up. The duplicate row of section pills is hidden because the
   bar below and the sidebar list the same pages. */
(function () {
  "use strict";
  var P, D;
  try { P = window.parent; D = P.document; D.body; } catch (e) { return; }
  var CFG = window.PZ_CFG || { pages: [], alias: {} };
  if (P.__pzPager && P.__pzPager.destroy) { try { P.__pzPager.destroy(); } catch (e) {} }

  var allPages = CFG.pages.map(function (p) { return { id: p[0], label: p[1], after: p[2] || null }; });
  var pages = allPages.slice();
  var alias = CFG.alias || {};
  var combine = CFG.combine || [];
  var embeds = CFG.embeds || {};   // page id -> [[report section id, label], ...]
  var known = {}; allPages.forEach(function (p) { known[p.id] = true; });
  function pageOf(id) { var t = alias[id] || id; return known[t] ? t : null; }
  var REDUCE = P.matchMedia && P.matchMedia("(prefers-reduced-motion: reduce)").matches;

  var active = null, topic = 0, all = false, items = [], bar = null, timer = null, enabled = false, poll = null;
  var model = {};            // page id -> { topics: [{title, kind, idx}], cards: [els], root: shadowRoot, host: item }
  try { all = P.sessionStorage.getItem("pzAll") === "1"; } catch (e) {}

  var SHADOW_CSS = ".pz-off{display:none !important}.pz-solo{grid-column:1 / -1 !important;max-width:100% !important}";
  var css = D.createElement("style");
  css.id = "pz-pager-css";
  css.textContent =
    ".pz-off{display:none !important}" +
    ".pz-embed{margin:4px 0 24px}.pz-embed iframe{width:100%;border:0;display:block;background:transparent}" +
    ".pz-filter{position:sticky;top:0;z-index:60;background:#fff;transition:transform .22s ease;padding:6px 0 2px;border-bottom:1px solid #e2e8f0}" +
    ".pz-filter.pz-hide{transform:translateY(-130%);box-shadow:none;pointer-events:none}" +
    ".pz-filter.pz-stuck{box-shadow:0 4px 14px rgba(11,37,48,.10)}" +
    "@media (prefers-reduced-motion: reduce){.pz-filter{transition:none}}" +
    "#pz-bar{position:fixed;left:50%;transform:translateX(-50%);bottom:16px;z-index:999;display:flex;align-items:center;gap:10px;" +
    "background:#fff;border:1px solid #cbe3ea;border-radius:14px;padding:8px 10px;box-shadow:0 6px 24px rgba(11,37,48,.16);" +
    "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0b2530;max-width:calc(100vw - 24px)}" +
    "#pz-bar button{white-space:nowrap;font:inherit;font-weight:700;font-size:14px;min-height:44px;min-width:44px;padding:0 16px;border-radius:10px;cursor:pointer;" +
    "background:#1d6e86;color:#fff;border:1px solid #123c4a}" +
    "#pz-bar button:hover:not(:disabled){background:#123c4a}" +
    "#pz-bar button:disabled{opacity:.4;cursor:default}" +
    "#pz-bar button.pz-q{background:transparent;color:#0e7490;border-color:#cbe3ea;font-weight:600;font-size:13px;padding:0 12px}" +
    "#pz-bar button:focus-visible{outline:2px solid #1d6e86;outline-offset:2px}" +
    "#pz-bar .pz-l{display:flex;flex-direction:column;line-height:1.2;min-width:0;text-align:center}" +
    "#pz-bar .pz-n{font-size:11px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;color:#6b7280;white-space:nowrap}" +
    "#pz-bar .pz-t{font-family:Georgia,serif;font-style:italic;font-weight:700;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:34vw}" +
    "@media (max-width:640px){#pz-bar{gap:6px;padding:6px}#pz-bar button{padding:0 10px}#pz-bar .pz-t{max-width:30vw;font-size:14px}#pz-bar button.pz-q{display:none}}";
  D.head.appendChild(css);

  function mainBlock() {
    var c = D.querySelector('[data-testid="stMainBlockContainer"]') || D.querySelector('[data-testid="stMain"]');
    return c ? c.querySelector('[data-testid="stVerticalBlock"]') : null;
  }
  function cardTitle(c) {
    var t = (c.innerText || c.textContent || "").trim().split("\n")[0].replace(/\s+/g, " ");
    return t.length > 64 ? t.slice(0, 61) + "..." : t;
  }
  function ensureShadowCss(root) {
    if (root.querySelector("style[data-pz]")) return;
    var s = D.createElement("style"); s.setAttribute("data-pz", "1"); s.textContent = SHADOW_CSS; root.appendChild(s);
  }
  function findBoard(item) {
    var all_ = item.querySelectorAll("*");
    for (var i = 0; i < all_.length; i++) {
      var sr = all_[i].shadowRoot;
      if (sr && sr.querySelector(".pb-sec")) return sr;
    }
    return null;
  }

  function scan() {
    var block = mainBlock();
    if (!block) return false;
    // pages that exist only as Insights sections need the report on the page
    var haveReport = !!reportSrc();
    pages = allPages.filter(function (p) { return !p.after || haveReport; });
    known = {}; pages.forEach(function (p) { known[p.id] = true; });
    var kids = Array.prototype.slice.call(block.children), cur = null, found = {};
    items = [];
    model = {};
    kids.forEach(function (it) {
      var own = false;
      try { own = window.frameElement && it.contains(window.frameElement); } catch (e) {}
      var a = it.querySelector(".pulse-anchor[id]");
      if (a && !own) { var t = pageOf(a.id); if (t) { cur = t; found[t] = true; } }
      var rec = { el: it, page: own ? null : cur, board: null, nav: false, filter: false, hide: false, embed: it.classList && it.classList.contains("pz-embed") ? it.getAttribute("data-sec") : null, anchorOnly: !!a && !(it.innerText || "").trim() };
      if (!own && cur) {
        var sr = findBoard(it);
        if (sr) { rec.board = sr; ensureShadowCss(sr); }
        var links = it.querySelectorAll('a[href^="#pulse-"]');
        if (!rec.board && links.length >= 2 && !a && !it.querySelector("iframe")) rec.nav = true;
        // the full report viewer and its intro are retired as a page: the report now lives in the sections
        var ifr = it.querySelectorAll("iframe"), isRep = false;
        for (var q = 0; q < ifr.length; q++) { var sd = ifr[q].srcdoc || ""; if (sd.length > 50000 && sd.indexOf("sectionList") > -1) isRep = true; }
        var tx = (it.innerText || "");
        if (isRep || /Insights report: the full story/i.test(tx) || /Use the tabs or Previous and Next/i.test(tx)) rec.hide = true;
      }
      items.push(rec);
    });
    // the pills row sits before the first anchor (page null); catch it too
    items.forEach(function (x) {
      if (x.page === null || x.page === undefined) {
        var links = x.el.querySelectorAll('a[href^="#pulse-"]');
        if (links.length >= 5) x.nav = true;
        var rd = x.el.querySelector('[data-testid="stRadio"]');
        if (rd && /data window/i.test(rd.innerText || "")) x.filter = true;
      }
    });
    // the custom range pickers belong to the filter bar
    for (var i = 0; i < items.length; i++) {
      if (items[i].filter && items[i + 1] && items[i + 1].el.querySelector('[data-testid="stDateInput"]')) items[i + 1].filter = true;
    }
    // topics per page
    pages.forEach(function (p) { model[p.id] = { topics: [{ title: p.label, kind: "main" }], cards: [], root: null }; });
    items.forEach(function (x) {
      if (x.board && x.page && model[x.page]) {
        var cards = Array.prototype.slice.call(x.board.querySelectorAll(".pb-card"));
        if (cards.length && !model[x.page].root) {
          model[x.page].root = x.board; model[x.page].cards = cards;
          var tp = [{ title: "Overview", kind: "overview" }];
          cards.forEach(function (c, k) { tp.push({ title: cardTitle(c), kind: "card", idx: k }); });
          model[x.page].topics = tp;
        }
      }
    });
    pages.forEach(function (p) {
      var m = model[p.id];
      if (p.after) {
        var vem = embeds[p.id] || [];
        m.topics = vem.map(function (e) { return { title: e[1], kind: "embed", sec: e[0] }; });
        if (!m.topics.length) m.topics = [{ title: p.label, kind: "main" }];
        return;
      }
      var extras = items.some(function (x) { return x.page === p.id && !x.board && !x.embed && !x.anchorOnly && (x.el.innerText || "").trim() && !x.nav && !x.filter; });
      if (m.cards.length && extras) m.topics.push({ title: "More and ask", kind: "more" });
      else if (!m.cards.length) m.topics = [{ title: p.label, kind: "main" }];
      // the matching Insights sections, each as its own topic, before "More and ask"
      if (reportSrc()) {
        var em = embeds[p.id] || [], at = 0;   // the report section leads its page
        var add = em.map(function (e) { return { title: e[1], kind: "embed", sec: e[0] }; });
        Array.prototype.splice.apply(m.topics, [at, 0].concat(add));
        if (add.length && combine.indexOf(p.id) > -1 && !m.cards.length) m.topics = add;
      }
    });
    return Object.keys(found).length >= 3;
  }

  function setShadow(m, kind, k) {
    if (!m.root) return;
    var r = m.root;
    function each(sel, fn) { Array.prototype.forEach.call(r.querySelectorAll(sel), fn); }
    if (all) { each(".pz-off", function (e) { e.classList.remove("pz-off"); }); each(".pz-solo", function (e) { e.classList.remove("pz-solo"); }); return; }
    var overview = kind === "overview", card = kind === "card", more = kind === "more" || kind === "embed";
    each(".pb-kpis,.pb-ans", function (e) { e.classList.toggle("pz-off", !overview); });
    each(".pb-grid", function (e) { e.classList.toggle("pz-off", overview || more); });
    m.cards.forEach(function (c, i) {
      c.classList.toggle("pz-off", !(card && i === k));
      c.classList.toggle("pz-solo", card && i === k);
    });
    // keep the section title and question visible on every topic of the page
    each(".pb-sh,.pb-q", function (e) { e.classList.remove("pz-off"); });
  }

  // ---- Insights report, one section at a time (cloned from the report already on the page) ----
  var srcCache = null;
  function reportSrc() {
    if (srcCache) return srcCache;
    var fr = D.querySelectorAll("iframe");
    for (var i = 0; i < fr.length; i++) {
      var sd = fr[i].srcdoc || fr[i].getAttribute("srcdoc") || "";
      if (sd.length > 50000 && sd.indexOf("sectionList") > -1 && !fr[i].closest(".pz-embed")) { srcCache = sd; return sd; }
    }
    return null;
  }
  function injectFor(sec) {
    var css_ = "<style id=\"pz-embed-css\">.hdr-l,.tabs-bar,.pageline,#prev,#next,#one,#allp,#fs,.skip,.upnext{display:none !important}" +
      ".hdr{justify-content:flex-end !important;padding-block:6px !important}</style>";
    var js_ = "<script>(function(){var S=" + JSON.stringify(sec) + ";" +
      "function sel(){var t=document.querySelector('.tab[data-s=\"'+S+'\"]');if(t)t.click();}sel();" +
      "document.addEventListener('click',function(e){var g=e.target.closest&&e.target.closest('[data-go]');" +
      "if(g){e.preventDefault();e.stopPropagation();try{parent.postMessage({pzSection:g.getAttribute('data-go')},'*');}catch(x){}}},true);" +
      "document.addEventListener('click',function(e){if(e.target.closest&&e.target.closest('#exp')){setTimeout(function(){var d=document.querySelector('dialog[open]');" +
      "if(!d||!window.frameElement)return;var r=window.frameElement.getBoundingClientRect();var top=Math.max(0,-r.top)+48;" +
      "d.style.position='absolute';d.style.margin='0 auto';d.style.top=top+'px';d.style.left='0';d.style.right='0';},0);}},true);" +
      "})();<\/script>";
    var src = reportSrc(), k = src.lastIndexOf("</body>");
    return k > -1 ? src.slice(0, k) + css_ + js_ + src.slice(k) : src + css_ + js_;
  }
  function fit(f) {
    try {
      var d = f.contentDocument, m = d && d.getElementById("main");
      if (!m) return;
      var h = Math.ceil(m.getBoundingClientRect().bottom + (f.contentWindow.scrollY || 0)) + 90;
      if (h > 200) f.style.height = h + "px";
    } catch (e) {}
  }
  function ensureEmbed(pageId, sec) {
    var key = pageId + "|" + sec;
    var ex = D.querySelector('.pz-embed[data-sec="' + sec + '"]');
    if (ex) { var fx = ex.querySelector("iframe"); if (fx) fit(fx); return; }
    if (!reportSrc()) return;
    var last = null;
    var hostId = pageId;
    for (var j = 0; j < pages.length; j++) if (pages[j].id === pageId && pages[j].after) hostId = pages[j].after;
    for (var i = 0; i < items.length; i++) if (items[i].page === hostId) last = items[i];
    if (!last || !last.el.parentNode) return;
    var box = D.createElement("div");
    box.className = "pz-embed"; box.setAttribute("data-sec", sec);
    var f = D.createElement("iframe");
    f.title = "Insights report, " + sec; f.setAttribute("scrolling", "no"); f.style.height = "900px";
    f.srcdoc = injectFor(sec);
    f.onload = function () {
      fit(f);
      [300, 900, 2200].forEach(function (t) { setTimeout(function () { fit(f); }, t); });
      try { new P.ResizeObserver(function () { fit(f); }).observe(f.contentDocument.body); } catch (e) {}
    };
    box.appendChild(f);
    if (combine.indexOf(hostId) > -1) {
      var first = null;
      for (var q = 0; q < items.length; q++) if (items[q].page === hostId && !items[q].nav && !items[q].filter && !items[q].hide && !items[q].anchorOnly) { first = items[q]; break; }
      if (first && first.el.parentNode) { first.el.parentNode.insertBefore(box, first.el); schedule(); return; }
    }
    last.el.parentNode.insertBefore(box, last.el.nextSibling);
    schedule();
  }
  function goSection(sec) {
    for (var i = 0; i < pages.length; i++) {
      var tp = model[pages[i].id].topics;
      for (var k = 0; k < tp.length; k++) if (tp[k].kind === "embed" && tp[k].sec === sec) return jump(pages[i].id, k);
    }
    go("pulse-insights");
  }

  function apply() {
    timer = null;
    if (!scan()) {
      enabled = false;
      items.forEach(function (x) { x.el.classList.remove("pz-off"); });
      if (bar) bar.style.display = "none";
      return;
    }
    enabled = true;
    if (!active || !known[active]) active = (hashPage() || pages[0].id);
    var m = model[active] || { topics: [{ kind: "main" }] };
    if (topic >= m.topics.length) topic = m.topics.length - 1;
    if (topic < 0) topic = 0;
    var cur = m.topics[topic] || { kind: "main" };
    items.forEach(function (x) {
      var off = false;
      if (x.embed) off = all || cur.kind !== "embed" || cur.sec !== x.embed;
      else if (!all && x.hide) off = true;
      else if (!all) {
        if (x.nav) off = true;
        else if (cur.kind === "embed" && x.page === active && combine.indexOf(active) > -1) off = false;
        else if (cur.kind === "embed" && x.page === active) off = !(x.board && m.cards.length) && !x.filter;
        else if (x.page && x.page !== active) off = true;
        else if (x.page === active && m.cards.length && cur.kind !== "embed") {
          // board pages: the board chunk shows on every topic; extras only on the last
          if (!x.board && !x.anchorOnly && cur.kind !== "more") off = true;
        }
      }
      x.el.classList.toggle("pz-off", off);
      x.el.classList.toggle("pz-filter", !!x.filter);
    });
    pages.forEach(function (p) { setShadow(model[p.id], p.id === active ? cur.kind : "overview", cur.idx || 0); });
    if (cur.kind === "embed" && !all) ensureEmbed(active, cur.sec);
    drawBar();
    P.dispatchEvent(new P.Event("resize"));
  }
  function schedule() { if (timer) return; timer = setTimeout(apply, 120); }

  function hashPage() {
    var h = (P.location.hash || "").replace("#", "");
    return h ? pageOf(h) : null;
  }
  function idx() { for (var i = 0; i < pages.length; i++) if (pages[i].id === active) return i; return 0; }
  function steps() { var n = 0; pages.forEach(function (p) { n += model[p.id].topics.length; }); return n; }
  function pos() { var n = 0; for (var i = 0; i < pages.length; i++) { if (pages[i].id === active) return n + topic; n += model[pages[i].id].topics.length; } return 0; }
  function stepBy(d) {
    var m = model[active], i = idx();
    if (d > 0) {
      if (topic < m.topics.length - 1) return jump(active, topic + 1);
      if (i < pages.length - 1) return jump(pages[i + 1].id, 0);
    } else {
      if (topic > 0) return jump(active, topic - 1);
      if (i > 0) { var pm = model[pages[i - 1].id]; return jump(pages[i - 1].id, pm.topics.length - 1); }
    }
  }

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
      bar.querySelector(".pz-b").onclick = function () { stepBy(-1); };
      bar.querySelector(".pz-f").onclick = function () { stepBy(1); };
      bar.querySelector(".pz-q").onclick = function () {
        all = !all; try { P.sessionStorage.setItem("pzAll", all ? "1" : "0"); } catch (e) {}
        apply(); if (!all) jump(active, topic);
        else P.dispatchEvent(new P.Event("resize"));
      };
    }
    bar.style.display = "flex";
    var m = model[active], t = m.topics[topic] || { title: pages[idx()].label };
    var multi = m.topics.length > 1;
    bar.querySelector(".pz-n").textContent = all ? "All sections" :
      pages[idx()].label + (multi ? " · " + (topic + 1) + " of " + m.topics.length : "");
    bar.querySelector(".pz-t").textContent = all ? "Long scroll" : ((multi && topic > 0) || t.kind === "embed" ? t.title : pages[idx()].label);
    bar.querySelector(".pz-b").disabled = all || pos() === 0;
    bar.querySelector(".pz-f").disabled = all || pos() === steps() - 1;
    bar.querySelector(".pz-q").textContent = all ? "One page at a time" : "Show all sections";
  }

  function jump(id, k, noScroll) {
    var t = pageOf(id); if (!t) return;
    if (all) { all = false; try { P.sessionStorage.setItem("pzAll", "0"); } catch (e) {} }
    active = t; topic = k || 0;
    try { P.history.replaceState(null, "", "#" + t); } catch (e) {}
    apply();
    if (!noScroll) scrollTopOfPage();
    setTimeout(function () { P.dispatchEvent(new P.Event("resize")); }, 60);
    setTimeout(function () { P.dispatchEvent(new P.Event("resize")); }, 400);
  }
  function go(id, noScroll) { jump(id, 0, noScroll); }
  function scrollTopOfPage() {
    var first = null;
    for (var i = 0; i < items.length; i++) if (items[i].page === active && !items[i].el.classList.contains("pz-off") && !items[i].anchorOnly) { first = items[i].el; break; }
    if (first) first.scrollIntoView({ behavior: REDUCE ? "auto" : "smooth", block: "start" });
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
    if (d && typeof d.pzSection === "string" && enabled) goSection(d.pzSection);
  }
  function onKey(e) {
    if (!enabled || !e.altKey) return;
    if (e.key === "ArrowRight") stepBy(1);
    if (e.key === "ArrowLeft") stepBy(-1);
  }
  function onHash() { var t = hashPage(); if (t && enabled && t !== active) go(t); }

  // Date window bar: tucks away while reading down, returns on scroll up
  var lastY = 0;
  function scroller() { return D.querySelector('[data-testid="stMain"]') || D.querySelector("section.main") || null; }
  function onScroll(e) {
    var s = scroller(); if (!s) return;
    var tgt = e && e.target;
    if (tgt && tgt !== s && tgt !== D && tgt !== D.documentElement) return;
    var y = s.scrollTop || 0;
    var bars = D.querySelectorAll(".pz-filter");
    bars.forEach(function (b) {
      b.classList.toggle("pz-stuck", y > 8);
      if (y > 120 && y > lastY + 4) b.classList.add("pz-hide");
      else if (y < lastY - 4 || y <= 120) b.classList.remove("pz-hide");
    });
    lastY = y;
  }

  D.addEventListener("click", onClick, true);
  D.addEventListener("scroll", onScroll, true);
  P.addEventListener("message", onMsg);
  P.addEventListener("keydown", onKey);
  P.addEventListener("hashchange", onHash);
  var host = D.querySelector('[data-testid="stMainBlockContainer"]') || D.body;
  var mo = new P.MutationObserver(schedule);
  mo.observe(host, { childList: true, subtree: true });
  // board cards are drawn inside shadow roots after the page loads; keep checking for a while
  var polls = 0;
  poll = setInterval(function () { polls++; apply(); if (polls > 30) { clearInterval(poll); poll = null; } }, 1000);
  apply();
  var first = hashPage();
  if (first && enabled) go(first, true);

  P.__pzPager = {
    go: go, step: stepBy,
    destroy: function () {
      D.removeEventListener("click", onClick, true); D.removeEventListener("scroll", onScroll, true);
      P.removeEventListener("message", onMsg); P.removeEventListener("keydown", onKey); P.removeEventListener("hashchange", onHash);
      mo.disconnect(); if (poll) clearInterval(poll);
      items.forEach(function (x) { x.el.classList.remove("pz-off"); x.el.classList.remove("pz-filter"); });
      pages.forEach(function (p) { var m = model[p.id]; if (m && m.root) { all = true; setShadow(m, "overview", 0); } });
      if (bar && bar.parentNode) bar.parentNode.removeChild(bar);
      if (css.parentNode) css.parentNode.removeChild(css);
    }
  };
})();
