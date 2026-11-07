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
VDP Analytics, Interactive Visual Components
Narrative boxes, blob loaders, shader wallpapers, iridescent data cards,
fun facts sprite, monthly highlights, topic animations.
"""

import json
import streamlit as st
import streamlit.components.v1 as components


def render_narrative_box(tab_id: str, sample_text: str, height: int = 400) -> None:
    """
    Render a large editable text box with canvas particle effects for keywords.
    Keywords: 'fire' → embers, 'smoke' → puffs, 'metal' → sparks, 'wind' → streaks
    """
    html_template = """
<div style="position:relative; width:100%; height:%%HEIGHT%%px; font-family:sans-serif; background:#0F1419; border-radius:12px; overflow:hidden; border:1px solid #1E293B;">
  <canvas id="particles-%%TID%%" style="position:absolute; top:0; left:0; width:100%; height:100%; z-index:3; pointer-events:none;"></canvas>
  <textarea id="narrative-%%TID%%" style="position:absolute; top:0; left:0; width:100%; height:100%; padding:16px; border:0; border-radius:12px; background:rgba(15,20,25,0.7); color:#E2E8F0; font-size:14px; font-family:'DM Sans',sans-serif; resize:none; z-index:1; outline:none; box-sizing:border-box; line-height:1.6;" placeholder="Enter narrative...">%%STEXT%%</textarea>
</div>
<script>
(function() {
  const TID = '%%TID%%';
  const canvas = document.getElementById('particles-' + TID);
  const textarea = document.getElementById('narrative-' + TID);
  if (!canvas || !textarea) return;

  const ctx = canvas.getContext('2d');
  let particles = [];
  const particleMax = 180;

  function resizeCanvas() {
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width;
    canvas.height = rect.height;
  }
  resizeCanvas();
  window.addEventListener('resize', resizeCanvas);

  const keywords = {
    fire: /\\b(fire|flame|blaze|heat|burn|inferno)\\b/gi,
    smoke: /\\b(smoke|fog|haze|mist|vapor)\\b/gi,
    metal: /\\b(metal|steel|iron|copper|gold|silver|aluminum)\\b/gi,
    wind: /\\b(wind|breeze|gust|air|drift)\\b/gi,
  };

  const colors = {
    fire: ['#FF6B6B', '#FF8C42', '#FFA366'],
    smoke: ['#64748B', '#475569', '#94A3B8'],
    metal: ['#CBD5E1', '#E2E8F0', '#F1F5F9'],
    wind: ['#0567C8', '#00D4C8', '#32B8C6'],
  };

  function detectKeywords(text) {
    const events = [];
    for (const [type, pattern] of Object.entries(keywords)) {
      let match;
      while ((match = pattern.exec(text)) !== null) {
        events.push({ type, pos: match.index });
      }
    }
    return events;
  }

  function emitParticles(type, count = 5) {
    const typeMap = { fire: 'e', smoke: 's', metal: 'k', wind: 'w' };
    const t = typeMap[type];
    const cols = colors[type];
    for (let i = 0; i < count; i++) {
      const p = {
        x: Math.random() * canvas.width,
        y: Math.random() * canvas.height,
        vx: (Math.random() - 0.5) * (type === 'wind' ? 3 : 1.5),
        vy: (Math.random() - 0.5) * (type === 'wind' ? 0.5 : 2),
        life: 1,
        t: t,
        col: cols[Math.floor(Math.random() * cols.length)],
      };
      particles.push(p);
    }
  }

  textarea.addEventListener('input', function() {
    const text = this.value;
    const events = detectKeywords(text);
    for (const ev of events) {
      if (Math.random() < 0.1) emitParticles(ev.type, 2);
    }
  });

  function updateParticles() {
    for (let i = particles.length - 1; i >= 0; i--) {
      const p = particles[i];
      p.x += p.vx;
      p.y += p.vy;
      p.life -= 0.01;
      p.vy += 0.15;
      if (p.life <= 0) particles.splice(i, 1);
    }
    if (particles.length > particleMax) particles.splice(0, particles.length - particleMax);
  }

  function drawParticles() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    for (const p of particles) {
      ctx.globalAlpha = p.life;
      ctx.fillStyle = p.col;
      if (p.t === 'e') {
        ctx.fillRect(p.x - 3, p.y - 3, 6, 6);
      } else if (p.t === 's') {
        ctx.beginPath();
        ctx.arc(p.x, p.y, Math.max(2, 8 * p.life), 0, Math.PI * 2);
        ctx.fill();
      } else if (p.t === 'k') {
        ctx.beginPath();
        ctx.moveTo(p.x, p.y - 4);
        ctx.lineTo(p.x + 4, p.y + 4);
        ctx.lineTo(p.x - 4, p.y + 4);
        ctx.closePath();
        ctx.fill();
      } else if (p.t === 'w') {
        ctx.strokeStyle = p.col;
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(p.x - 8, p.y);
        ctx.lineTo(p.x + 8, p.y);
        ctx.stroke();
      }
    }
    ctx.globalAlpha = 1;
  }

  function animate() {
    updateParticles();
    drawParticles();
    requestAnimationFrame(animate);
  }
  animate();
})();
</script>
"""

    html = html_template.replace("%%TID%%", tab_id).replace("%%HEIGHT%%", str(height)).replace("%%STEXT%%", sample_text)
    components.html(html, height=height + 40)


def render_kpi_blob_loaders(height: int = 560) -> None:
    """Render 50 unique CSS blob loading indicators in a wrapping grid."""

    html = f"""
<div style="width:100%; padding:16px; background:rgba(10,20,30,0.5); border-radius:12px;">
  <style>
    .blob-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(64px, 1fr));
      gap: 16px;
      padding: 16px;
      background: linear-gradient(135deg, rgba(0,212,200,0.04) 0%, rgba(5,103,200,0.04) 100%);
      border-radius: 8px;
    }}
    /* base, no animation shorthand here to avoid override conflicts */
    .blob {{
      width: 64px;
      height: 64px;
      position: relative;
      animation-timing-function: ease-in-out;
      animation-iteration-count: infinite;
      animation-fill-mode: both;
    }}
    @keyframes blob-breathe {{
      0%, 100% {{ border-radius: 60% 40% 30% 70% / 60% 30% 70% 40%; transform: scale(1); }}
      50%       {{ border-radius: 30% 60% 70% 40% / 40% 60% 30% 70%; transform: scale(1.05); }}
    }}
    @keyframes blob-morph {{
      0%   {{ border-radius: 60% 40% 30% 70% / 60% 30% 70% 40%; }}
      25%  {{ border-radius: 40% 60% 70% 30% / 70% 60% 40% 30%; }}
      50%  {{ border-radius: 70% 30% 60% 40% / 30% 70% 60% 40%; }}
      75%  {{ border-radius: 40% 70% 40% 60% / 60% 40% 70% 30%; }}
      100% {{ border-radius: 60% 40% 30% 70% / 60% 30% 70% 40%; }}
    }}
    @keyframes blob-pulse {{
      0%, 100% {{ transform: scale(0.95); border-radius: 50%; }}
      50%       {{ transform: scale(1.10); border-radius: 40% 60% 60% 40% / 40% 40% 60% 60%; }}
    }}
    @keyframes blob-squish {{
      0%, 100% {{ transform: scaleY(1)   scaleX(1);   border-radius: 60% 40% 40% 60% / 60% 60% 40% 40%; }}
      50%       {{ transform: scaleY(1.2) scaleX(0.85); border-radius: 40% 60% 60% 40% / 70% 30% 70% 30%; }}
    }}
    @keyframes blob-drift {{
      0%   {{ border-radius: 60% 40% 30% 70% / 60% 30% 70% 40%; transform: translateY(0px); }}
      33%  {{ border-radius: 30% 60% 70% 40% / 50% 70% 30% 50%; transform: translateY(-6px); }}
      66%  {{ border-radius: 70% 30% 40% 60% / 40% 60% 50% 50%; transform: translateY(4px); }}
      100% {{ border-radius: 60% 40% 30% 70% / 60% 30% 70% 40%; transform: translateY(0px); }}
    }}
    /* each variant: full animation shorthand, name, duration only needed */
    .blob1  {{ background:#00D4C8; border-radius:60% 40% 30% 70%/60% 30% 70% 40%; animation-name:blob-breathe; animation-duration:3.8s; }}
    .blob2  {{ background:#0567C8; border-radius:40% 60% 70% 30%/70% 60% 40% 30%; animation-name:blob-morph;   animation-duration:5.2s; }}
    .blob3  {{ background:#21808D; border-radius:50% 50% 40% 60%/60% 40% 50% 50%; animation-name:blob-pulse;   animation-duration:2.9s; }}
    .blob4  {{ background:#10B981; border-radius:60% 40% 60% 40%/40% 60% 40% 60%; animation-name:blob-squish;  animation-duration:3.4s; }}
    .blob5  {{ background:#FF8C42; border-radius:70% 30% 50% 50%/50% 50% 70% 30%; animation-name:blob-drift;   animation-duration:4.6s; }}
    .blob6  {{ background:#32B8C6; border-radius:30% 70% 40% 60%/60% 40% 60% 40%; animation-name:blob-morph;   animation-duration:4.1s; }}
    .blob7  {{ background:#0A3D54; border-radius:50% 50% 60% 40%/40% 60% 50% 50%; animation-name:blob-breathe; animation-duration:5.5s; }}
    .blob8  {{ background:#21808D; border-radius:40% 60% 30% 70%/70% 30% 60% 40%; animation-name:blob-squish;  animation-duration:4.8s; }}
    .blob9  {{ background:#10B981; border-radius:60% 40% 70% 30%/30% 70% 40% 60%; animation-name:blob-pulse;   animation-duration:3.1s; }}
    .blob10 {{ background:#FF8C42; border-radius:50% 50% 40% 60%/60% 40% 70% 30%; animation-name:blob-drift;   animation-duration:6.0s; }}
  </style>
  <div class="blob-grid">
    {"".join(f'<div class="blob blob{(i%10)+1}" style="animation-delay:{i*0.12:.2f}s;"></div>' for i in range(50))}
  </div>
</div>
"""
    components.html(html, height=height)


def inject_shader_wallpaper() -> None:
    """Inject a full-page interactive shader wallpaper that reacts to mouse position."""

    shader_html = """
<canvas id="vdp-bg-shader" style="position:fixed; top:0; left:0; width:100%; height:100%; z-index:-1; pointer-events:none;"></canvas>
<script>
(function() {
  if (window.__vdpBgInit) return;
  window.__vdpBgInit = true;

  const canvas = document.getElementById('vdp-bg-shader');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  function resize() {
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
  }
  resize();
  window.addEventListener('resize', resize);

  let mouseX = canvas.width / 2;
  let mouseY = canvas.height / 2;
  document.addEventListener('mousemove', (e) => {
    mouseX = e.clientX;
    mouseY = e.clientY;
  });

  const colors = [
    { r: 0, g: 212, b: 200 },   // #00D4C8
    { r: 10, g: 61, b: 84 },    // #0A3D54
    { r: 33, g: 128, b: 141 },  // #21808D
    { r: 5, g: 103, b: 200 },   // #0567C8
  ];

  function drawGradient() {
    const grd = ctx.createRadialGradient(mouseX, mouseY, 0, mouseX, mouseY, 800);
    grd.addColorStop(0, 'rgba(0, 212, 200, 0.15)');
    grd.addColorStop(0.5, 'rgba(10, 61, 84, 0.08)');
    grd.addColorStop(1, 'rgba(33, 128, 141, 0.02)');
    ctx.fillStyle = grd;
    ctx.fillRect(0, 0, canvas.width, canvas.height);
  }

  function animate() {
    ctx.fillStyle = 'rgba(8, 12, 18, 0.05)';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    drawGradient();
    requestAnimationFrame(animate);
  }
  animate();
})();
</script>
"""

    st.markdown(shader_html, unsafe_allow_html=True)


def render_mono_cards(cards_data: list, tab_id: str, height: int = 280) -> None:
    """
    Render iridescent monochromatic data cards with 3D perspective hover effect.
    cards_data: list of dicts with keys 'label', 'value', 'unit'
    """

    card_html_items = []
    for i, card in enumerate(cards_data):
        label = card.get('label', f'Card {i}')
        value = card.get('value', '0')
        unit = card.get('unit', '')

        card_html_items.append(f"""
<div class="mono-card" style="animation-delay: {i*0.05}s;">
  <div class="mono-card-inner">
    <div class="mono-card-content">
      <div class="mono-label">{label}</div>
      <div class="mono-value">{value}</div>
      <div class="mono-unit">{unit}</div>
    </div>
  </div>
</div>
""")

    html = f"""
<div style="width:100%; padding:16px; background:rgba(10,20,30,0.3); border-radius:12px;">
  <style>
    .mono-cards-container {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      perspective: 1000px;
    }}
    .mono-card {{
      aspect-ratio: 1;
      background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
      border-radius: 12px;
      padding: 16px;
      border: 1px solid rgba(226, 232, 240, 0.1);
      cursor: pointer;
      transform-style: preserve-3d;
      transition: all 0.4s cubic-bezier(0.23, 1, 0.320, 1);
      position: relative;
      overflow: hidden;
    }}
    .mono-card::before {{
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: radial-gradient(circle at var(--mouse-x, 50%) var(--mouse-y, 50%),
                                  rgba(226, 232, 240, 0.2) 0%,
                                  transparent 50%);
      opacity: 0;
      transition: opacity 0.3s;
      pointer-events: none;
    }}
    .mono-card:hover {{
      transform: perspective(1000px) rotateX(5deg) rotateY(5deg) translateZ(10px);
      border-color: rgba(226, 232, 240, 0.3);
      box-shadow: 0 20px 40px rgba(0, 212, 200, 0.1);
    }}
    .mono-card:hover::before {{
      opacity: 1;
    }}
    .mono-card-inner {{
      width: 100%;
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      text-align: center;
    }}
    .mono-label {{
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: rgba(226, 232, 240, 0.6);
      margin-bottom: 8px;
    }}
    .mono-value {{
      font-size: 20px;
      font-weight: 700;
      color: #E2E8F0;
      font-family: 'Helvetica Neue', Helvetica, sans-serif;
      letter-spacing: -0.02em;
    }}
    .mono-unit {{
      font-size: 10px;
      color: rgba(226, 232, 240, 0.5);
      margin-top: 4px;
    }}
  </style>
  <div class="mono-cards-container">
    {"".join(card_html_items)}
  </div>
</div>
<script>
document.querySelectorAll('.mono-card').forEach(card => {{
  card.addEventListener('mousemove', (e) => {{
    const rect = card.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width * 100;
    const y = (e.clientY - rect.top) / rect.height * 100;
    card.style.setProperty('--mouse-x', x + '%');
    card.style.setProperty('--mouse-y', y + '%');
  }});
}});
</script>
"""

    components.html(html, height=height + 40)


def render_fun_facts_sprite(facts: list, height: int = 480) -> None:
    """
    Canvas sprite animation, circles of various sizes orbit with fun facts.
    facts: list of {"text": str, "value": str} dicts from real DB data.
    Monochromatic VDP palette, Helvetica-style font.
    """
    facts_json = json.dumps(facts)

    html = """
<canvas id="vdp-sprite-sf" style="width:100%;height:%%HEIGHT%%px;border-radius:12px;background:#060d14;display:block;"></canvas>
<script>
(function(){
  const canvas = document.getElementById('vdp-sprite-sf');
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  const DPR = window.devicePixelRatio || 1;
  const W = canvas.parentElement.clientWidth || 800;
  const H = %%HEIGHT%%;
  canvas.width  = W * DPR;
  canvas.height = H * DPR;
  canvas.style.width  = W + 'px';
  canvas.style.height = H + 'px';
  ctx.scale(DPR, DPR);

  const FACTS = %%FACTS_JSON%%;
  const PAL   = ['#00D4C8','#21808D','#0567C8','#4A9EAD','#E2E8F0','#64748B','#32B8C6'];

  const bubbles = FACTS.map((f, i) => {
    const r = 34 + Math.random() * 52;
    return {
      x: r + Math.random() * (W - 2*r),
      y: r + Math.random() * (H - 2*r),
      r,
      vx: (Math.random()-.5)*0.55,
      vy: (Math.random()-.5)*0.55,
      col: PAL[i % PAL.length],
      fact: f,
      alpha: 0,
      phase: Math.random() * Math.PI * 2,
      born: performance.now() + i * 220,
    };
  });

  const ambient = Array.from({length: 18}, () => ({
    x: Math.random()*W, y: Math.random()*H,
    r: 3 + Math.random()*12,
    vx:(Math.random()-.5)*.3, vy:(Math.random()-.5)*.3,
    col: PAL[Math.floor(Math.random()*PAL.length)],
    a: 0.06 + Math.random()*0.12,
  }));

  function wrapText(ctx, text, x, y, maxW, lineH) {
    const words = text.split(' ');
    let line = '';
    const lines = [];
    for(const w of words){
      const test = line ? line+' '+w : w;
      if(ctx.measureText(test).width > maxW && line){ lines.push(line); line=w; }
      else line = test;
    }
    lines.push(line);
    const top = y - (lines.length*lineH)/2;
    lines.forEach((l,i)=> ctx.fillText(l, x, top + i*lineH + lineH/2));
  }

  function drawBubble(b, now) {
    if(now < b.born){ b.alpha = 0; return; }
    b.alpha = Math.min(1, (now - b.born)/800);
    b.x += b.vx; b.y += b.vy;
    if(b.x < b.r || b.x > W-b.r) b.vx *= -1;
    if(b.y < b.r || b.y > H-b.r) b.vy *= -1;
    const pulse = 1 + 0.04*Math.sin(now/900 + b.phase);
    const r = b.r * pulse;
    ctx.save();
    ctx.globalAlpha = b.alpha * 0.92;
    const grd = ctx.createRadialGradient(b.x, b.y, 0, b.x, b.y, r*1.6);
    grd.addColorStop(0, b.col+'33');
    grd.addColorStop(1, 'transparent');
    ctx.fillStyle = grd;
    ctx.beginPath(); ctx.arc(b.x, b.y, r*1.6, 0, Math.PI*2); ctx.fill();
    ctx.beginPath(); ctx.arc(b.x, b.y, r, 0, Math.PI*2);
    ctx.strokeStyle = b.col+'88'; ctx.lineWidth = 1.5;
    ctx.fillStyle = '#0a1520ee';
    ctx.fill(); ctx.stroke();
    ctx.globalAlpha = b.alpha;
    ctx.fillStyle = b.col;
    ctx.font = `700 ${Math.round(r*0.38)}px 'Helvetica Neue',Helvetica,sans-serif`;
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    if(b.fact.value) ctx.fillText(b.fact.value, b.x, b.y - r*0.16);
    ctx.fillStyle = '#CBD5E1';
    const fs = Math.max(9, Math.round(r*0.19));
    ctx.font = `500 ${fs}px 'Helvetica Neue',Helvetica,sans-serif`;
    wrapText(ctx, b.fact.text, b.x, b.y + r*0.28, r*1.7, fs*1.25);
    ctx.restore();
  }

  function frame(now) {
    ctx.clearRect(0,0,W,H);
    for(const a of ambient){
      a.x+=a.vx; a.y+=a.vy;
      if(a.x<0||a.x>W) a.vx*=-1;
      if(a.y<0||a.y>H) a.vy*=-1;
      ctx.globalAlpha=a.a;
      ctx.fillStyle=a.col;
      ctx.beginPath(); ctx.arc(a.x,a.y,a.r,0,Math.PI*2); ctx.fill();
    }
    ctx.globalAlpha=1;
    for(const b of bubbles) drawBubble(b, now);
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
})();
</script>
""".replace("%%HEIGHT%%", str(height)).replace("%%FACTS_JSON%%", facts_json)

    components.html(html, height=height + 8)


def render_monthly_highlights(insights: list, height: int = 260) -> None:
    """
    Display top monthly insights from insights_daily with animated entrance cards.
    insights: list of dicts with keys headline, body, category, audience.
    """
    if not insights:
        st.markdown(
            "<div style='padding:24px;text-align:center;color:#5A7A95;"
            "font-size:13px;border:1px dashed rgba(0,212,200,0.2);"
            "border-radius:12px;margin:8px 0;'>"
            "◈ No insights available, run the pipeline to generate forward-looking analysis."
            "</div>",
            unsafe_allow_html=True,
        )
        return

    icon_map = {
        "demand_trend": "📈", "tbid_projection": "💰", "compression_outlook": "🔴",
        "event_roi": "🎉", "tot_revenue": "🏛️", "feeder_market": "🗺️",
        "visitor_profile": "👥", "economic_impact": "💹", "best_value": "⭐",
        "rate_outlook": "🏷️", "upcoming_events": "📅", "peak_alert": "⚠️",
        "feeder_value_gap": "🔍", "daytrip_conversion": "🚗", "oos_adr_premium": "✈️",
        "compression_daytrip": "🏖️",
    }

    cards_html = ""
    for i, ins in enumerate(insights[:3]):
        icon  = icon_map.get(ins.get("category", ""), "◈")
        aud   = str(ins.get("audience", "")).upper()
        hl    = ins.get("headline", "")
        body  = ins.get("body", "")
        body_short = body[:160] + ("…" if len(body) > 160 else "")
        delay = i * 120

        cards_html += f"""
<div style="flex:1;min-width:220px;
  background:linear-gradient(135deg,#0d1f2d 0%,#091623 100%);
  border:1px solid rgba(0,212,200,0.18);border-radius:14px;padding:18px 16px;
  animation:hl-slide-in 0.5s {delay}ms both;position:relative;overflow:hidden;">
  <div style="position:absolute;top:-18px;right:-18px;font-size:64px;opacity:.06;line-height:1;">{icon}</div>
  <div style="font-size:9px;font-weight:800;letter-spacing:.12em;color:#00D4C8;
              text-transform:uppercase;margin-bottom:6px;">{aud} · {ins.get("category","").replace("_"," ").title()}</div>
  <div style="font-size:14px;font-weight:700;color:#E2E8F0;line-height:1.35;margin-bottom:8px;">{icon} {hl}</div>
  <div style="font-size:12px;color:#94A3B8;line-height:1.5;">{body_short}</div>
</div>"""

    html = f"""
<style>
@keyframes hl-slide-in {{
  from {{ opacity:0; transform: translateY(14px); }}
  to   {{ opacity:1; transform: translateY(0); }}
}}
</style>
<div style="display:flex;gap:12px;flex-wrap:wrap;padding:4px 0;">
{cards_html}
</div>
"""
    st.markdown(html, unsafe_allow_html=True)


def render_topic_animation(topic: str, data_points: list, height: int = 420) -> None:
    """
    Canvas-based animated data video for a chosen topic.
    topic: display string, data_points: list of {"label":str,"value":str} dicts.
    Loops through slides with smooth transitions. VDP monochromatic palette.
    """
    dp_json  = json.dumps(data_points)
    topic_js = json.dumps(topic)

    html = """
<div style="position:relative;border-radius:16px;overflow:hidden;background:#040c14;">
  <canvas id="vdp-topic-tv"
    style="display:block;width:100%;height:%%HEIGHT%%px;"></canvas>
  <div style="position:absolute;bottom:10px;right:14px;display:flex;gap:8px;z-index:10;">
    <button id="btn-play-tv"
      style="background:#00D4C888;border:0;border-radius:6px;color:#fff;
             font-size:11px;font-weight:700;padding:4px 10px;cursor:pointer;">
      ⏸ Pause
    </button>
    <button id="btn-export-tv"
      style="background:#0567C888;border:0;border-radius:6px;color:#fff;
             font-size:11px;font-weight:700;padding:4px 10px;cursor:pointer;">
      ↗ Share
    </button>
  </div>
</div>
<script>
(function(){
  const canvas  = document.getElementById('vdp-topic-tv');
  const btnPlay = document.getElementById('btn-play-tv');
  const btnExp  = document.getElementById('btn-export-tv');
  if(!canvas) return;
  const ctx = canvas.getContext('2d');
  const DPR = window.devicePixelRatio || 1;
  const W   = canvas.parentElement.clientWidth || 820;
  const H   = %%HEIGHT%%;
  canvas.width  = W * DPR;
  canvas.height = H * DPR;
  canvas.style.width  = W + 'px';
  canvas.style.height = H + 'px';
  ctx.scale(DPR, DPR);

  const TOPIC  = %%TOPIC_JS%%;
  const POINTS = %%DP_JSON%%;
  const PAL    = ['#00D4C8','#21808D','#32B8C6','#0567C8','#4A9EAD'];
  let playing  = true;
  let slideIdx = 0;
  let slideT   = 0;
  const SLIDE_DUR = 2800;

  const particles = Array.from({length:28},()=>({
    x: Math.random()*W, y: Math.random()*H,
    r: 4 + Math.random()*22,
    vx:(Math.random()-.5)*.4, vy:(Math.random()-.5)*.4,
    col: PAL[Math.floor(Math.random()*PAL.length)],
    a: 0.04+Math.random()*0.08,
  }));

  function ease(t){ return t<.5 ? 2*t*t : -1+(4-2*t)*t; }

  function drawBg(t){
    const grd = ctx.createLinearGradient(0,0,W,H);
    grd.addColorStop(0,'#04101a'); grd.addColorStop(1,'#061622');
    ctx.fillStyle=grd; ctx.fillRect(0,0,W,H);
    for(const p of particles){
      if(playing){ p.x+=p.vx; p.y+=p.vy; }
      if(p.x<0||p.x>W) p.vx*=-1;
      if(p.y<0||p.y>H) p.vy*=-1;
      ctx.globalAlpha=p.a; ctx.fillStyle=p.col;
      ctx.beginPath(); ctx.arc(p.x,p.y,p.r,0,Math.PI*2); ctx.fill();
    }
    ctx.globalAlpha=1;
    const sl = (t/1200) % H;
    const slG = ctx.createLinearGradient(0,sl-4,0,sl+4);
    slG.addColorStop(0,'transparent');
    slG.addColorStop(.5,'rgba(0,212,200,0.06)');
    slG.addColorStop(1,'transparent');
    ctx.fillStyle=slG; ctx.fillRect(0,sl-4,W,8);
  }

  function drawSlide(pt, progress){
    const alpha = ease(Math.min(progress*2,1)) * ease(Math.min((1-progress)*2,1));
    const yOff  = (1 - ease(Math.min(progress*3,1))) * 28;
    ctx.save();
    ctx.globalAlpha = alpha;
    ctx.translate(0, yOff);
    ctx.font='600 11px "Helvetica Neue",Helvetica,sans-serif';
    ctx.fillStyle='#00D4C8'; ctx.textAlign='left'; ctx.textBaseline='top';
    ctx.fillText(TOPIC.toUpperCase(), 28, 28);
    ctx.strokeStyle='#00D4C844'; ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(28,46); ctx.lineTo(W-28,46); ctx.stroke();
    const valSize = Math.min(64, W/9);
    ctx.font=`800 ${valSize}px "Helvetica Neue",Helvetica,sans-serif`;
    ctx.fillStyle='#E2E8F0'; ctx.textAlign='center'; ctx.textBaseline='middle';
    ctx.fillText(pt.value, W/2, H/2 - 12);
    ctx.font='500 16px "Helvetica Neue",Helvetica,sans-serif';
    ctx.fillStyle='#64748B';
    ctx.fillText(pt.label, W/2, H/2 + valSize*0.58);
    const cx=W/2, cy=H-52, rinR=18;
    ctx.strokeStyle='#1E293B'; ctx.lineWidth=3;
    ctx.beginPath(); ctx.arc(cx,cy,rinR,0,Math.PI*2); ctx.stroke();
    ctx.strokeStyle='#00D4C8'; ctx.lineWidth=3;
    ctx.beginPath();
    ctx.arc(cx,cy,rinR,-Math.PI/2,-Math.PI/2+progress*Math.PI*2); ctx.stroke();
    ctx.restore();
  }

  function drawBar(overall){
    ctx.fillStyle='#0d1f2d'; ctx.fillRect(0,H-6,W,6);
    ctx.fillStyle='#00D4C8'; ctx.fillRect(0,H-6,W*overall,6);
  }

  let last=0;
  function frame(now){
    const dt=Math.min(now-last,80); last=now;
    if(playing) slideT+=dt;
    if(slideT>=SLIDE_DUR){ slideT-=SLIDE_DUR; slideIdx=(slideIdx+1)%Math.max(1,POINTS.length); }
    drawBg(now);
    if(POINTS.length){
      drawSlide(POINTS[slideIdx], slideT/SLIDE_DUR);
      drawBar((slideIdx + slideT/SLIDE_DUR)/POINTS.length);
    }
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);

  btnPlay.addEventListener('click',()=>{
    playing=!playing;
    btnPlay.textContent=playing?'⏸ Pause':'▶ Play';
  });
  btnExp.addEventListener('click',()=>{
    const a=document.createElement('a');
    a.href=canvas.toDataURL('image/png');
    a.download='vdp-'+TOPIC.replace(/\\s+/g,'-')+'.png';
    a.click();
  });
})();
</script>
""".replace("%%HEIGHT%%", str(height)).replace("%%TOPIC_JS%%", topic_js).replace("%%DP_JSON%%", dp_json)

    components.html(html, height=height + 52)
