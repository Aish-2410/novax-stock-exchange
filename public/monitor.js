// Surveillance panel + time-travel replay. Loaded after the main script in index.html, so it shares its helpers
// ($, api, toast, money, pf, ticks, sym, me, toLogin).
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const clock = t => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
const dayclock = t => new Date(t).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
const view = () => document.body.dataset.view;

// ---------- navigation (hash based, so a refresh or a shared link lands on the same view) ----------
const HASH = { trade: '', surv: '#surveillance', time: '#time-travel' };
function showView(v) {
  document.body.dataset.view = v; document.querySelectorAll('#nav a').forEach(a => a.classList.toggle('on', a.dataset.v == v));
  if (location.hash !== HASH[v]) history.replaceState(null, '', HASH[v] || location.pathname);
  if (v == 'surv') { SV.unseen = 0; badge(); loadSurv(); }
  if (v == 'time') tmOpen();
  if (v == 'trade') setTimeout(() => draw(), 0);
}
$('#nav').onclick = e => { if (e.target.dataset.v) showView(e.target.dataset.v); };
const fromHash = () => showView(location.hash == '#surveillance' ? 'surv' : location.hash == '#time-travel' ? 'time' : 'trade');
addEventListener('hashchange', fromHash);

// ---------- Surveillance ----------
const SV = { alerts: [], accounts: [], cfg: null, demo: false, running: null, unseen: 0, poll: null };
const badge = () => { const b = $('#bd'); b.hidden = !SV.unseen; b.textContent = SV.unseen; };
const kindLabel = { trickster: 'trickster bot' };

async function loadSurv() {
  try {
    const j = await api('/api/surveillance'); Object.assign(SV, { alerts: j.alerts, accounts: j.accounts, cfg: j.config, demo: j.demo, running: j.running, stats: j.stats });
    renderSurv(); clearTimeout(SV.poll); if (j.running && view() == 'surv') SV.poll = setTimeout(loadSurv, 1500);
  } catch (e) { toast(e, 1); }
}
function renderSurv() { renderAlerts(); renderRisk(); renderRules(); renderDemo(); }
function renderAlerts() {
  $('#svstat').textContent = SV.stats ? `${SV.stats.tradesScanned.toLocaleString()} trades scanned, ${SV.stats.watched} accounts watched` : '';
  $('#svalerts').innerHTML = SV.alerts.length ? SV.alerts.map(a => {
    const ev = a.evidence, ids = (ev.orders || ev.trades || []).slice(0, 6), more = (ev.orders || ev.trades || []).length - ids.length, what = ev.orders ? 'orders' : 'trades';
    return `<div class="al ${a.severity}"><div class="ah"><b>${esc(a.title)}</b><span class="chip">${esc(a.symbol)}</span>${a.count > 1 ? `<span class="mut">seen ${a.count} times</span>` : ''}<span class="mut r">${clock(a.ts)}</span></div>
      <div>${esc(a.detail)}</div>
      <div class="ev"><span>Accounts: <b>${a.accounts.map(esc).join(', ')}</b></span><span>Evidence: sequence ${ev.from} to ${ev.to}, ${what} ${ids.map(i => '#' + i).join(', ')}${more > 0 ? ` and ${more} more` : ''}</span><a class="lnk" onclick="rewind(${a.q},'${esc(a.symbol)}')">Rewind to this moment</a></div></div>`;
  }).join('') : '<p class="empty">No alerts yet. Use the trickster bot on the right to make some.</p>';
}
function renderRisk() {
  $('#svrisk').innerHTML = SV.accounts.length ? SV.accounts.map(a => {
    const chips = Object.entries(a.counts).map(([k, n]) => `<span class="chip">${esc(k.replace('_', ' ').toLowerCase())} x${n}</span>`).join(' ');
    return `<div class="risk"><div class="top"><span><b>${esc(a.name)}</b>${kindLabel[a.kind] ? ` <span class="mut">${kindLabel[a.kind]}</span>` : ''}</span><b class="${a.level == 'high' ? 'dn' : a.level == 'medium' ? '' : 'up'}" style="${a.level == 'medium' ? 'color:var(--warn)' : ''}">${a.score} ${a.level}</b></div>
      <div class="meter"><i class="${a.level}" style="width:${a.score}%"></i></div>
      <div class="mut">${a.placed} orders, ${a.cancelled} cancelled${a.fastCancelPct ? `, ${a.fastCancelPct}% cancelled within seconds` : ''}</div>${chips ? `<div style="margin-top:4px">${chips}</div>` : ''}</div>`;
  }).join('') : '<p class="empty">No account activity to score yet. Scores decay by half every 10 minutes.</p>';
}
function renderRules() {
  const c = SV.cfg; if (!c) return;
  $('#svrules').innerHTML = `<p><b>Spoofing.</b> ${c.spoofCount} or more orders of at least ${c.spoofMinQty} shares, and ${c.spoofRel}x the rest of the visible book, cancelled unfilled within ${c.spoofMs / 1000}s inside ${c.spoofWindowMs / 1000}s.</p>
   <p><b>Wash trading.</b> The engine blocks an account trading with itself and flags the attempt. It also flags two accounts that make ${c.washTrades}+ trades with each other inside ${c.washWindowMs / 1000}s with shares flowing both ways.</p>
   <p><b>Pump pattern.</b> Price up ${c.pumpPct}% or more within ${c.pumpMs / 1000}s while ${Math.round(c.pumpConc * 100)}% of the volume involves just two accounts.</p>
   <p><b>Risk score.</b> Each alert adds points to the accounts involved (0 to 100). Points decay by half every ${Math.round(c.halfLifeMs / 60000)} minutes. Liquidity bots are never scored.</p>`;
}
function renderDemo() {
  $('#svdemo').hidden = !SV.demo; if (!SV.demo) return;
  const d = $('#dsym'); if (!d.options.length) d.innerHTML = Object.keys(ticks).map(s => `<option>${s}</option>`).join(''), d.value = ticks.TSLA ? 'TSLA' : sym;
  document.querySelectorAll('#svdemo [data-d]').forEach(b => b.disabled = !!SV.running);
  $('#dst').textContent = SV.running ? `Running: ${SV.running === 'all' ? 'all four scenarios' : SV.running}. Alerts appear as they trigger.` : '';
}
$('#svdemo').onclick = async e => {
  const k = e.target.dataset.d; if (!k) return;
  try { await api('/api/surveillance/demo', { method: 'POST', body: { scenario: k, symbol: $('#dsym').value } }); SV.running = k; renderDemo(); clearTimeout(SV.poll); SV.poll = setTimeout(loadSurv, 1500); }
  catch (x) { toast(x, 1); }
};
function onAlert(a) {                                            // pushed over the WebSocket the moment surveillance raises or updates an alert
  const i = SV.alerts.findIndex(x => x.id == a.id); if (i >= 0) SV.alerts.splice(i, 1); else if (view() != 'surv') { SV.unseen++; badge(); }
  SV.alerts.unshift(a); SV.alerts = SV.alerts.slice(0, 100);
  if (i < 0 && a.severity == 'high') toast(`${a.title} on ${a.symbol}: ${a.accounts.join(', ')}`, 1);
  if (view() == 'surv') { renderAlerts(); clearTimeout(SV.refresh); SV.refresh = setTimeout(loadSurv, 600); }
  if (view() == 'time') tmRefreshMarks();
}

// ---------- Time travel ----------
const TM = { meta: null, q: null, data: null, busy: false, pending: null, sym: 'AAPL', timer: null, live: true };
const num = n => Number(n).toLocaleString();

async function tmOpen() {
  try {
    TM.meta = await api('/api/replay/meta'); const r = $('#tmr'); r.min = TM.meta.minSeq; r.max = TM.meta.headSeq;
    if (TM.q == null || TM.live) TM.q = TM.meta.headSeq; r.value = TM.q;
    const s = $('#tmsym'); if (!s.options.length) { s.innerHTML = Object.keys(ticks).map(x => `<option>${x}</option>`).join(''); s.value = TM.sym = sym; }
    $('#tmrange').textContent = `History kept: sequence ${num(TM.meta.minSeq)} to ${num(TM.meta.headSeq)} (${dayclock(TM.meta.minTs)} to now)`;
    tmMarks(); tmLoad(TM.q);
  } catch (e) { toast(e, 1); }
}
function tmMarks() {
  const m = TM.meta, span = Math.max(1, m.headSeq - m.minSeq);
  $('#tmmarks').innerHTML = m.alerts.filter(a => a.q >= m.minSeq).map(a => `<span class="mk ${a.severity}" title="${esc(a.type.replace('_', ' ').toLowerCase())} at ${clock(a.ts)}" style="left:${(a.q - m.minSeq) / span * 100}%" onclick="tmGo(${a.q},'${esc(a.symbol || '')}')"></span>`).join('');
}
async function tmRefreshMarks() { try { const m = await api('/api/replay/meta'); TM.meta = m; tmMarks(); } catch {} }
async function tmLoad(q) {                                       // one request at a time; while dragging only the latest position is fetched
  if (TM.busy) { TM.pending = q; return; } TM.busy = true;
  try { const d = await api('/api/replay?seq=' + q); TM.data = d; TM.q = d.seq; TM.live = d.seq >= d.head; $('#tmr').value = d.seq; tmRender(); }
  catch (e) { toast(e, 1); } finally { TM.busy = false; if (TM.pending != null) { const p = TM.pending; TM.pending = null; tmLoad(p); } }
}
function tmGo(q, s) { if (s && ticks[s]) tmSym(s); tmStop(); $('#tmr').value = q; tmLoad(q); }
function rewind(q, s) { if (s) TM.sym = s; showView('time'); setTimeout(() => { if (s) $('#tmsym').value = s; tmGo(q); }, 50); }
function tmRender() {
  const d = TM.data; if (!d) return;
  $('#tmread').className = 'read' + (TM.live ? '' : ' past');
  $('#tmread').innerHTML = `<div><b>${TM.live ? 'Live now' : 'Viewing the past'}</b> <span class="mut">${dayclock(d.ts)}</span></div><div>Sequence <b>${num(d.seq)}</b> of ${num(d.head)}</div><div class="mut">${d.stats.users} ${d.stats.users == 1 ? 'trader' : 'traders'}, ${d.stats.openOrders} open orders, rebuilt from ${num(d.stats.eventsApplied)} logged events</div>`;
  const st = d.symbols[TM.sym], r = (x, c) => `<div class="row"><i style="width:${x.size / Math.max(1, ...st.asks.map(y => y.size), ...st.bids.map(y => y.size)) * 100}%;background:var(--${c})"></i><span class="${c}">${pf(x.price)}</span><span>${x.size}</span></div>`;
  $('#tasks').innerHTML = st.asks.slice(0, 10).reverse().map(x => r(x, 'dn')).join(''); $('#tbids').innerHTML = st.bids.slice(0, 10).map(x => r(x, 'up')).join('');
  const a = st.asks[0], b = st.bids[0]; $('#tspr').innerHTML = st.last != null ? money(st.last) + (a && b ? ` <small class="mut">spread ${pf(a.price - b.price)}</small>` : '') : '-';
  $('#ttiles').innerHTML = Object.entries(d.symbols).map(([s, v]) => { const now = ticks[s] && ticks[s].last, ch = now && v.last ? (now - v.last) / v.last * 100 : null;
    return `<div class="tile ${s == TM.sym ? 'on' : ''}" onclick="tmSym('${s}')"><b>${s}</b>${v.last != null ? money(v.last) : '-'}<small>${ch == null ? '' : `now ${ch >= 0 ? '+' : ''}${ch.toFixed(2)}%`}</small></div>`; }).join('');
  $('#tme').innerHTML = d.me ? `<div class="kv"><span>Cash</span><b>${money(d.me.cash)}</b></div><div class="kv"><span>Reserved</span><span>${money(d.me.held)}</span></div><div class="kv"><span>Total value</span><b>${money(d.me.equity)}</b></div>` +
    (Object.keys(d.me.pos).length ? `<table>${Object.entries(d.me.pos).map(([s, n]) => `<tr><td>${s}</td><td>${n} shares</td></tr>`).join('')}</table>` : '<p class="mut" style="margin:6px 0 0">No shares held at this moment.</p>') : '<p class="empty">Your account did not exist yet.</p>';
  $('#talerts').innerHTML = d.alerts.length ? d.alerts.slice(0, 8).map(a => `<div class="kv"><span><b>${esc(a.title)}</b> <span class="mut">${esc(a.symbol)}</span></span><a class="lnk" onclick="tmGo(${a.q},'${esc(a.symbol)}')">${clock(a.ts)}</a></div>`).join('') : '<p class="empty">Nothing had been flagged yet.</p>';
  $('#tacc').innerHTML = d.accounts.length ? `<table>${d.accounts.slice(0, 8).map(a => `<tr><td>${esc(a.name)}</td><td style="text-align:right">${money(a.equity)}</td></tr>`).join('')}</table>` : '<p class="empty">No traders yet.</p>';
}
function tmSym(s) { TM.sym = s; $('#tmsym').value = s; tmRender(); }
$('#tmsym').onchange = e => tmSym(e.target.value);
$('#tmr').oninput = e => { tmStop(); tmLoad(+e.target.value); };
$('#tmlive').onclick = () => { tmStop(); TM.live = true; tmOpen(); };
function tmStop() { clearInterval(TM.timer); TM.timer = null; $('#tmplay').textContent = 'Play'; }
$('#tmplay').onclick = () => {
  if (TM.timer) return tmStop();
  const m = TM.meta; if (!m) return; const step = Math.max(1, Math.round((m.headSeq - m.minSeq) / 200)); if (TM.q >= m.headSeq) TM.q = m.minSeq;
  $('#tmplay').textContent = 'Pause';
  TM.timer = setInterval(() => { TM.q = Math.min(m.headSeq, TM.q + step); tmLoad(TM.q); if (TM.q >= m.headSeq) tmStop(); }, 200);
};
$('#tmver').onclick = async () => {
  const v = $('#tmverdict'); v.innerHTML = '<div class="verdict">Rebuilding the exchange from the event log…</div>';
  try { const j = await api('/api/replay/verify');
    v.innerHTML = j.ok ? `<div class="verdict ok"><b>Replay matches the live exchange</b> at sequence ${num(j.seq)}: ${j.users} accounts and ${j.symbols} order books are identical to the cent and the share, rebuilt from the log.</div>`
      : `<div class="verdict bad"><b>Replay differs from the live exchange.</b> ${j.mismatches.map(esc).join('<br>')}</div>`;
  } catch (e) { v.innerHTML = `<div class="verdict bad">${esc(e)}</div>`; }
};
setInterval(() => { if (view() == 'time' && TM.live && !TM.timer && !TM.busy) tmOpen(); }, 5000);   // stay at the live edge while looking at "now"
(async () => { for (let i = 0; i < 50 && !Object.keys(ticks).length; i++) await new Promise(r => setTimeout(r, 100)); fromHash(); })();
