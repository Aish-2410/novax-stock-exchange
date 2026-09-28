// Load test: N concurrent users register, log in, open WebSockets, trade, cancel, then verify invariants.
// Usage: node loadtest.js [baseUrl] [users] [seconds]     (start server with RATE_LIMIT=100000 for local runs)
const { WebSocket } = require('ws');
const BASE = process.argv[2] || 'http://localhost:3000', N = +process.argv[3] || 50, SECS = +process.argv[4] || 30;
const SYMS = ['AAPL', 'MSFT', 'GOOG', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX', 'AMD', 'JPM', 'DIS', 'KO'], run = Date.now().toString(36);
const stats = { req: 0, fail: 0, orders: 0, rejected: 0, cancels: 0, ws: 0, wsMsgs: 0, lat: [] };
const ids = new Set(); let dupIds = 0; const errors = {};
const pick = a => a[Math.random() * a.length | 0];

async function call(path, tok, method = 'GET', body) {
  const t0 = performance.now(); stats.req++;
  try {
    const r = await fetch(BASE + path, { method, headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + tok }, body: body && JSON.stringify(body) });
    const j = await r.json(); stats.lat.push(performance.now() - t0);
    if (!r.ok) { const k = r.status + ' ' + j.error; errors[k] = (errors[k] || 0) + 1; }
    return { ok: r.ok, status: r.status, j };
  } catch (e) { stats.fail++; errors[e.message] = (errors[e.message] || 0) + 1; return { ok: false, j: {} }; }
}

async function user(i) {
  const name = `lt_${run}_${i}`, pw = 'secret123';
  let r = await call('/api/register', '', 'POST', { name, password: pw });
  if (!r.ok) { stats.fail++; return null; }
  const tok = r.j.token; r = await call('/api/login', '', 'POST', { name, password: pw }); if (!r.ok) { stats.fail++; return null; }
  const t = r.j.token, ws = new WebSocket(BASE.replace('http', 'ws') + '/ws', { headers: { Cookie: 'sid=' + t } });
  ws.on('open', () => stats.ws++); ws.on('message', () => stats.wsMsgs++); ws.on('error', () => {});
  const end = Date.now() + SECS * 1000, open = [];
  const bookOf = async s => (await call('/marketdata/orderBook/L2?symbol=' + s + '&depth=5', t)).j;
  while (Date.now() < end) {
    const s = pick(SYMS), b = await bookOf(s), roll = Math.random();
    if (roll < .25 && open.length) { const id = open.pop(); const c = await call('/v1/order/' + id, t, 'DELETE'); if (c.ok) stats.cancels++; }
    else if (b.asks && b.asks[0] && b.bids && b.bids[0]) {
      const side = Math.random() < .5 ? 'buy' : 'sell', cross = Math.random() < .5;
      const price = side === 'buy' ? (cross ? b.asks[0].price * 1.005 : b.bids[0].price * .998) : (cross ? b.bids[0].price * .995 : b.asks[0].price * 1.002);
      const o = await call('/v1/order', t, 'POST', { symbol: s, side, price: Math.round(price), quantity: 1 + Math.random() * 10 | 0 });
      if (o.ok) { stats.orders++; if (ids.has(o.j.id)) dupIds++; ids.add(o.j.id); if (o.j.status !== 'filled') open.push(o.j.id); }
      else if (o.status >= 500) stats.fail++; else stats.rejected++;
    }
    await call('/api/me', t);
    await new Promise(r => setTimeout(r, 100 + Math.random() * 300));
  }
  const me = (await call('/api/me', t)).j.user; ws.close();
  return me;
}

(async () => {
  console.log(`Load test: ${N} users, ${SECS}s, ${BASE}`); const t0 = Date.now();
  const users = (await Promise.all(Array.from({ length: N }, (_, i) => user(i)))).filter(Boolean);
  const bad = [];
  for (const u of users) {
    if (u.cash < 0) bad.push(`${u.name} negative cash`); if (u.available < 0) bad.push(`${u.name} negative available cash`);
    for (const s in u.pos) if (u.pos[s] < 0 || (u.heldPos[s] || 0) < 0 || (u.heldPos[s] || 0) > u.pos[s]) bad.push(`${u.name} bad ${s} holdings`);
  }
  const l = stats.lat.sort((a, b) => a - b), p = q => l[Math.floor(l.length * q)] ?? 0;
  console.log(`\nUsers completed: ${users.length}/${N}   WebSockets opened: ${stats.ws}   WS msgs received: ${stats.wsMsgs}`);
  console.log(`Requests: ${stats.req}  (${(stats.req / ((Date.now() - t0) / 1000)).toFixed(0)}/s)  network/5xx failures: ${stats.fail}`);
  console.log(`Orders accepted: ${stats.orders}  rejected(risk/funds/shares): ${stats.rejected}  cancels: ${stats.cancels}  duplicate order ids: ${dupIds}`);
  console.log(`Latency ms  p50 ${p(.5).toFixed(1)}  p95 ${p(.95).toFixed(1)}  p99 ${p(.99).toFixed(1)}  max ${(l[l.length - 1] || 0).toFixed(1)}`);
  if (Object.keys(errors).length) console.log('Error breakdown:', errors);
  console.log(bad.length ? 'INVARIANT FAILURES:\n' + bad.join('\n') : 'Invariants OK: no negative cash/holdings, no duplicate IDs');
  const pass = users.length === N && !stats.fail && !bad.length && !dupIds;
  console.log(pass ? '\nRESULT: PASS' : '\nRESULT: FAIL'); process.exit(pass ? 0 : 1);
})();
