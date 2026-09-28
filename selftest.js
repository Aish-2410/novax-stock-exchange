// Pre-demo check: makes the trickster bot misbehave, then asserts every alert fires and the replay matches the live exchange.
// Usage: node selftest.js [baseUrl] [symbol]      (server must be running with DEMO_TRICKSTER not set to 0; takes ~25s)
const B = process.argv[2] || 'http://localhost:3000', SYM = process.argv[3] || 'TSLA';
const sleep = ms => new Promise(r => setTimeout(r, ms));
const checks = []; const check = (name, ok, extra = '') => { checks.push(ok); console.log((ok ? 'PASS ' : 'FAIL ') + name + (extra ? '  ' + extra : '')); };
(async () => {
  const name = 'st' + Date.now().toString(36), r = await fetch(B + '/api/register', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name, password: 'selftest1' }) });
  if (!r.ok) throw 'register failed: ' + r.status;
  const H = { cookie: r.headers.get('set-cookie').split(';')[0], 'content-type': 'application/json' }, get = async p => (await fetch(B + p, { headers: H })).json();
  const d = await fetch(B + '/api/surveillance/demo', { method: 'POST', headers: H, body: JSON.stringify({ scenario: 'all', symbol: SYM }) });
  if (!d.ok) throw 'demo refused: ' + (await d.json()).error;
  console.log(`trickster running on ${SYM}...`);
  let s; for (let i = 0; i < 90; i++) { await sleep(1000); s = await get('/api/surveillance'); if (!s.running) break; }
  const types = new Set(s.alerts.map(a => a.type));
  for (const t of ['SELF_TRADE', 'WASH_TRADING', 'SPOOFING', 'PUMP_PATTERN']) check('alert raised: ' + t, types.has(t));
  check('trickster accounts scored high', s.accounts.filter(a => a.kind === 'trickster' && a.level === 'high').length >= 1, s.accounts.map(a => a.name + '=' + a.score).join(' '));
  const v = await get('/api/replay/verify'); check('replay of the event log matches the live exchange', v.ok, `${v.users} accounts, ${v.eventsApplied} events applied ${v.mismatches?.join('; ') || ''}`);
  const pump = s.alerts.find(a => a.type === 'PUMP_PATTERN');
  if (pump) { const before = await get('/api/replay?seq=' + (pump.evidence.from - 1)), at = await get('/api/replay?seq=' + pump.q), rise = at.symbols[SYM].last / before.symbols[SYM].last - 1;
    check('time travel shows the price before/after the pump', rise > .015, `${SYM} ${(before.symbols[SYM].last / 100).toFixed(2)} -> ${(at.symbols[SYM].last / 100).toFixed(2)} (+${(rise * 100).toFixed(2)}%)`); }
  console.log(checks.every(Boolean) ? '\nRESULT: PASS' : '\nRESULT: FAIL'); process.exit(checks.every(Boolean) ? 0 : 1);
})().catch(e => { console.error('ERROR', e); process.exit(1); });
