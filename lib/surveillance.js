// Market surveillance: spoofing, wash trading, pump patterns, per-account risk score.
// Hooks are called by the engine (server.js) as things happen; every check is O(small) so it never slows matching.
// Every alert carries EVIDENCE: the sequence-number range plus the order/trade ids involved, so you can jump the
// time-travel replay to exactly that moment ("prove what happened").
const fs = require('fs');

const num = (v, d) => (v !== undefined && v !== '' && !isNaN(+v) ? +v : d);
const kindOf = u => u.kind || (u.bot ? 'mm' : 'user');          // 'mm' = liquidity bots: never monitored, only used as counterparties

const SEV = { SELF_TRADE: 'medium', SPOOFING: 'high', WASH_TRADING: 'high', PUMP_PATTERN: 'high' };
const POINTS = { SELF_TRADE: 10, SPOOFING: 30, WASH_TRADING: 35, PUMP_PATTERN: 30 };
const TITLES = { SELF_TRADE: 'Self-trade attempt (blocked)', SPOOFING: 'Spoofing', WASH_TRADING: 'Wash trading', PUMP_PATTERN: 'Pump pattern' };

module.exports = function createSurveillance({ seq, users, depth, enabled = () => true, onAlert, alertFile, env = process.env }) {
  const cfg = {
    spoofMs: num(env.SURV_SPOOF_MS, 5000),             // an order cancelled within this long, unfilled, counts as "fleeting"
    spoofMinQty: num(env.SURV_SPOOF_MIN_QTY, 300),     // ...if it is at least this big
    spoofRel: num(env.SURV_SPOOF_REL, 2),              // ...and at least this many times the OTHER accounts' visible same-side depth (top 10 levels)
    spoofCount: num(env.SURV_SPOOF_COUNT, 3),          // this many fleeting big orders...
    spoofWindowMs: num(env.SURV_SPOOF_WINDOW_MS, 60000), // ...inside this window raise an alert
    washTrades: num(env.SURV_WASH_TRADES, 4),          // >= this many trades between the same two accounts...
    washWindowMs: num(env.SURV_WASH_WINDOW_MS, 60000), // ...inside this window...
    washBalance: num(env.SURV_WASH_BALANCE, .5),       // ...flowing both ways with net transfer <= this share of gross volume
    pumpPct: num(env.SURV_PUMP_PCT, 1.5),              // price up >= X percent...
    pumpMs: num(env.SURV_PUMP_MS, 30000),              // ...within Y ms...
    pumpConc: num(env.SURV_PUMP_CONC, .7),             // ...with >= this share of volume involving the top 2 accounts
    cooldownMs: num(env.SURV_COOLDOWN_MS, 30000),      // same alert (type + subject) is merged into one within this time
    halfLifeMs: num(env.SURV_HALF_LIFE_MS, 600000),    // risk points decay by half every 10 minutes
  };
  const accts = new Map(), meta = new Map(), pairs = new Map(), wins = {}, alerts = [], lastAlert = new Map();
  let nextId = 1, tradesSeen = 0;

  // ---- persistence: alerts.log is append-only (updates append a newer line for the same id; the newest wins on load) ----
  if (alertFile) {
    try {
      const byId = new Map();
      for (const line of fs.readFileSync(alertFile, 'utf8').split('\n')) { if (!line) continue; try { const a = JSON.parse(line); byId.set(a.id, a); } catch {} }
      for (const a of [...byId.values()].sort((x, y) => x.id - y.id).slice(-500)) { alerts.push(a); nextId = Math.max(nextId, a.id + 1); }
      fs.writeFileSync(alertFile, alerts.map(a => JSON.stringify(a)).join('\n') + (alerts.length ? '\n' : ''));   // compact on start
    } catch {}
  }
  const persist = a => { if (alertFile) try { fs.appendFileSync(alertFile, JSON.stringify(a) + '\n'); } catch {} };

  const acct = u => { let a = accts.get(u.id); if (!a) accts.set(u.id, a = { id: u.id, name: u.name, kind: kindOf(u), placed: [], fast: [], spoof: [], pts: 0, ptsT: Date.now(), counts: {}, cancelled: 0, totalPlaced: 0, last: 0 }); return a; };
  const watched = u => u && kindOf(u) !== 'mm';
  const decayed = (a, t = Date.now()) => a.pts * Math.pow(.5, (t - a.ptsT) / cfg.halfLifeMs);
  const addPts = (a, p) => { const t = Date.now(); a.pts = Math.min(100, decayed(a, t) + p); a.ptsT = t; };
  const trim = (arr, since) => { while (arr.length && arr[0] < since) arr.shift(); };

  // Raise (or merge into a recent identical) alert. `subject` identifies what it is about, so the same behaviour keeps
  // updating ONE alert (count goes up) instead of spamming the panel.
  function raise(type, subject, accountsInvolved, symbol, detail, evidence) {
    const t = Date.now(), key = type + ':' + subject, prev = lastAlert.get(key);
    let a, merged = false;
    if (prev && t - prev.ts < cfg.cooldownMs) {
      merged = true;
      a = prev.alert; a.count++; a.ts = t; a.q = seq(); a.detail = detail;
      a.evidence = { ...evidence, from: Math.min(a.evidence.from, evidence.from) };
    } else {
      a = { id: nextId++, type, title: TITLES[type], severity: SEV[type], ts: t, q: seq(), symbol, accounts: accountsInvolved.map(u => u.name), detail, count: 1, evidence };
      alerts.push(a); if (alerts.length > 500) alerts.shift();
    }
    lastAlert.set(key, { ts: prev && a === prev.alert ? prev.ts : t, alert: a });
    for (const u of accountsInvolved) addPts(acct(u), merged ? POINTS[type] / 3 : POINTS[type]);   // repeats of a live alert add less
    for (const u of accountsInvolved) acct(u).counts[type] = (acct(u).counts[type] || 0) + 1;
    persist(a); onAlert && onAlert(a);
    return a;
  }

  // ---------- hook: an order was accepted (before it is matched) ----------
  function order(o, u) {
    if (!enabled() || !watched(u)) return;
    const a = acct(u), t = o.ts; a.totalPlaced++; a.placed.push(t); a.last = t; trim(a.placed, t - 300000);
    const same = depth(o.symbol, o.side === 'buy' ? 'bids' : 'asks', 10, u.id), ref = Math.max(same, 1);   // others' visible depth: your own layered orders don't count as 'the book'
    if (o.qty >= cfg.spoofMinQty && o.qty >= cfg.spoofRel * ref) meta.set(o.id, { t, rel: +(o.qty / ref).toFixed(1), depth: same });
  }

  // ---------- hook: the user cancelled an order ----------
  function cancel(o, u) {
    if (!enabled() || !watched(u)) return;
    const a = acct(u), t = Date.now(), age = t - o.ts; a.cancelled++; a.last = t;
    const m = meta.get(o.id); meta.delete(o.id);
    if (age <= cfg.spoofMs) { a.fast.push(t); trim(a.fast, t - 300000); }
    if (m && age <= cfg.spoofMs && o.filled <= o.qty * .1) {          // big, cancelled within seconds, (almost) never filled
      a.spoof.push({ t, id: o.id, q: o.id, qty: o.qty, rel: m.rel, age, symbol: o.symbol, side: o.side });
      while (a.spoof.length && a.spoof[0].t < t - cfg.spoofWindowMs) a.spoof.shift();
      const mine = a.spoof.filter(x => x.symbol === o.symbol);
      if (mine.length >= cfg.spoofCount) {
        const avg = Math.round(mine.reduce((s, x) => s + x.age, 0) / mine.length), big = Math.max(...mine.map(x => x.rel));
        raise('SPOOFING', u.id + o.symbol, [u], o.symbol,
          `${mine.length} large ${o.side} orders (up to ${big}x the visible book) cancelled unfilled after ~${(avg / 1000).toFixed(1)}s on average, within ${Math.round(cfg.spoofWindowMs / 1000)}s`,
          { from: mine[0].q, to: seq(), orders: mine.map(x => x.id) });
      }
    }
  }

  // ---------- hook: the engine caught an account trading against its own resting order (self-trade prevention fired) ----------
  function selfTrade(o, resting, u) {
    if (!enabled() || !watched(u)) return;
    acct(u).last = Date.now();
    raise('SELF_TRADE', u.id + o.symbol, [u], o.symbol, `Order #${o.id} would have traded against the account's own resting order #${resting.id}; the resting order was cancelled`,
      { from: resting.id, to: o.id, orders: [resting.id, o.id] });
  }

  // ---------- hook: a trade happened ----------
  function trade(t, bu, su, taker) {
    tradesSeen++; if (!enabled()) return;
    const w = wins[t.symbol] || (wins[t.symbol] = []), a = watched(bu), b = watched(su);
    w.push({ ts: t.ts, p: t.price, q: t.qty, id: t.id, b: a ? bu : null, s: b ? su : null }); trim2(w, t.ts - cfg.pumpMs);

    // wash trading across accounts: two accounts trading back and forth with each other (self-trades are blocked by the engine)
    if (a && b && bu.id !== su.id) {
      const k = bu.id < su.id ? bu.id + '|' + su.id : su.id + '|' + bu.id; let p = pairs.get(k); if (!p) pairs.set(k, p = []);
      p.push({ ts: t.ts, id: t.id, q: t.qty, buyer: bu.id }); while (p.length && p[0].ts < t.ts - cfg.washWindowMs) p.shift();
      const ids = [bu.id, su.id].sort(), vol = id => p.filter(x => x.buyer === id).reduce((s, x) => s + x.q, 0), v0 = vol(ids[0]), v1 = vol(ids[1]), gross = v0 + v1;
      if (p.length >= cfg.washTrades && v0 > 0 && v1 > 0 && Math.abs(v0 - v1) / gross <= cfg.washBalance)
        raise('WASH_TRADING', k, [bu, su], t.symbol, `${p.length} trades between ${bu.name} and ${su.name} in ${Math.round(cfg.washWindowMs / 1000)}s, flowing both ways (${v0} / ${v1} shares): no real change of ownership`,
          { from: p[0].id, to: t.id, trades: p.map(x => x.id) });
    }

    // pump pattern: price up >= X% within Y seconds, driven by very few accounts
    let lo = Infinity, loT = 0; for (const x of w) if (x.id !== t.id && x.p < lo) { lo = x.p; loT = x.ts; }
    if (lo < Infinity && t.price / lo - 1 >= cfg.pumpPct / 100) {
      const vol = new Map(); let tot = 0;
      for (const x of w) { tot += x.q; for (const u of [x.b, x.s]) if (u) vol.set(u.id, { u, q: (vol.get(u.id)?.q || 0) + x.q }); }
      const top = [...vol.values()].sort((x, y) => y.q - x.q).slice(0, 2), ids = new Set(top.map(x => x.u.id));
      const inv = w.filter(x => (x.b && ids.has(x.b.id)) || (x.s && ids.has(x.s.id))).reduce((s, x) => s + x.q, 0);
      if (top.length && inv / tot >= cfg.pumpConc)
        raise('PUMP_PATTERN', t.symbol, top.map(x => x.u), t.symbol,
          `${t.symbol} rose ${((t.price / lo - 1) * 100).toFixed(2)}% in ${((t.ts - loT) / 1000).toFixed(1)}s; ${Math.round(inv / tot * 100)}% of the volume involved just ${top.length} account${top.length > 1 ? 's' : ''}`,
          { from: w[0].id, to: t.id, trades: w.filter(x => ids.has(x.b?.id) || ids.has(x.s?.id)).map(x => x.id).slice(-30) });
    }
  }
  const trim2 = (w, since) => { while (w.length && w[0].ts < since) w.shift(); };

  // ---------- views ----------
  function accounts(limit = 15) {
    const t = Date.now();
    return [...accts.values()].map(a => {
      trim(a.placed, t - 300000); trim(a.fast, t - 300000);
      const ratio = a.placed.length >= 8 ? a.fast.length / a.placed.length : 0, base = decayed(a, t), score = Math.min(100, Math.round(base + Math.min(15, ratio * 20)));
      return { id: a.id, name: a.name, kind: a.kind, score, level: score >= 60 ? 'high' : score >= 30 ? 'medium' : 'low', placed: a.totalPlaced, cancelled: a.cancelled,
        fastCancelPct: Math.round(ratio * 100), counts: a.counts, last: a.last };
    }).filter(a => a.score > 0 || Date.now() - a.last < 600000).sort((x, y) => y.score - x.score || y.last - x.last).slice(0, limit);
  }
  const list = (n = 100) => alerts.slice(-n).reverse();
  const upTo = q => alerts.filter(a => a.q <= q).slice(-50).reverse();
  function sweep() {                                                     // bound memory
    const t = Date.now();
    for (const [k, p] of pairs) if (!p.length || p[p.length - 1].ts < t - cfg.washWindowMs) pairs.delete(k);
    for (const [k, m] of meta) if (m.t < t - 60000) meta.delete(k);
    for (const [k, v] of lastAlert) if (t - v.ts > cfg.cooldownMs * 4) lastAlert.delete(k);
  }
  setInterval(sweep, 30000).unref();

  return { order, cancel, selfTrade, trade, accounts, list, upTo, cfg, stats: () => ({ alerts: alerts.length, watched: accts.size, tradesScanned: tradesSeen }) };
};
