// NovaX Exchange - order manager (risk + wallet), sequencer, matching engine, market data, REST + WebSocket.
// Prices are integers in cents. One global sequence stamps every order and trade.
// Durability: append-only journal (every user/session/order/cancel) + periodic snapshot; on start = snapshot + journal replay.
// Evidence trail: a separate, never-truncated, segmented event log (lib/eventlog.js) powers time-travel replay (lib/replay-worker.js).
// Surveillance (lib/surveillance.js) watches orders/cancels/trades for spoofing, wash trading and pump patterns.
const express = require('express'), http = require('http'), { WebSocketServer } = require('ws');
const crypto = require('crypto'), fs = require('fs'), path = require('path'), { promisify } = require('util');
const { Worker } = require('worker_threads');
const createEventLog = require('./lib/eventlog'), createSurveillance = require('./lib/surveillance'), createTrickster = require('./lib/trickster');
const PORT = process.env.PORT ?? 3000;
const DIR = process.env.DATA_DIR || __dirname, FILE = path.join(DIR, 'data.json'), JOURNAL = path.join(DIR, 'journal.log');
const EVENTS = path.join(DIR, 'events'), ALERTS = path.join(DIR, 'alerts.log');

fs.mkdirSync(DIR, { recursive: true });
fs.mkdirSync(EVENTS, { recursive: true });
const DEMO = process.env.DEMO_TRICKSTER !== '0';                  // trickster demo buttons (set DEMO_TRICKSTER=0 to disable on a public deployment)
const MAX_DAILY = +process.env.MAX_DAILY_QTY || 10000;          // risk rule: max shares per symbol per user per day
const RATE_LIMIT = +process.env.RATE_LIMIT || 300;              // trading/auth requests per minute per IP
const MD_LIMIT = +process.env.MD_LIMIT || RATE_LIMIT * 5;       // public market-data requests per minute per IP
const SESSION_TTL = 7 * 864e5, RES = [5, 60], CAP = { 5: 600, 60: 12000 };   // live candle resolutions (seconds) and how many are kept in memory
const SYMS = { AAPL: 190, MSFT: 420, GOOG: 170, TSLA: 250, AMZN: 185, NVDA: 130, META: 560, NFLX: 690, AMD: 165, JPM: 210, DIS: 95, KO: 62 };
// Display currencies; the engine only stores USD cents. Override: FX_RATES='{"INR":84}'
const CURRENCIES = { USD: { name: 'US Dollar', rate: 1, locale: 'en-US', dec: 2 }, INR: { name: 'Indian Rupee', rate: 83.5, locale: 'en-IN', dec: 2 },
  EUR: { name: 'Euro', rate: 0.92, locale: 'de-DE', dec: 2 }, GBP: { name: 'British Pound', rate: 0.79, locale: 'en-GB', dec: 2 }, JPY: { name: 'Japanese Yen', rate: 150, locale: 'ja-JP', dec: 0 } };
try { for (const [k, v] of Object.entries(JSON.parse(process.env.FX_RATES || '{}'))) if (CURRENCIES[k] && v > 0) CURRENCIES[k].rate = +v; } catch {}

let clock = null, replaying = false;                            // clock is set during journal replay so timestamps are reproduced
const now = () => clock ?? Date.now(), dayOf = t => new Date(t).toISOString().slice(0, 10);
const { Side, OPEN } = require('./lib/book');
const jfd = fs.openSync(JOURNAL, 'a');                          // persistent O_APPEND fd: snapshot() truncating the file is safe, writes just continue at the new end
const evlog = createEventLog({ dir: EVENTS, segBytes: (+process.env.EVENT_SEGMENT_MB || 20) * 1048576, keep: +process.env.EVENT_SEGMENTS || 8, seq: () => S.seq });
const journal = ev => { if (replaying) return; try { fs.writeSync(jfd, JSON.stringify(ev) + '\n'); } catch (e) { console.error('journal write failed', e.message); } evlog.append(ev); };

let S = { seq: 1, users: {}, orders: {}, trades: [], sessions: {} };
try { S = { ...S, ...JSON.parse(fs.readFileSync(FILE)) }; } catch {}
for (const k of Object.keys(S.sessions)) if (typeof S.sessions[k] !== 'object') delete S.sessions[k];

// ---------- Order book (see lib/book.js) ----------
const books = {}, last = {}, mid = {}, tradesBy = {}, cand = {}, uo = new Map(), clients = new Set(), dirtySyms = new Set(), dirtyUsers = new Set();
for (const s in SYMS) { books[s] = { bids: new Side(false), asks: new Side(true) }; last[s] = SYMS[s] * 100; tradesBy[s] = []; cand[s] = { 5: [], 60: [] }; }
const book = (s, d = 12) => ({ bids: books[s].bids.depth(d), asks: books[s].asks.depth(d) });
const emit = m => { const d = JSON.stringify(m); for (const c of clients) c.readyState === 1 && c.send(d); };
// visible depth on one side of the book (top n price levels), NOT counting the given account's own resting orders
const depthOf = (s, side, n, exclude) => { let t = 0, b = books[s][side]; for (const p of b.prices.slice(0, n)) for (const o of b.levels.get(p).q) if (OPEN(o) && o.userId !== exclude) t += o.qty - o.filled; return t; };
const seqNow = () => S.seq - 1, isMM = u => u.bot && (u.kind || 'mm') === 'mm';
const wsEmitAuthed = m => { const d = JSON.stringify(m); for (const c of clients) if (c.uid && c.readyState === 1) c.send(d); };   // alerts name accounts: logged-in users only
const surv = createSurveillance({ seq: seqNow, depth: depthOf, enabled: () => !replaying, alertFile: ALERTS, onAlert: a => wsEmitAuthed({ t: 'alert', alert: a }) });
const rebuildIndex = () => { uo.clear(); for (const o of Object.values(S.orders)) (uo.get(o.userId) || uo.set(o.userId, []).get(o.userId)).push(o.id); };

// ---------- Market data: candles are built incrementally from the execution stream ----------
function addCandle(t) {
  for (const r of RES) {
    const ms = r * 1000, k = Math.floor(t.ts / ms) * ms, arr = cand[t.symbol][r], c = arr[arr.length - 1];
    if (c && k <= c.time) { c.high = Math.max(c.high, t.price); c.low = Math.min(c.low, t.price); c.close = t.price; c.volume += t.qty; }
    else { arr.push({ time: k, open: t.price, high: t.price, low: t.price, close: t.price, volume: t.qty }); if (arr.length > CAP[r]) arr.splice(0, CAP[r] / 6 | 0); }
  }
}
function addTrade(t) {
  S.trades.push(t); if (S.trades.length > 30000) S.trades.splice(0, 10000);
  const tb = tradesBy[t.symbol]; tb.push(t); if (tb.length > 6000) tb.splice(0, 2000);
  last[t.symbol] = t.price; addCandle(t);
}

// ---------- Users ----------
const hashA = promisify(crypto.scrypt);   // async: never blocks the matching thread
const view = u => ({ id: u.id, name: u.name, cash: u.cash, held: u.held, available: u.cash - u.held, pos: u.pos, heldPos: u.heldPos, traded: u.traded, dailyLimit: MAX_DAILY });
const fmt = o => ({ id: o.id, symbol: o.symbol, side: o.side, price: o.price, orderType: 'limit', quantity: o.qty,
  filledQuantity: o.filled, remainingQuantity: o.qty - o.filled, status: o.status, creationTime: o.ts });
function addUser(name, salt, hash, bot, kind) {
  const id = 'u' + (Object.keys(S.users).length + 1);
  const u = S.users[id] = { id, name, salt, hash, cash: bot ? 1e13 : 10000000, held: 0,
    pos: bot ? Object.fromEntries(Object.keys(SYMS).map(s => [s, 1e6])) : {}, heldPos: {}, traded: {}, day: dayOf(now()), bot: !!bot, kind: bot ? kind || 'mm' : undefined };
  journal({ t: 'u', user: u }); return u;
}

// ---------- Order manager: risk checks + wallet holds, then sequencer -> matching engine ----------
function place(u, { symbol, side, price, quantity }) {
  if (!books[symbol]) throw 'Unknown symbol';
  if (side !== 'buy' && side !== 'sell') throw 'Invalid side';
  if (!Number.isInteger(price) || price <= 0 || !Number.isInteger(quantity) || quantity <= 0 || quantity > 100000) throw 'Invalid price or quantity';
  const ts = now(); if (u.day !== dayOf(ts)) { u.day = dayOf(ts); u.traded = {}; }
  if (!u.bot && (u.traded[symbol] || 0) + quantity > MAX_DAILY) throw `Risk check failed: daily limit of ${MAX_DAILY} ${symbol} shares`;
  if (side === 'buy') {
    if (price * quantity > u.cash - u.held) throw 'Insufficient funds';
    u.held += price * quantity;
  } else {
    if (quantity > (u.pos[symbol] || 0) - (u.heldPos[symbol] || 0)) throw 'Insufficient shares';
    u.heldPos[symbol] = (u.heldPos[symbol] || 0) + quantity;
  }
  const id = S.seq++;
  journal({ t: 'o', id, uid: u.id, symbol, side, price, quantity, ts });
  const o = S.orders[id] = { id, userId: u.id, symbol, side, price, qty: quantity, filled: 0, status: 'new', ts };
  (uo.get(u.id) || uo.set(u.id, []).get(u.id)).push(id);
  surv.order(o, u);
  match(o); dirtyUsers.add(u.id);
  return o;
}
function release(o) { const u = S.users[o.userId], rem = o.qty - o.filled; if (o.side === 'buy') u.held -= o.price * rem; else u.heldPos[o.symbol] -= rem; dirtyUsers.add(u.id); }

// ---------- Matching engine: price-time priority, self-trade prevention (cancel resting order) ----------
function match(o) {
  const b = books[o.symbol], opp = o.side === 'buy' ? b.asks : b.bids, mine = o.side === 'buy' ? b.bids : b.asks;
  let r;
  while (o.filled < o.qty && (r = opp.best())) {
    if (o.side === 'buy' ? r.price > o.price : r.price < o.price) break;
    if (r.userId === o.userId) { surv.selfTrade(o, r, S.users[o.userId]); r.status = 'canceled'; release(r); opp.remove(r); continue; }   // flag the attempt, then cancel the resting order
    const q = Math.min(o.qty - o.filled, r.qty - r.filled), p = r.price; // trade at resting order's price
    const [bo, so] = o.side === 'buy' ? [o, r] : [r, o], bu = S.users[bo.userId], su = S.users[so.userId];
    bu.cash -= p * q; bu.held -= bo.price * q; bu.pos[o.symbol] = (bu.pos[o.symbol] || 0) + q;
    su.cash += p * q; su.pos[o.symbol] -= q; su.heldPos[o.symbol] -= q;
    for (const x of [bu, su]) x.traded[o.symbol] = (x.traded[o.symbol] || 0) + q;
    o.filled += q; r.filled += q;
    for (const x of [o, r]) x.status = x.filled === x.qty ? 'filled' : 'partially_filled';
    opp.fill(r, q);
    const t = { id: S.seq++, symbol: o.symbol, price: p, qty: q, buyOrderId: bo.id, sellOrderId: so.id, taker: o.side, ts: now() };
    addTrade(t); emit({ t: 'trade', symbol: o.symbol, price: p, qty: q, side: o.side, ts: t.ts });
    surv.trade(t, bu, su, o.side);
    dirtyUsers.add(bu.id); dirtyUsers.add(su.id);
  }
  if (o.filled < o.qty) mine.add(o);
  dirtySyms.add(o.symbol);
}
function cancel(u, id) {
  const o = S.orders[id];
  if (!o || o.userId !== u.id) throw 'Order not found';
  if (!OPEN(o)) throw `Cannot cancel: order already ${o.status}`;
  journal({ t: 'c', uid: u.id, id: o.id, ts: now() });
  o.status = 'canceled'; release(o); const b = books[o.symbol]; (o.side === 'buy' ? b.bids : b.asks).remove(o);
  dirtySyms.add(o.symbol); dirtyUsers.add(u.id);
  surv.cancel(o, u);
  return o;
}

// ---------- Persistence: snapshot + journal replay ----------
function snapshot() {
  try {
    const cut = Date.now() - 864e5;
    for (const id in S.orders) { const o = S.orders[id]; if (!OPEN(o) && o.ts < cut) delete S.orders[id]; }
    for (const k in S.sessions) if (S.sessions[k].exp < Date.now()) delete S.sessions[k];
    rebuildIndex();
    fs.writeFileSync(FILE + '.tmp', JSON.stringify(S)); fs.renameSync(FILE + '.tmp', FILE); fs.writeFileSync(JOURNAL, ''); // journal is only cleared after the snapshot is safely in place
  } catch (e) { console.error('snapshot failed', e.message); }
}
function replay() {
  let txt = ''; try { txt = fs.readFileSync(JOURNAL, 'utf8'); } catch { return; }
  replaying = true;
  for (const line of txt.split('\n')) {
    if (!line) continue; let e; try { e = JSON.parse(line); } catch { continue; }
    clock = e.ts ?? null;
    try {
      if (e.t === 'u') { if (!S.users[e.user.id]) S.users[e.user.id] = e.user; }
      else if (e.t === 's') S.sessions[e.token] = { u: e.uid, exp: e.exp };
      else if (e.t === 'x') delete S.sessions[e.token];
      else if (e.t === 'o') { if (e.id >= S.seq) { S.seq = e.id; place(S.users[e.uid], e); } }   // ids below S.seq are already in the snapshot
      else if (e.t === 'c') cancel(S.users[e.uid], e.id);
    } catch {}
  }
  replaying = false; clock = null;
}
for (const o of Object.values(S.orders)) if (OPEN(o) && books[o.symbol]) books[o.symbol][o.side === 'buy' ? 'bids' : 'asks'].add(o);

// ---------- Simulated market: one deterministic price path that every chart range and the liquidity bots share ----------
// Daily log-price is a slowly mean-reverting random walk (fixed seeds, so it is identical after every restart/deploy) pinned so that
// the reference price is hit at ANCHOR. Each day is then filled in minute by minute (a bridge between its open and close).
// Volatility scales with sqrt(time): a 1-minute window barely moves, a 1-week window moves several percent.
const MIN = 6e4, DAY = 864e5, ORIGIN = Date.UTC(2021, 0, 1), ANCHOR = Date.UTC(2026, 8, 28), THETA = .002;
const rng = seed => () => { seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; };
const gauss = r => Math.sqrt(-2 * Math.log(r() + 1e-12)) * Math.cos(2 * Math.PI * r());
const hs = s => [...s].reduce((a, c) => a * 31 + c.charCodeAt(0) | 0, 7);
const VOL = { TSLA: .03, NVDA: .028, AMD: .026, NFLX: .022, META: .022, KO: .008, JPM: .014, DIS: .017 };   // typical daily volatility
const volOf = s => VOL[s] || .016, dayN = t => Math.floor((t - ORIGIN) / DAY), A = dayN(ANCHOR), XS = {};
function levelAt(s, n) {                                        // closing price (cents, float) of UTC day n
  const a = XS[s] || (XS[s] = [0]), m = Math.max(n, A);
  for (let i = a.length; i <= m; i++) a.push(a[i - 1] * (1 - THETA) + volOf(s) * gauss(rng(hs(s) + i * 7919)));
  return SYMS[s] * 100 * Math.exp(a[n] - a[A]);
}
const dayCache = new Map();
function dayPath(s, n) {                                        // 1441 minute prices for day n (open -> close bridge) + wicks + volumes
  const key = s + ':' + n; let d = dayCache.get(key); if (d) return d;
  const N = 1440, r = rng(hs(s) * 5 + n * 15485863), sg = volOf(s) / Math.sqrt(N), o = levelAt(s, n - 1), c = levelAt(s, n);
  const hm = Array.from({ length: 24 }, () => .6 + r() * .9);   // some hours are busier than others
  const w = new Float64Array(N + 1), p = new Float64Array(N + 1), wk = new Float32Array(N), vs = new Int32Array(N), lo = Math.log(o), dl = Math.log(c / o);
  for (let i = 1; i <= N; i++) w[i] = w[i - 1] + gauss(r) * sg * hm[(i - 1) / 60 | 0];
  for (let i = 0; i <= N; i++) p[i] = Math.exp(lo + w[i] - i / N * w[N] + i / N * dl);
  for (let i = 0; i < N; i++) { wk[i] = Math.abs(gauss(r)) * sg * .5; vs[i] = Math.round(1200 * (.5 + r()) * (1 + Math.abs(Math.log(p[i + 1] / p[i])) / sg * .4)); }
  d = { p, wk, vs }; dayCache.set(key, d); if (dayCache.size > 300) dayCache.delete(dayCache.keys().next().value); return d;
}
function simAt(s, t) {                                          // simulated price (cents, float) at time t, interpolated within the minute
  const n = dayN(t), f = (t - ORIGIN - n * DAY) / MIN, i = Math.min(1439, f | 0), p = dayPath(s, n).p; return p[i] + (p[i + 1] - p[i]) * (f - i);
}
const refOf = s => Math.round(levelAt(s, dayN(Date.now()) - 1));   // previous UTC day's close: the base for the watchlist % change
const histCache = {};
function getHist(s) {                                           // completed weekday daily candles since 2021 (stable: never rewritten)
  const tn = dayN(Date.now()), c0 = histCache[s]; if (c0 && c0.day === tn) return c0.arr;
  const arr = [], vol = volOf(s), R = Math.round;
  for (let n = 1; n < tn; n++) {
    const t = ORIGIN + n * DAY, wd = new Date(t).getUTCDay(); if (wd === 0 || wd === 6) continue;
    const r = rng(hs(s) * 3 + n * 104729), o = levelAt(s, n - 1), c = levelAt(s, n), g = () => gauss(r);
    arr.push({ time: t, open: R(o), high: R(Math.max(o, c) * (1 + Math.abs(g()) * vol * .35)), low: R(Math.min(o, c) * (1 - Math.abs(g()) * vol * .35)), close: R(c), volume: Math.round((2e6 + r() * 3e6) * (.7 + Math.abs(g()) * .5)) });
  }
  histCache[s] = { day: tn, arr }; return arr;
}
// 1-minute candles for the last 8 days: the simulated path, with real engine candles laid over the minutes where trades actually happened.
const mCache = {};
function getMinutes(s) {
  const nowMs = Date.now(), key = Math.floor(nowMs / 5000), c0 = mCache[s]; if (c0 && c0.key === key) return c0.arr;
  const end = Math.floor(nowMs / MIN) * MIN, start = end - 8 * DAY, real = new Map(), out = [], R = Math.round;
  const list = cand[s][60]; for (let i = list.length - 1; i >= 0 && list[i].time >= start; i--) real.set(list[i].time, list[i]);
  let prev = 0;
  for (let t = start; t <= end; t += MIN) {
    const n = dayN(t), i = (t - ORIGIN - n * DAY) / MIN, d = dayPath(s, n), k = real.get(t), sv = d.vs[i];
    if (k) { out.push({ time: t, open: k.open, high: k.high, low: k.low, close: k.close, volume: sv + k.volume }); prev = k.close; continue; }
    const sc = prev ? prev / d.p[i] : 1, o = d.p[i] * sc, c = d.p[i + 1] * sc;   // keep the path continuous after a real candle
    out.push({ time: t, open: R(o), high: R(Math.max(o, c) * (1 + d.wk[i])), low: R(Math.min(o, c) * (1 - d.wk[(i * 7 + 3) % 1440])), close: R(c), volume: sv }); prev = c;
  }
  mCache[s] = { key, arr: out }; return out;
}
const RANGES = { '1M': [30, 'd'], '3M': [91, 'd'], '6M': [182, 'd'], '1Y': [365, 'w'], '5Y': [1826, 'm'] };   // [days back, bucket: daily/weekly/monthly]
const SHORT = { '1min': [MIN, 5e3], '1h': [36e5, MIN], '12h': [12 * 36e5, 15 * MIN], '1d': [DAY, 30 * MIN], '1w': [7 * DAY, 4 * 36e5] };   // [window, candle size] in ms
const bucket = (t, m) => { if (m === 'd') return t; const d = new Date(t); return m === 'w' ? t - ((d.getUTCDay() + 6) % 7) * DAY : Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1); };
const merge = (arr, c, k) => { const l = arr[arr.length - 1]; if (l && l.time === k) { l.high = Math.max(l.high, c.high); l.low = Math.min(l.low, c.low); l.close = c.close; l.volume += c.volume; } else arr.push({ ...c, time: k }); };
for (const t of S.trades) if (tradesBy[t.symbol]) { tradesBy[t.symbol].push(t); last[t.symbol] = t.price;
  if (t.ts > Date.now() - 8 * DAY && Math.abs(t.price / simAt(t.symbol, t.ts) - 1) < .05) addCandle(t); }   // ignore old trades at prices unrelated to today's path
rebuildIndex(); replay();

// ---------- Liquidity bots ----------
const mkBot = (n, kind) => addUser(n, crypto.randomBytes(8).toString('hex'), crypto.randomBytes(32).toString('hex'), true, kind);   // bots never log in: random credentials, no KDF needed
const bots = [1, 2, 3, 4].map(i => Object.values(S.users).find(u => u.name === 'mm' + i) || mkBot('mm' + i));
const tricksters = [1, 2].map(i => Object.values(S.users).find(u => u.name === 'trickster' + i) || mkBot('trickster' + i, 'trickster'));   // misbehave on purpose (demo)
for (const b of [...bots, ...tricksters]) for (const k in SYMS) if (b.pos[k] == null) { b.pos[k] = 1e6; evlog.append({ t: 'p', uid: b.id, symbol: k, qty: 1e6 }); }   // bots hold every symbol, including newly added ones (logged so replay matches)
const checkpoint = () => ({ seq: S.seq, ts: Date.now(), last: { ...last },   // full state at this instant: the starting point of an event-log segment
  users: Object.fromEntries(Object.values(S.users).map(u => [u.id, { id: u.id, name: u.name, cash: u.cash, held: u.held, pos: { ...u.pos }, heldPos: { ...u.heldPos }, bot: u.bot, kind: u.kind }])),
  orders: Object.values(S.orders).filter(OPEN).sort((a, b) => a.id - b.id).map(o => ({ ...o })) });
evlog.init(checkpoint);
const trick = createTrickster({ place, cancel, books, last, users: () => tricksters, notify: m => console.log(m) });
for (const s in SYMS) {                                        // start every symbol on the simulated path; drop stale bot quotes from an older run
  const tg = Math.round(simAt(s, Date.now())), lt = tradesBy[s][tradesBy[s].length - 1];
  if (!lt || Date.now() - lt.ts > 6e5 || Math.abs(lt.price / tg - 1) > .01) {
    last[s] = tg;
    for (const o of Object.values(S.orders)) if (o.symbol === s && OPEN(o) && S.users[o.userId].bot) try { cancel(S.users[o.userId], o.id); } catch {}
  }
  mid[s] = last[s];
}
snapshot();
const botOpen = Object.values(S.orders).filter(o => OPEN(o) && S.users[o.userId].bot).map(o => o.id);
if (!process.env.NO_BOTS) setInterval(() => {
  const keys = Object.keys(SYMS), s = keys[Math.random() * keys.length | 0], u = bots[Math.random() * bots.length | 0];
  mid[s] = Math.max(100, mid[s] + (simAt(s, Date.now()) - mid[s]) * .15) * (1 + (Math.random() - .5) * .0003);   // follow the simulated path with tiny jitter
  const side = Math.random() < .5 ? 'buy' : 'sell', opp = side === 'buy' ? books[s].asks.best() : books[s].bids.best();
  let price;
  if (Math.random() < .35 && opp && Math.abs(opp.price - mid[s]) / mid[s] < .002) price = opp.price; // aggressive: trades now at the best opposite price
  else { const off = .0002 + Math.random() * .0006; price = Math.round(mid[s] * (1 + (side === 'buy' ? -off : off))); }          // passive: rests in the book
  try { const o = place(u, { symbol: s, side, price: Math.max(1, price), quantity: 1 + Math.random() * 30 | 0 }); if (OPEN(o)) botOpen.push(o.id); } catch {}
  while (botOpen.length > 25 * keys.length) { const o = S.orders[botOpen.shift()]; if (o && OPEN(o)) try { cancel(S.users[o.userId], o.id); } catch {} }
}, 150);

setInterval(() => { // market data + user notifications are batched
  for (const s of dirtySyms) emit({ t: 'book', symbol: s, ...book(s) });
  for (const c of clients) if (dirtyUsers.has(c.uid) && c.readyState === 1) c.send('{"t":"user"}');
  dirtySyms.clear(); dirtyUsers.clear();
}, 250);
setInterval(() => { snapshot(); evlog.maybeRotate(checkpoint); }, 30000);
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { snapshot(); process.exit(0); });
process.on('uncaughtException', e => console.error('uncaught', e));
process.on('unhandledRejection', e => console.error('unhandled', e));

// ---------- API gateway ----------
const app = express();
app.set('trust proxy', 1);               // behind Render/Heroku-style proxies req.ip is the real client
app.use(express.json({ limit: '10kb' }));
app.use(express.static(path.join(__dirname, 'public')));
const bad = (res, m, c = 400) => res.status(c).json({ error: String(m) });
const hits = new Map(), PUBLIC = new Set(['/api/symbols', '/api/currencies', '/api/trades', '/api/health']);
app.use((req, res, next) => { const md = req.path.startsWith('/marketdata') || PUBLIC.has(req.path) || (req.method === 'GET' && (req.path.startsWith('/api/replay') || req.path === '/api/surveillance')), k = req.ip + (md ? 'm' : 'a'), n = (hits.get(k) || 0) + 1;
  hits.set(k, n); n > (md ? MD_LIMIT : RATE_LIMIT) ? bad(res, 'Rate limit exceeded', 429) : next(); });
setInterval(() => hits.clear(), 60000);

const cookies = h => Object.fromEntries((h || '').split(';').map(x => x.trim().split('=')).filter(x => x[0]).map(([k, ...v]) => [k, v.join('=')]));
const userOf = tok => { const s = S.sessions[tok]; return s && s.exp > Date.now() ? S.users[s.u] : null; };
const tokenOf = req => (req.headers.authorization || '').startsWith('Bearer ') ? req.headers.authorization.slice(7) : cookies(req.headers.cookie).sid;
const auth = (req, res, next) => { const u = userOf(tokenOf(req)); u ? (req.user = u, next()) : bad(res, 'Unauthorized', 401); };
function startSession(u, req, res) {   // HttpOnly cookie: JS (and XSS) can't read it; token is also returned for API clients
  const token = crypto.randomBytes(24).toString('hex'), exp = Date.now() + SESSION_TTL;
  S.sessions[token] = { u: u.id, exp }; journal({ t: 's', token, uid: u.id, exp });
  res.setHeader('Set-Cookie', `sid=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${SESSION_TTL / 1000}${req.secure ? '; Secure' : ''}`);
  return { token, user: view(u) };
}
const taken = name => Object.values(S.users).some(u => u.name.toLowerCase() === name.toLowerCase());

app.post('/api/register', async (req, res) => {
  try {
    const { name, password } = req.body || {};
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(name || '') || typeof password !== 'string' || password.length < 6 || password.length > 128) return bad(res, 'Username: 3-20 letters/digits; password: 6+ characters');
    if (taken(name)) return bad(res, 'Username taken');
    const salt = crypto.randomBytes(8).toString('hex'), h = (await hashA(password, salt, 32)).toString('hex');
    if (taken(name)) return bad(res, 'Username taken');   // re-check: another request may have won while hashing
    res.json(startSession(addUser(name, salt, h, false), req, res));
  } catch { bad(res, 'Server error', 500); }
});
app.post('/api/login', async (req, res) => {
  try {
    const { name, password } = req.body || {}, u = typeof name === 'string' && Object.values(S.users).find(x => x.name.toLowerCase() === name.toLowerCase() && !x.bot);
    if (!u || typeof password !== 'string' || password.length > 128) return bad(res, 'Invalid credentials', 401);
    const h = await hashA(password, u.salt, 32);
    if (!crypto.timingSafeEqual(h, Buffer.from(u.hash, 'hex'))) return bad(res, 'Invalid credentials', 401);
    res.json(startSession(u, req, res));
  } catch { bad(res, 'Server error', 500); }
});
app.post('/api/logout', (req, res) => { const t = tokenOf(req); if (S.sessions[t]) { delete S.sessions[t]; journal({ t: 'x', token: t }); }
  res.setHeader('Set-Cookie', 'sid=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0'); res.json({ ok: true }); });
app.get('/api/me', auth, (req, res) => res.json({ user: view(req.user),
  orders: (uo.get(req.user.id) || []).slice(-100).reverse().map(id => S.orders[id]).filter(Boolean).map(fmt) }));
app.get('/api/health', (_, res) => res.json({ ok: true, users: Object.keys(S.users).length }));
app.get('/api/currencies', (_, res) => res.json(CURRENCIES));
app.get('/api/symbols', (_, res) => res.json(Object.fromEntries(Object.keys(SYMS).map(s => [s, { last: last[s], ref: refOf(s) }]))));
app.post('/v1/order', auth, (req, res) => { try { res.json(fmt(place(req.user, req.body || {}))); } catch (e) { bad(res, e); } });
app.delete('/v1/order/:id', auth, (req, res) => { try { res.json(fmt(cancel(req.user, +req.params.id))); } catch (e) { bad(res, e); } });
app.get('/execution', auth, (req, res) => {
  const { symbol, orderId, startTime = 0, endTime = Date.now() } = req.query, mine = new Set(uo.get(req.user.id) || []), out = [];
  for (const t of S.trades) if ((!symbol || t.symbol === symbol) && t.ts >= +startTime && t.ts <= +endTime)
    for (const [oid, side] of [[t.buyOrderId, 'buy'], [t.sellOrderId, 'sell']])
      if (mine.has(oid) && (!orderId || +orderId === oid)) out.push({ id: t.id, orderId: oid, symbol: t.symbol, side, price: t.price, orderType: 'limit', quantity: t.qty });
  res.json({ executions: out.slice(-200) });
});
app.get('/marketdata/orderBook/L2', (req, res) => books[req.query.symbol] ? res.json(book(req.query.symbol, Math.min(50, +req.query.depth || 10))) : bad(res, 'Unknown symbol'));
app.get('/marketdata/candles', (req, res) => {   // 1min / 1h / 12h / 1d / 1w: candles over the last window, always continuous
  const { symbol } = req.query; if (!books[symbol]) return bad(res, 'Unknown symbol');
  const rg = SHORT[req.query.range || '1h']; if (!rg) return bad(res, 'Unknown range (use 1min, 1h, 12h, 1d, 1w)');
  const [span, step] = rg, endB = Math.floor(Date.now() / step) * step, startB = endB - span, mins = getMinutes(symbol), out = [];
  if (step >= MIN) { for (const c of mins) if (c.time >= startB) merge(out, c, Math.floor(c.time / step) * step); return res.json({ candles: out }); }
  const t0 = mins[0].time, px = t => { const c = mins[Math.min(mins.length - 1, Math.max(0, Math.floor((t - t0) / MIN)))]; return c.open + (c.close - c.open) * ((t % MIN) / MIN); };
  const real = new Map(), list = cand[symbol][5]; for (let i = list.length - 1; i >= 0 && list[i].time >= startB; i--) real.set(list[i].time, list[i]);
  let prev = 0;   // 1min view: real 5s candles; quiet gaps stay flat (before the first trade in view they follow the simulated path)
  for (let t = startB; t <= endB; t += step) { const c = real.get(t); if (c) { out.push(c); prev = c.close; } else { const v = prev || Math.round(px(t)); out.push({ time: t, open: v, high: v, low: v, close: v, volume: 0 }); } }
  res.json({ candles: out });
});
app.get('/marketdata/history', (req, res) => {   // 1M / 3M / 6M / 1Y / 5Y: daily/weekly/monthly candles + today's candle built from the minute data
  const s = req.query.symbol; if (!books[s]) return bad(res, 'Unknown symbol');
  const rg = RANGES[req.query.range || '1M']; if (!rg) return bad(res, 'Unknown range (use 1M, 3M, 6M, 1Y, 5Y)');
  const from = Date.now() - rg[0] * DAY, src = getHist(s).filter(c => c.time >= from), today = Math.floor(Date.now() / DAY) * DAY, tm = [];
  for (const c of getMinutes(s)) if (c.time >= today) merge(tm, c, today);
  if (tm.length) src.push(tm[0]);
  const out = []; for (const c of src) merge(out, c, bucket(c.time, rg[1]));
  res.json({ candles: out, interval: rg[1] });
});
app.get('/api/trades', (req, res) => tradesBy[req.query.symbol] ? res.json(tradesBy[req.query.symbol].slice(-40).reverse().map(t => ({ price: t.price, qty: t.qty, side: t.taker, ts: t.ts }))) : bad(res, 'Unknown symbol'));
// ---------- Surveillance ----------
app.get('/api/surveillance', auth, (req, res) => res.json({ alerts: surv.list(100), accounts: surv.accounts(15), stats: surv.stats(), config: surv.cfg, demo: DEMO, running: trick.running(), head: seqNow() }));
app.post('/api/surveillance/demo', auth, async (req, res) => {   // make the trickster bot misbehave on cue
  if (!DEMO) return bad(res, 'Demo is disabled on this server', 403);
  try { const { scenario = 'all', symbol = 'TSLA' } = req.body || {}; await trick.run(String(scenario), String(symbol)); res.json({ ok: true, scenario, symbol }); } catch (e) { bad(res, e, 409); }
});

// ---------- Time-travel replay (runs in a worker thread; see lib/replay-worker.js) ----------
let worker = null, rid = 0; const pend = new Map();
function spawnWorker() {
  worker = new Worker(path.join(__dirname, 'lib', 'replay-worker.js'), { workerData: { dir: EVENTS, symbols: Object.keys(SYMS) } });
  worker.on('message', ({ id, result, error }) => { const p = pend.get(id); if (!p) return; pend.delete(id); clearTimeout(p.t); error ? p.rej(error) : p.res(result); });
  worker.on('error', e => console.error('replay worker', e.message));
  worker.on('exit', () => { for (const p of pend.values()) { clearTimeout(p.t); p.rej('Replay worker restarted, try again'); } pend.clear(); worker = null; });
  worker.unref();
}
const ask = msg => new Promise((res, rej) => { if (!worker) spawnWorker(); const id = ++rid, t = setTimeout(() => { pend.delete(id); rej('Replay timed out'); }, 20000); pend.set(id, { res, rej, t }); worker.postMessage({ id, ...msg }); });
const nz = m => Object.fromEntries(Object.entries(m || {}).filter(([, v]) => v).sort(([a], [b]) => a < b ? -1 : 1));
app.get('/api/replay/meta', auth, (req, res) => { const m = evlog.meta();
  res.json({ minSeq: m.minQ, minTs: m.minTs, headSeq: seqNow(), headTs: Date.now(), segments: m.segments.length, alerts: surv.list(200).map(a => ({ id: a.id, q: a.q, ts: a.ts, type: a.type, severity: a.severity, symbol: a.symbol })) }); });
app.get('/api/replay', auth, async (req, res) => {                // the whole exchange as of sequence number `seq`
  const head = seqNow(); let q = req.query.seq === undefined ? head : Math.floor(+req.query.seq);
  if (!Number.isFinite(q)) return bad(res, 'seq must be a number'); q = Math.min(q, head);
  try { const st = await ask({ type: 'state', q, uid: req.user.id }); st.alerts = surv.upTo(st.seq); st.head = head; res.json(st); } catch (e) { bad(res, e); }
});
app.get('/api/replay/verify', auth, async (req, res) => {         // proof: rebuild everything from the log and compare with the live exchange at the same sequence number
  const q = seqNow(), live = { users: {}, books: {} };
  for (const u of Object.values(S.users)) live.users[u.id] = [u.cash, u.held, nz(u.pos), nz(u.heldPos)];
  for (const s in SYMS) live.books[s] = { bids: JSON.stringify(books[s].bids.depth(50)), asks: JSON.stringify(books[s].asks.depth(50)) };
  try { res.json(await ask({ type: 'verify', q, live })); } catch (e) { bad(res, e); }
});
app.use((err, req, res, next) => bad(res, err.type === 'entity.parse.failed' ? 'Invalid JSON' : 'Server error', err.status || 500));

const server = http.createServer(app), wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws, req) => {      // auth via the HttpOnly session cookie (no token in the URL); same-origin only
  const o = req.headers.origin; if (o) { try { if (new URL(o).host !== req.headers.host) return ws.close(); } catch { return ws.close(); } }
  const u = userOf(cookies(req.headers.cookie).sid); ws.uid = u && u.id;
  clients.add(ws); ws.on('close', () => clients.delete(ws)); ws.on('error', () => {});
});
server.listen(PORT, () => console.log('Exchange running on :' + PORT));
module.exports = { S, place, cancel, books, book, addUser, cand, getHist };
