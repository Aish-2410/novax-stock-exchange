// Time-travel replay, run in a worker thread so scrubbing never blocks the live matching engine.
// Rebuilds the exchange (order books, balances, last prices) as of sequence number N from
//   the newest checkpoint <= N  +  every logged event with q <= N   (see lib/eventlog.js).
// The matching rules below mirror match()/place()/cancel() in server.js and use the very same book class (lib/book.js).
const { parentPort, workerData } = require('worker_threads');
const fs = require('fs'), path = require('path');
const { Side, OPEN } = require('./book');
const { dir, symbols } = workerData;

// ---------- Incremental segment reader: only new bytes are read/parsed when the (live) segment grows ----------
const cache = new Map();
function load(name) {
  let c = cache.get(name);
  if (!c) { c = { off: 0, cp: null, events: [] }; cache.set(name, c); while (cache.size > 2) cache.delete(cache.keys().next().value); }
  const fd = fs.openSync(path.join(dir, name), 'r');
  try {
    const size = fs.fstatSync(fd).size;
    if (size > c.off) {
      const buf = Buffer.alloc(size - c.off); fs.readSync(fd, buf, 0, buf.length, c.off);
      const txt = buf.toString('utf8'), nl = txt.lastIndexOf('\n');
      if (nl >= 0) {
        c.off += Buffer.byteLength(txt.slice(0, nl + 1));
        for (const line of txt.slice(0, nl).split('\n')) {
          if (!line) continue; let e; try { e = JSON.parse(line); } catch { continue; }
          if (e.t === 'cp' && !c.cp) c.cp = e; else c.events.push(e);
        }
      }
    }
  } finally { fs.closeSync(fd); }
  return c;
}
const segments = () => fs.readdirSync(dir).map(f => { const m = /^seg-(\d+)-(\d+)\.log$/.exec(f); return m && { file: f, q: +m[1], ts: +m[2] }; }).filter(Boolean).sort((a, b) => a.q - b.q || a.ts - b.ts);

// ---------- Engine (state rebuilt from events) ----------
class Engine {
  constructor(cp) {
    cp = structuredClone(cp);
    this.seq = cp.seq; this.ts = cp.ts; this.users = cp.users; this.last = cp.last || {}; this.applied = 0; this.trades = 0; this.open = 0;
    this.orders = new Map(); this.books = {};
    for (const s of symbols) this.books[s] = { bids: new Side(false), asks: new Side(true) };
    for (const o of cp.orders) if (this.books[o.symbol]) { this.orders.set(o.id, o); this.books[o.symbol][o.side === 'buy' ? 'bids' : 'asks'].add(o); }
  }
  release(o) { const u = this.users[o.userId], rem = o.qty - o.filled; if (o.side === 'buy') u.held -= o.price * rem; else u.heldPos[o.symbol] -= rem; }
  apply(e) {
    this.applied++; if (e.ts) this.ts = e.ts;
    if (e.t === 'u') { if (!this.users[e.user.id]) this.users[e.user.id] = { held: 0, heldPos: {}, ...e.user, pos: { ...e.user.pos } }; }
    else if (e.t === 'p') { const u = this.users[e.uid]; if (u) u.pos[e.symbol] = (u.pos[e.symbol] || 0) + e.qty; }
    else if (e.t === 'o') {
      const u = this.users[e.uid]; if (!u || !this.books[e.symbol]) return;
      if (e.side === 'buy') u.held += e.price * e.quantity; else u.heldPos[e.symbol] = (u.heldPos[e.symbol] || 0) + e.quantity;
      this.seq = e.id + 1;
      const o = { id: e.id, userId: e.uid, symbol: e.symbol, side: e.side, price: e.price, qty: e.quantity, filled: 0, status: 'new', ts: e.ts };
      this.orders.set(o.id, o); this.match(o);
    } else if (e.t === 'c') {
      const o = this.orders.get(e.id); if (!o || !OPEN(o)) return;
      o.status = 'canceled'; this.release(o); const b = this.books[o.symbol]; (o.side === 'buy' ? b.bids : b.asks).remove(o);
    }
  }
  match(o) {
    const b = this.books[o.symbol], opp = o.side === 'buy' ? b.asks : b.bids, mine = o.side === 'buy' ? b.bids : b.asks; let r;
    while (o.filled < o.qty && (r = opp.best())) {
      if (o.side === 'buy' ? r.price > o.price : r.price < o.price) break;
      if (r.userId === o.userId) { r.status = 'canceled'; this.release(r); opp.remove(r); continue; }   // self-trade prevention
      const q = Math.min(o.qty - o.filled, r.qty - r.filled), p = r.price;
      const [bo, so] = o.side === 'buy' ? [o, r] : [r, o], bu = this.users[bo.userId], su = this.users[so.userId];
      bu.cash -= p * q; bu.held -= bo.price * q; bu.pos[o.symbol] = (bu.pos[o.symbol] || 0) + q;
      su.cash += p * q; su.pos[o.symbol] -= q; su.heldPos[o.symbol] -= q;
      o.filled += q; r.filled += q;
      for (const x of [o, r]) x.status = x.filled === x.qty ? 'filled' : 'partially_filled';
      opp.fill(r, q); this.seq++; this.trades++; this.last[o.symbol] = p;
    }
    if (o.filled < o.qty) mine.add(o);
  }
}

function build(q) {
  const segs = segments(); if (!segs.length) throw 'No event log yet';
  if (q < segs[0].q) throw `Sequence ${q} is older than the retained history (starts at ${segs[0].q})`;
  let s = segs[0]; for (const x of segs) if (x.q <= q) s = x;
  const c = load(s.file); if (!c.cp) throw 'Event log segment has no checkpoint';
  const eng = new Engine(c.cp); eng.ts = c.cp.ts; let reached = c.cp.q;
  for (const e of c.events) { if (e.q > q) break; eng.apply(e); reached = e.q; }
  return { eng, reached, segment: s.file };
}

const compactMap = m => Object.fromEntries(Object.entries(m || {}).filter(([, v]) => v).sort(([a], [b]) => a < b ? -1 : 1));   // sorted: stable comparison
const depthKey = (d, n) => JSON.stringify(d(n));

const handlers = {
  state({ q, uid, depth = 10 }) {
    const { eng, reached, segment } = build(q), out = {};
    for (const s of symbols) out[s] = { last: eng.last[s] ?? null, bids: eng.books[s].bids.depth(depth), asks: eng.books[s].asks.depth(depth) };
    const worth = u => { let v = u.cash; for (const s in u.pos) v += (u.pos[s] || 0) * (eng.last[s] || 0); return v; };
    const humans = Object.values(eng.users).filter(u => !u.bot);
    const accounts = humans.map(u => ({ name: u.name, cash: u.cash, held: u.held, equity: worth(u) })).sort((a, b) => b.equity - a.equity).slice(0, 10);
    const me = uid && eng.users[uid] ? { name: eng.users[uid].name, cash: eng.users[uid].cash, held: eng.users[uid].held, pos: compactMap(eng.users[uid].pos), equity: worth(eng.users[uid]) } : null;
    let open = 0; for (const o of eng.orders.values()) if (OPEN(o)) open++;
    return { seq: reached, ts: eng.ts, segment, symbols: out, accounts, me, stats: { users: humans.length, openOrders: open, trades: eng.trades, eventsApplied: eng.applied } };
  },
  // Replay the whole log up to q and compare with a summary of the live exchange captured at the same sequence number.
  verify({ q, live }) {
    const { eng, segment, reached } = build(q), bad = [];
    let users = 0;
    for (const [id, l] of Object.entries(live.users)) {
      const r = eng.users[id]; users++;
      if (!r) { bad.push(`user ${id} missing in replay`); continue; }
      const a = JSON.stringify([r.cash, r.held, compactMap(r.pos), compactMap(r.heldPos)]), b = JSON.stringify(l);
      if (a !== b) bad.push(`${r.name}: replay ${a} != live ${b}`);
    }
    for (const s of symbols) {
      if (depthKey(n => eng.books[s].bids.depth(n), 50) !== live.books[s].bids) bad.push(`${s} bids differ`);
      if (depthKey(n => eng.books[s].asks.depth(n), 50) !== live.books[s].asks) bad.push(`${s} asks differ`);
    }
    return { ok: !bad.length, seq: q, reached, segment, users, symbols: symbols.length, eventsApplied: eng.applied, mismatches: bad.slice(0, 10) };
  },
};

parentPort.on('message', ({ id, type, ...args }) => {
  try { parentPort.postMessage({ id, result: handlers[type](args) }); }
  catch (e) { parentPort.postMessage({ id, error: String(e && e.message || e) }); }
});
