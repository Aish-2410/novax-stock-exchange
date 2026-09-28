// Order book side: price levels (FIFO queue per level) + sorted price array; lazy deletion for cancels.
// Shared by the live engine (server.js) and the replay worker so both use identical book logic.
const OPEN = o => o.status === 'new' || o.status === 'partially_filled';
class Side {
  constructor(asc) { this.asc = asc; this.prices = []; this.levels = new Map(); }
  idx(p) { let lo = 0, hi = this.prices.length; while (lo < hi) { const m = (lo + hi) >> 1; (this.asc ? this.prices[m] < p : this.prices[m] > p) ? lo = m + 1 : hi = m; } return lo; }
  add(o) { let lv = this.levels.get(o.price); if (!lv) { lv = { q: [], size: 0 }; this.levels.set(o.price, lv); this.prices.splice(this.idx(o.price), 0, o.price); } lv.q.push(o); lv.size += o.qty - o.filled; }
  drop(p) { const i = this.idx(p); if (this.prices[i] === p) this.prices.splice(i, 1); this.levels.delete(p); }
  best() { while (this.prices.length) { const p = this.prices[0], lv = this.levels.get(p); while (lv.q.length && !OPEN(lv.q[0])) lv.q.shift(); if (lv.q.length) return lv.q[0]; this.drop(p); } return null; }
  fill(o, q) { const lv = this.levels.get(o.price); lv.size -= q; if (o.filled === o.qty) { lv.q.shift(); if (lv.size <= 0) this.drop(o.price); } }
  remove(o) { const lv = this.levels.get(o.price); if (!lv) return; lv.size -= o.qty - o.filled; if (lv.size <= 0) this.drop(o.price); else if (lv.q.length > 64) lv.q = lv.q.filter(OPEN); }
  depth(n) { const out = []; for (const p of this.prices) { const lv = this.levels.get(p); if (lv.size > 0) out.push({ price: p, size: lv.size }); if (out.length >= n) break; } return out; }
}
module.exports = { Side, OPEN };
