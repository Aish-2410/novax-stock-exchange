// "Trickster" bot: two accounts that misbehave on purpose so the surveillance demo fires on cue.
// It goes through the normal place()/cancel() path (risk checks, journal, event log, matching), so what surveillance sees
// is exactly what it would see from a real misbehaving account.
const wait = ms => new Promise(r => setTimeout(r, ms));

module.exports = function createTrickster({ place, cancel, books, last, users, notify }) {
  let running = null;
  const T = () => users();                                    // [trickster1, trickster2]
  const top = (sym, side) => books[sym][side].best();
  const size = (sym, side, upTo) => { const s = books[sym][side]; let n = 0; for (const p of s.prices) { if (upTo(p)) { n += s.levels.get(p).size; } else break; } return n; };
  const say = m => notify && notify(m);

  // Scenarios need a two-sided book. Wait for the liquidity bots; if the book is still empty (fresh start), seed quotes ourselves.
  async function liquid(sym) {
    for (let i = 0; i < 24; i++) { if (top(sym, 'bids') && top(sym, 'asks')) return; await wait(250); }
    const [a] = T(), p = last[sym];
    try { place(a, { symbol: sym, side: 'sell', price: Math.round(p * 1.002), quantity: 20 }); place(a, { symbol: sym, side: 'buy', price: Math.round(p * .998), quantity: 20 }); } catch {}
  }

  // A cross between the same account (blocked by self-trade prevention, but the attempt is flagged).
  async function selfTrade(sym) {
    const [a] = T();
    for (let i = 0; i < 3; i++) {
      const bb = top(sym, 'bids'), ba = top(sym, 'asks'); if (!bb || !ba) return;
      const p = Math.max(bb.price + 1, Math.floor((bb.price + ba.price) / 2));
      if (p >= ba.price) return;
      const s = place(a, { symbol: sym, side: 'sell', price: p, quantity: 50 });        // rests
      const b = place(a, { symbol: sym, side: 'buy', price: p, quantity: 50 });         // would trade with itself -> blocked + flagged
      try { cancel(a, b.id); } catch {}
      await wait(400);
    }
  }

  // Two accounts pass the same shares back and forth at the mid price: volume with no real change of ownership.
  async function wash(sym) {
    const [a, b] = T();
    for (let i = 0; i < 8; i++) {
      const bb = top(sym, 'bids'), ba = top(sym, 'asks'); if (!bb || !ba) return;
      const p = Math.floor((bb.price + ba.price) / 2); if (p <= bb.price || p >= ba.price) { await wait(300); continue; }
      const [seller, buyer] = i % 2 ? [b, a] : [a, b];
      place(seller, { symbol: sym, side: 'sell', price: p, quantity: 100 });
      place(buyer, { symbol: sym, side: 'buy', price: p, quantity: 100 });
      await wait(350);
    }
  }

  // Layered big bids near the top of the book to fake demand, cancelled within seconds before anyone can hit them.
  async function spoof(sym) {
    const [a] = T();
    for (let round = 0; round < 4; round++) {
      const ids = [];
      for (let i = 0; i < 2; i++) {
        const bb = top(sym, 'bids'), ba = top(sym, 'asks'); if (!bb || !ba) return;
        const p = Math.min(bb.price + 1, ba.price - 1); if (p < 1) return;
        try { ids.push(place(a, { symbol: sym, side: 'buy', price: p, quantity: 2500 + i * 500 }).id); } catch {}
      }
      await wait(1800);
      for (const id of ids) try { cancel(a, id); } catch {}
      await wait(500);
    }
  }

  // Ramp the price up in steps: one account posts a higher ask, the other sweeps the book up to it and buys it.
  async function pump(sym) {
    const [a, b] = T(), base = last[sym];
    for (let k = 1; k <= 5; k++) {
      const p = Math.round(base * (1 + .005 * k));
      place(a, { symbol: sym, side: 'sell', price: p, quantity: 200 });
      const need = size(sym, 'asks', x => x <= p);                                       // everything up to and including our own ask
      place(b, { symbol: sym, side: 'buy', price: p, quantity: need });
      await wait(600);
    }
  }

  const scenarios = { self: selfTrade, wash, spoof, pump };
  async function run(name, sym) {
    if (running) throw `Demo already running: ${running}`;
    if (!books[sym]) throw 'Unknown symbol';
    const list = name === 'all' ? ['self', 'wash', 'spoof', 'pump'] : [name];
    if (list.some(n => !scenarios[n])) throw 'Unknown scenario (self, wash, spoof, pump, all)';
    running = name;
    (async () => { try { for (const n of list) { say(`trickster: ${n} on ${sym}`); await liquid(sym); await scenarios[n](sym); await wait(700); } } catch (e) { console.error('trickster', e); } finally { running = null; say('trickster: done'); } })();
  }
  return { run, running: () => running };
};
