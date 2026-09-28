// Append-only event log = the exchange's evidence trail and the source for time-travel replay.
//
// Unlike journal.log (which is truncated after every snapshot and only exists for crash recovery), the event log is
// never truncated. It is split into SEGMENTS so it can be retained/pruned without losing the ability to replay:
//   events/seg-<startSeq>-<startTs>.log
//     line 1 : {"t":"cp", ...}  checkpoint = full account balances + open orders at the moment the segment started
//     line 2+: {"t":"u"|"o"|"c"|"p", "q":<seq marker>, ...}  one event per line, in the order they were applied
//
// "q" is the exchange-wide sequence number at the time of the event (orders use their own id, cancels/users use the last
// assigned sequence number). It is non-decreasing, so "state as of sequence N" = checkpoint + every event with q <= N.
// Events are written with writeSync on a persistent fd, so a crash (kill -9) loses nothing that was acknowledged.
const fs = require('fs'), path = require('path');

module.exports = function createEventLog({ dir, segBytes, keep, seq }) {
  fs.mkdirSync(dir, { recursive: true });
  let fd = null, file = null, pending = [], segs = [];

  const pad = n => String(n).padStart(12, '0');
  const scan = () => {
    segs = fs.readdirSync(dir).map(f => { const m = /^seg-(\d+)-(\d+)\.log$/.exec(f); return m && { file: f, q: +m[1], ts: +m[2] }; })
      .filter(Boolean).sort((a, b) => a.q - b.q || a.ts - b.ts);
  };
  function openNew(cp) {
    if (fd != null) fs.closeSync(fd);
    const q = cp.seq - 1, name = `seg-${pad(q)}-${cp.ts}.log`;
    file = path.join(dir, name);
    fs.writeFileSync(file, JSON.stringify({ t: 'cp', q, ...cp }) + '\n');
    fd = fs.openSync(file, 'a'); scan();
  }
  function prune() {
    while (segs.length > keep) { const s = segs.shift(); try { fs.unlinkSync(path.join(dir, s.file)); } catch {} }
  }
  const write = ev => { try { fs.writeSync(fd, JSON.stringify(ev) + '\n'); } catch (e) { console.error('eventlog write failed', e.message); } };

  return {
    dir,
    // Call once state is fully loaded. First run: start a segment from the current state. Later runs: continue the last one.
    init(checkpoint) {
      scan();
      if (!segs.length) { openNew(checkpoint()); pending = []; }   // pending events are already reflected in the checkpoint
      else { file = path.join(dir, segs[segs.length - 1].file); fd = fs.openSync(file, 'a'); for (const ev of pending) write(ev); pending = []; }
    },
    append(ev) {
      if (ev.t === 'o') ev = { ...ev, q: ev.id };
      else if (ev.t === 'u') ev = { t: 'u', q: seq() - 1, ts: ev.ts ?? Date.now(), user: { id: ev.user.id, name: ev.user.name, cash: ev.user.cash, pos: ev.user.pos, bot: ev.user.bot, kind: ev.user.kind } };
      else if (ev.t === 'c' || ev.t === 'p') ev = { ...ev, q: seq() - 1 };
      else return;                                              // sessions etc. never enter the evidence log
      fd == null ? pending.push(ev) : write(ev);
    },
    // Called periodically: start a new segment (with a fresh checkpoint) once the current one is big enough.
    maybeRotate(checkpoint) {
      if (fd == null) return;
      try { if (fs.fstatSync(fd).size < segBytes) return; openNew(checkpoint()); prune(); } catch (e) { console.error('eventlog rotate failed', e.message); }
    },
    meta() { scan(); return { segments: segs.map(s => ({ ...s })), minQ: segs.length ? segs[0].q : null, minTs: segs.length ? segs[0].ts : null }; },
  };
};
