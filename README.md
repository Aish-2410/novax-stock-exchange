# NovaX – Simulated Stock Exchange

Run: `npm install && npm start` → http://localhost:3000

## Features
Login/registration, 12 stocks, live order book, price-time matching, portfolio, WebSocket updates,
liquidity bots, and display currencies (USD, INR, EUR, GBP, JPY). The engine stores USD cents only;
currency is converted for display and order entry. Override rates: `FX_RATES='{"INR":84}'`.

## Architecture notes
- Order book: price levels (FIFO queue per level) + sorted price array; O(1) best price, lazy cancel.
- Self-trade prevention: a taker that would match its own resting order cancels the resting order.
- Durability: append-only `journal.log` (users, sessions, orders, cancels) + snapshot every 30s; startup = snapshot + journal replay (verified: kill -9 recovers identical state).
- Chart ranges: 1 min (5s candles), 1 hr (1m), 12 hr (15m), 1 day (30m), 1 week (4h), then 1M/3M/6M (daily), 1Y (weekly), 5Y (monthly).
  All ranges come from ONE deterministic simulated price path (fixed seeds: identical after every restart/deploy, no daily rewrites), with real
  engine candles laid over the minutes where trades actually happened. Volatility scales with sqrt(time), so a 1-minute window barely moves
  while a 1-week window moves several percent. The liquidity bots follow the same path with tiny jitter and tight quotes, so live prices match the charts.
  `GET /marketdata/candles?symbol=AAPL&range=1h` (1min|1h|12h|1d|1w) and `GET /marketdata/history?symbol=AAPL&range=6M` (1M|3M|6M|1Y|5Y).
- Watchlist % change is versus the previous UTC day's close on the simulated path.
- Auth: scrypt (async), 7-day sessions in an HttpOnly SameSite cookie, WebSocket authenticates by cookie + same-origin check. `/login.html` is the login page.
- Rate limits: per-IP, separate buckets for trading/auth and public market data; `trust proxy` enabled for Render.

## Surveillance (Surveillance tab)
Detects spoofing, wash trading (self-trade attempts + two-account round trips) and pump patterns, with a decaying 0-100 risk score per account.
Every alert stores evidence (sequence range, order/trade ids) and links to the replay. Alerts persist in `alerts.log`.
Trickster bot: two bot accounts misbehave on cue via buttons in the tab. Set `DEMO_TRICKSTER=0` on public deployments.
Thresholds: `SURV_*` env vars (see lib/surveillance.js). Pre-demo check: `npm run selftest`.

## Time travel (Time travel tab)
`events/` is a never-truncated, segmented event log (each segment starts with a full checkpoint). A worker thread rebuilds the exchange
at any sequence number. "Verify against live exchange" replays the log and compares it to live state.
`EVENT_SEGMENT_MB` (default 20), `EVENT_SEGMENTS` (default 8, about 2.5 days with bots).

## Env vars (no API keys needed)
PORT, DATA_DIR, MAX_DAILY_QTY, RATE_LIMIT (trading/auth req/min/IP, default 300), MD_LIMIT (market data, default 5x), FX_RATES, NO_BOTS=1 (disable liquidity bots)

## 50-user load test
```
RATE_LIMIT=100000 npm start          # terminal 1
node loadtest.js http://localhost:3000 50 60   # terminal 2 (users, seconds)
```
Checks: all users register/login, WebSockets stay open, no 5xx, no duplicate order IDs, no negative cash/holdings.

## Deploy
Render: push to GitHub, New → Blueprint (uses render.yaml + Dockerfile, 1 GB disk at /data).
