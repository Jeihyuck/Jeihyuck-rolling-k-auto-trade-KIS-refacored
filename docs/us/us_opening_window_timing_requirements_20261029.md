# US Opening-Window Decision Evidence — required before enabling Timing Gate

## Reproduced incident (US practice 2026-10-28)
- `trader/us/runner/trade_tick_runner.py::_is_us_opening_buy_blocked` defaults to 09:30–10:00 ET BUY exclusion (`US_OPENING_BUY_BLOCK_MINUTES=30`).
- At 09:30:39, 09:34:57, 09:39:31, 09:44:07, 09:48:38, 09:53:13, 09:57:47 ET the tick explicitly logged `OPENING_30MIN_BUY_BLOCK`.
- First post-block 10:02:21 ET tick; PB1 intents ELV/MCK/PG at 10:03:34–36, MDLZ 10:08:26.
- No verified per-symbol 09:30–10:00 opening-window OHLCV/VWAP path was demonstrated in these entries.
- The #279 shadow currently operates in the entry-evaluation path. It cannot *create* opening-window history while that path is suppressed. Existing read-only shadow is not a valid fix for the opening-window omission.

## Required architecture (owner-isolated, no trading policy change in shadow)
1. A **separate opening observer** runs even if `entry_can_proceed=False`; no order intent or risk authority and never blocks SELL/TQQQ.
2. Its quotes come from trusted timestamped broker WS/batch feeds. No N-by-symbol REST polling in the main Tick. Enforce bounded per-cycle work, timeout, dedupe and backpressure.
3. A *durable* opening window keyed by `account_environment, trading_epoch, US_trade_date, symbol, strategy_owner, observation_source`; keep raw 1m/5m bar provenance separately from irregular quote snapshots.
4. Compute observed open/high/low/last, last-5m trend, opening-range top/bottom, volume and VWAP **only from valid market bars/trade-volume inputs**. Tick price samples alone do NOT establish high/low, VWAP, relative volume, or consecutive candle closes.
5. 10:00 ET is first time a sufficient opening window may be evaluated. For missing/stale data, record `WAIT_EVIDENCE_UNKNOWN`; never manufacture a BUY_READY result.
6. Compare strategy-specific conditions in Shadow: Momentum continuation/reclaim; Pullback support/decline deceleration; Breakout pivot/volume; VCP retain existing verified live pivot/volume proof. TQQQ Infinite is not modified.
7. After 10:00, continuously evaluate with rolling bars, and recheck price/age/trigger validity before BUY broker submission inside existing Tick budget. The **initial phase remains shadow-only**; production BUY gating needs separate approval and replay evidence.
8. Persist candidate WAIT, BUY_READY, INVALIDATED with timestamps even when no order is placed. 5W1H should include which observations were available, missing, and compared with PREP.
9. Replay historical 10/28 real data when obtainable. Test 10:00 false breakout, reversal after 10:00, stale quote, irregular sample fake bars, missing VWAP, rate limit/WS disconnect, TQQQ owner, concurrent protective SELL, restart and epoch isolation.
10. Provide matched-time baseline immediate-entry vs proposed gating, unrealized excursion MAE, adverse selection, missed rallies, slippage and PnL. No return improvement is assumed without validated tests.

## Merge/deployment distinction
#279 read-only instrumentation can be code-merged after CI and integration checks without changing strategy, but **must not be described as the completed 30-minute entry timing solution**. The gate should remain disabled until the opening observer + bar proof + historical shadow replay pass.
