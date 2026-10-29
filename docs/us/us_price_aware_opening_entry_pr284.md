# US PB1 Price-Aware Entry / Opening Window — PR #284

## Purpose and current-policy distinction

The existing US PB1 has a time-only 09:30–10:00 ET BUY block. The guard itself
works as designed (EXPECTED), but the old engine did not independently examine
opening-window bars and the price-to-structural-risk geometry before a standard
Pullback/Momentum/Breakout NEW_BUY (POLICY REVIEW).

This PR **does not claim to improve profitability**. It supplies the observation
pipeline, experimental owner-specific signal rules, broker-price ceiling, and a
practice-only approval gate to test whether it reduces adverse entry selection.

### Evidence sources

- Native KIS US 1-minute OHLCV: `GET /uapi/overseas-price/v1/quotations/inquire-time-itemchartprice`,
  TR `HHDFS76950200`, `NMIN=1`, exchange-local `xymd/xhms`,
  `open/high/low/last/evol/eamt`. Incomplete current-minute and wrong-day
  rows are dropped. The sidecar polls one symbol every 10 seconds by default,
  rather than querying the Final30 sequentially inside the Tick.
- Existing read-only KIS WebSocket collector records timestamped
  last-price/bid/ask **snapshots separately from bars**. Irregular snapshots
  cannot establish one-minute OHLCV or volume-relative-to-clock.
- True opening VWAP is produced only if native minute turnover and volume are
  valid and consistent with the price units. VWAP is NOT a mandatory blanket
  condition for PB1 Pullback.
- The SQLite evidence file is scoped by practice account, verified trading
  epoch and US trade date. It remains read-only to Tick children and can be
  reopened after a WSL session restart. On missing/invalid data the decision
  is WATCH or WAIT, never fabricated READY.

## Trial decision rules (not calibrated)

- Opening range coverage: all 30 complete original 1-minute bars from
  09:30 through 09:59 ET before the first 10:00 evaluation.
- Pullback: >=0.5% drop from genuine opening bar high to recent low,
  at least two successive increasing completed bar closes and non-decreasing
  lows; recent two-bar native volume >=80% of the preceding two bars.
- Momentum continuation: trial pullback-reclaim subtype (same reversal proof).
  A momentum-only straight-through breakout still needs a separate verified
  pivot/target model; do **not** silently force a momentum candidate into
  the pullback style or claim an unproven breakout.
- Breakout: completed bar holds a verified PREP pivot, nonzero native
  volume confirmation, and does not chase >0.8% above pivot; a verifiable
  target reference must exist or evidence is WAIT.
- Entry price: verified fresh KIS WS bid/ask; quoted spread <=0.3%,
  reference stop distance <=2.5%, reference reward/risk >=1.5.
  Order limit is capped at the resulting maximum allowed entry price.
- A *structural reference stop/target* is NOT a real stop order, exit
  target, or change to the existing frozen position management contract.
- At the actual native KIS BUY HTTP boundary, confirm digest, signal age
  <=120 seconds, fresh WS bid/ask <=5 seconds, price <=max_entry_price,
  and reject a price reversal >0.4% since the READY observation.
  No extra KIS REST call is made on the broker submission boundary.

### Owner isolation / exclusions

- Applies only to **US_STANDARD NEW_POSITION_BUY** selected as
  ENTRY_PULLBACK, ENTRY_MOMENTUM, or ENTRY_BREAKOUT.
- Does not change ADD_BUY lifecycle management, VCP's existing native
  pivot+volume proof, TQQQ Infinite's own strategy owner, protected SELL
  priority, account order Claims, or Stage4 reconciliation.
- A failed/missing observer NEVER suppresses protective SELL.
- Existing Final30 PREP and score contract remain unchanged.

## Operational settings

### Safe read-only trial (only after verification)

```bash
export KIS_ENV=practice
export US_ENTRY_OPPORTUNITY_MODE=SHADOW
export US_OPENING_OBSERVER_ENABLED=1
export US_OPENING_OBSERVER_BAR_FETCH_ENABLED=1
export US_OPENING_OBSERVER_INTERVAL_SEC=10
export US_WS_COLLECTOR_ENABLED=1
```

Session AM starts the observer before its Tick loop and terminates it in
`finally`. Its child process sets `DRY_RUN=1`,
`US_KIS_ORDER_ALLOWED=0`, `ALLOW_REAL_ORDER=0` independently of
the trading session. Note: `US_OPENING_OBSERVER_ENABLED=0` is the
default because KIS REST quota and WS subscription health have not been
validated in the user's actual WSL practice session.

### ENFORCE is not approved for production by this PR

```bash
# NOT FOR PRODUCTION: do this only for explicitly approved KIS practice testing
export US_ENTRY_OPPORTUNITY_MODE=ENFORCE
export US_ENTRY_OPPORTUNITY_APPROVED=1
export US_OPENING_OBSERVER_ENABLED=1
export US_WS_COLLECTOR_ENABLED=1
```

If ENFORCE is requested but approval is absent or account isn't practice,
new US_STANDARD BUY is blocked rather than silently reverting to instant-buy.
If native minute bars/quote/bid/ask or the observer cannot be verified, a
candidate remains WAIT. A valid READY is STILL subject to the unchanged
Risk Gate, account reservation, KIS order rules and broker fill result.

## Offline replay

```bash
python scripts/us_entry_opportunity_replay.py \
  reports/us_daily/<trade-date>/price_aware_replay_input.json \
  --output reports/us_daily/<trade-date>/price_aware_replay_output.json
```

Requires actual native KIS minute bars and timestamped broker quote
snapshots; an incomplete original log cannot be treated as a continuous
1-minute history. The replay reports WATCH/WAIT/READY, first READY price,
and forward 60-minute observed MFE/MAE only where genuine future bars exist.
It NEVER fabricates executed orders or net PnL.

## Merge acceptance and outstanding verification

- Run full `tests/us` and PostgreSQL US harness on PR #284's latest SHA.
- Confirm 09:30 observer started **before** any time-only BUY block, with 30
  original minute bars per evaluated symbol.
- Validate REST quota/WS occupancy under concurrent protective SELL and
  20+ Stage4 READY cycles; no 180-second routing budget regression.
- Replay Oct 28 US and multiple volatile sessions using original KIS bars,
  owner identities, same-time baseline fills, missed rallies, slippage,
  MAE/MFE and realized net PnL where available.
- Validate KIS practice native BUY pre-HTTP rejection and ACK/fill/reconcile,
  including collector disconnect/reconnect, position add, KIS timeout,
  DST shift, stale bar, bad trade epoch, and full observer restart.
- Enabling ENFORCE after tests is a **POLICY REVIEW decision**: numbers
  0.5%, 0.8%, 0.3%, 2.5%, 1.5 and 0.4% are experiments, not optimized facts.
- #284 was built on #279; merge #279 first and make #284 a clean successor
  (the GitHub PR may temporarily target dual-agent to run its full CI).

## Reference

KIS official example: `koreainvestment/open-trading-api`,
`examples_llm/overseas_stock/inquire_time_itemchartprice/`.
