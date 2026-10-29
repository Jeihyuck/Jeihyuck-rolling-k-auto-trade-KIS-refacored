# KR-E: Price-aware opening-window entry, all standard PB1 styles
Status: **DRAFT / SHADOW DEFAULT / NO AUTOMATIC OPERATIONAL ACTIVATION**

## What actually changes in code
- Preserves real KIS \`FHKST03010200\` minute OHLCV + source business date when present, never fabricates missing opening-high/low.
- Adds independent \`scripts.kr_opening_observer\` (read-only, practice only) to load the day's Final30 and save account/day/symbol-scoped completed-minute proofs. This observer is **not run from BUY/SELL**, no synchronous extra HTTP before KIS order, and is **not yet scheduled**.
- Adds \`trader.kr.opening_bar_contract\` pure verification: >=25 genuine complete first-30-minute bars, 09:00/09:29 coverage, no cross-day/future/incomplete candles, contiguous recent minutes, freshness <=180s, confirmed native weighted-price provenance when MOMENTUM requires VWAP.
- Distinct policy hypotheses:
  - MOMENTUM (including explicitly selected MOMENTUM_SWING): real 1m rising closes and higher lows, same-day volume change, verified native VWAP, opening-range breakout **or** pullback/reclaim with room-to-stop quality.
  - PULLBACK: support/rebound with bar lows/higher lows and risk/room; no mandatory MOMENTUM VWAP.
  - BREAKOUT: original owner-specific \`trigger_ok\` + true pivot + opening-range price + minute-volume expansion. No OR bypass.
  - All other owners/styles (KR_INFINITE/VCP/CORE/US): untouched.
- Adds immutable evidence payload digest/TTL and explicit maximum entry price at the standard PB1 **pre-submit** gate; no hot-path KIS data fetch and no SELL-path change.
- Defaults to \`KR_PB1_ENTRY_TIMING_MODE=SHADOW\`. Enforcement *requires both* \`KR_PB1_ENTRY_TIMING_MODE=ENFORCE\` and \`KR_PB1_ENTRY_TIMING_APPROVED=1\` **in the practice environment**. No operational environment changes in this PR.
- Existing risk, KIS idempotency, reservation/claims, DAY/SWING frozen contract, 180s hard deadline, submit architecture untouched.

## Important limitations — do not declare operational success
1. KIS may omit minute \`stck_oprc/stck_hgpr/stck_lwpr/stck_bsop_date\` or verified native VWAP. This must produce UNKNOWN/WAIT. Never fill the gaps with latest_price-only snapshots.
2. \`observed_volume_ratio\` is recent minute volume / mean opening minute volume **within today**, NOT historical time-adjusted RVOL. Threshold 1.2/1.5 and risk/room 1.5 are *research hypotheses*, not proven optimal.
3. All standard PB1 Final30 codes require independent observer operation and durable same-day proofs. The observer is not scheduled/automatically started by this PR; without fresh proof, practice ENFORCE blocks that style's new BUY. Do not enable ENFORCE until scheduling and KIS service capability have been explicitly validated.
4. No measured backtest for 2026-10-29 Mico real 1m bars is provided; the uploaded archive had irregular quote snapshots, not full minute OHLCV. Synthetic tests are structural regression fixtures only, not profit evidence.
5. Mico 09:31 observed 26,050 is not automatically a quality entry; original momentum trigger, volume curve, opening-bar risk/room must be proved. Cannot assert what its best eventual entry time would have been.
6. Do not enable any optional entry gate before P0 #287/#288/#289 reconciliation repair is proven; a profitable signal is not meaningful if SELL or fill reconciliation is broken.
7. Independent observer loop may consume KIS read quota; must benchmark actual per-symbol latency under 30-symbol load and ensure no new slowdown to protected SELL or 180s order deadline. Strict account/epoch/file permissions needed for multi-instance WSL access.

## Acceptance and test gates
- Unit/CI for missing/future/stale data, true OHLC requirements, 09:30 WAIT, Mico adverse entry price, pullback/breakout independent logic, KIS-native VWAP date freshness, no cross-market owner leakage, pre-submit explicit approval and capped price, independent observer no order permissions.
- Walk-forward replay point-in-time KIS original 1m/5m data vs old rules, candidate-level false-negative/profitability, 15/30/60min MAE/MFE, missed winners, slip+tax+fees, drawdown. Shadow must be enabled and observed at least one simulated market session.
- P50/P95/P99 running costs of collector and existing trader. Ensure native SELL precedence unchanged, no KIS BUY if price proof expired.
- Explicit operator approval before practice ENFORCE or changing scheduler. Never auto-merge.

## Relationship to other PRs
- #283 — sampled quote shadow only; do not mistake irregular quote samples for native minute OHLCV. Both PRs use the same conceptual state separation but different evidence sources.
- #286 — practice conditional TREND_SWING default ON (separate future holding policy, not entry timing).
- #287 — physically TEXT cumulative fill fix; #288 — exact owner; #289 — exact frozen DAY close recovery.
