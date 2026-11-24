# KR practice PB1 US-aligned entry opportunity V2

## Evidence and scope
2026-11-24 KR: 105 post-open candidate evaluations, 77 policy WAIT, 28 data UNKNOWN; 0 submissions. 2026-11-23 US: 10 filled standard BUY, 9 pullback and 1 momentum. ICE/COST/ABBV native evidence had 0.778/0.734/0.663% local pullback; ED equal consecutive lows; COST and U local volume ratios 0.907/0.893. These are actual US DB observations, NOT replay of Korean native bars. No historical Korean fill or profit is asserted.

## Active policy
WSL AM/PM practice defaults to KR_PB1_ENTRY_OPPORTUNITY_POLICY=US_ALIGNED_V2 with existing ENFORCE and approval. Explicit LEGACY_V1 rolls back to previous enforced policy; OFF and approval=0 retain existing fail-closed semantics. Real accounts, KR_INFINITE (122630), existing ADD/CLOSE, SELL and US are not changed.
- Original daily setup, final selected family and immutable exit contract remain required.
- Native recent 4 consecutive completed bars, age <=180s; 0.5% depth over last up to 15 observed post-open bars; rising 3 closes; nondecreasing 3 lows; latest 2-minute volume >=80% prior 2-minute volume. No requirement for prior high breakout in reclaim.
- Preserve independent eight-minute native path if opening missing/conflicted. Local high is not a claimed opening high. Reclaim never substitutes for the owner breakout path. Existing breakout/VCP/Minervini owner rules are not redefined.
- Evaluate proposed LIMIT with 1% deviation, structural reward/risk >=1.5 and stop distance <=2.5%. Floor maximum to existing KR equity tick function; do not change order quantity or raise planned price. Structural stop is diagnostic, NOT a new position exit stop.
- Actual READY V2 pre-submit also checks authenticated fresh KR quote, <=15s receipt age, <=180s minute age, current last within 1%, spread <=0.3% if bid/ask available. Only existing bounded no-ask passive fallback is eligible without both quotes. Final limit must remain inside cap. Other existing risk/volume/order/claim/180s broker fences remain.
- Retain entry evidence in metadata and skip payload, not in the immutable exit policy hash.

## Release gates
1. Merge #395 reliability before #396 or merge #396 as integration only after all combined CI checks. No deployment or actual orders performed by this PR.
2. Run V2 fixture, legacy regressions, native observer proof, settlement and strategy isolation tests. No real KIS calls in CI.
3. Obtain original KIS KR minute history for 11/24 and prior dates for point-in-time old/new replay. Database WAIT facts alone cannot reconstruct missing candles. Compare opportunities, slippage, forward drawdown and realized returns without lookahead; no target trade count.
4. Check actual WSL code SHA, ENFORCE policy and observer freshness before market. Log first actual eligible READY -> KIS submission -> fill -> original SST contract -> reconciled position. Never force a trade for validation.
5. Compare error rates, WAIT reason distributions, trade opportunities, execution latency, realized and unrealized risk. Roll back V2 if safety/regression fails, not by bypassing timing.
6. Outstanding #395 operational issue (10:36 engine stop and log mail not delivered) remains separately unproven. This change does not claim to fix it.
