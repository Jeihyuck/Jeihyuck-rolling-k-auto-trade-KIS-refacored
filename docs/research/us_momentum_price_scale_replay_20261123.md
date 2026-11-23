# US Momentum unit-invariance replay — 2026-11-23

## Data and reproducible query

Read-only Supabase `price_daily` (`market='US'`), latest 252 positive completed daily closes per ticker with `date <= '2026-11-19'`. **213** symbols had at least 63 observations. Two symbols did not have their latest bar on 11/19. This is a price-path cohort replay, **not** a historical Final30 selection replay, and adjustment/split provenance must be verified separately before interpreting performance.

```sql
WITH x AS (
 SELECT code,date,close,
        ROW_NUMBER() OVER(PARTITION BY code ORDER BY date DESC) rn
 FROM price_daily
 WHERE market='US' AND date <= DATE '2026-11-19' AND close > 0
)
SELECT code, COUNT(*) samples, MAX(date)::text latest,
       json_agg(close ORDER BY date) closes
FROM x WHERE rn <= 252
GROUP BY code HAVING COUNT(*) >= 63 ORDER BY code;
```

Computed baseline with the original `USMomentumStrategy.score()`: 20/60-session eligibility >=5%, price USD standard deviation of 20 closes, original 40% proximity / 60% volatility-adjusted score. Comparison uses identical eligible returns and weights but 20 daily returns' population standard deviation times sqrt(20). Both round to 3 decimals.

## Results, **not** backtest alpha

- Eligible: **17 / 213**, eligibility changed: **0**.
- Average baseline standalone score: **0.368**.
- Average corrected standalone score: **0.678** (large upward normalization shift; **do not silently re-calibrate strategy weights**).
- New `risk_adj >= 2` saturation: **0/17**.
- Standalone-score-only top 15 overlap: **14 of 15**. This is a hypothetical score ranking, not the executable production arbiter.
- Example: COST 0.392→0.943, WEC 0.407→0.904, GILD 0.392→0.732.
- Original/changed top15 strings were checked for diagnostics; no actual BUY, lot size, or historical immutable contract changed.

## Actual consumer trace

1. `candidate_pool_builder._compute_us_explicit_signal_proofs` calls `USMomentumStrategy.score` to populate `momentum_pass` (non-None) and `standalone_momentum_score`.
2. `four_family_arbitration.eligible_family_scores` checks that standalone is **present**, but obtains momentum ranking strength from a separate `row['momentum_score']` field. This PR does **not** replace or reweight it.
3. `pb1/us_entry_engine.py` checks `momentum_pass is True` and standalone score nonnegative as a signal-provenance gate; no new executable trade permission is granted by raising the standalone numeric score.
4. `USMomentumStrategy.generate_intent` does size based on its score if invoked directly. That separate direct path is not claimed to have been replayed with full KIS capital and orders.

## Decision

**IMPLEMENTATION BUG numeric correction, no claim of policy alpha.** Leave the existing 20/60 day eligibility and frozen BUY contracts unchanged. Unit tests explicitly verify that the numeric change does not overwrite the separate production family-arbiter score. A fully point-in-time Final30/order-size replay is still necessary for any **new** ranking/weight-sizing policy; this diagnostic is not that authorization.
