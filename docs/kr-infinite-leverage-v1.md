# KR Infinite Leverage v1

## Goal

Build a Korean-market leveraged ETF accumulation/harvest strategy inspired by the operating idea of repeated TQQQ accumulation, without copying a pure martingale or assuming a leveraged ETF is safe to hold indefinitely.

The implementation is isolated from PB1 stock entry/exit state. It reuses the existing KIS client, order ledger, run ledger, reconciliation, tick-size logic and DB migration framework, while keeping strategy lifecycle state in `kr_infinite_campaigns`.

## Default policy

| Parameter | Default | Meaning |
| --- | ---: | --- |
| Total tranches | 24 | Campaign capital is divided into 24 equal budgets |
| Initial entry | 2 tranches | Start with 8.33% of campaign budget |
| Maximum deployed | 18 tranches | Never deploy more than 75%; keep 25% reserve |
| Ladder step | -2.5% | Add one equal tranche after a 2.5% decline from the last confirmed buy fill |
| Max buys/day | 1 | Prevent intraday cascade buying |
| Normal take-profit | +4.5% | Exit the whole cycle above confirmed average cost |
| Weak-regime take-profit | +3.0% | Harvest earlier when the benchmark regime is weak but not defensive |
| Defense threshold | regime <= -1.0 | Stop new/additional buys |
| Hard campaign stop | -18% vs average cost | Liquidate rather than allow an unlimited averaging-down campaign |
| Campaign age | 90 days | At/after 90 days, exit at breakeven or better |
| Premium guard | 1.5% | Do not buy if externally supplied ETF premium/discount exceeds limit |

The defaults are initial simulation parameters, not claims of optimized performance.

## Regime score

The runner computes a simple benchmark score from daily candles:

- benchmark last price vs 20-day moving average: +/-0.75
- 20-day moving average vs 60-day moving average: +/-0.75
- 20-day return sign: +/-0.50

A new campaign requires score >= 0. Existing campaigns can continue laddering in mildly weak conditions, but score <= -1.0 stops averaging down. In mildly weak conditions, the take-profit target is reduced to +3.0%.

## Fill authority

Broker acceptance is **not** treated as a fill.

1. The runner first calls the existing KIS reconciliation path.
2. Campaign quantity, average price, last add price and deployed tranche count are rebuilt from reconciled fills joined back to this strategy's original orders via KIS order number.
3. KIS `rt_cd=0` changes the order only to submitted/acked state.
4. A newly accepted initial order creates an active campaign, but the next run waits for an actual reconciled fill before allowing the next ladder step.

This is required because tranche count and average cost must be based on actual broker fills, not order intentions.

## Configuration

Required:

```bash
KR_INF_ENABLED=1
KR_INF_SYMBOL=<six-digit leveraged ETF code>
KR_INF_BENCHMARK=<six-digit unlevered benchmark/ETF code>
KR_INF_CAMPAIGN_BUDGET_KRW=<maximum campaign capital>
```

Safe initial rollout:

```bash
STRATEGY_ENV=practice
KR_INF_DRY_RUN=1
```

Optional tuning:

```bash
KR_INF_TRANCHES=24
KR_INF_INITIAL_TRANCHES=2
KR_INF_MAX_DEPLOYED_TRANCHES=18
KR_INF_ADD_STEP_PCT=0.025
KR_INF_TAKE_PROFIT_PCT=0.045
KR_INF_TAKE_PROFIT_REDUCED_PCT=0.030
KR_INF_MAX_DRAWDOWN_PCT=0.18
KR_INF_MAX_CAMPAIGN_DAYS=90
KR_INF_MAX_BUYS_PER_DAY=1
KR_INF_MIN_REGIME_SCORE=0.0
KR_INF_DEFENSE_REGIME_SCORE=-1.0
KR_INF_MAX_MARKET_PREMIUM_PCT=0.015
KR_INF_NAV_PREMIUM_PCT=0.0
```

Run once:

```bash
python -m trader.kr_infinite_runner --env practice --dry-run
```

## Important v1 limitation

`KR_INF_NAV_PREMIUM_PCT` is currently an injected value. The runner does not yet calculate a trustworthy live iNAV premium from a dedicated ETF NAV feed. Therefore real-money enablement should remain blocked operationally until live iNAV/premium sourcing and stale-data validation are added.

Also, an accepted but unfilled initial order intentionally puts the campaign into `await_initial_fill` rather than submitting a second order. Cancel/replace automation for stale orders is a later hardening item.

## Rollout gates

1. Pure strategy unit tests pass.
2. Practice + dry-run logs are inspected for at least several sessions.
3. Practice with real KIS order submission validates reconciliation, partial fills and cycle closure.
4. Backtest/Monte Carlo parameter sweep validates drawdown and cash exhaustion behavior across crisis regimes.
5. Live iNAV/premium feed and stale-order cancel/replace are implemented.
6. Only then consider real-money enablement with a small campaign budget.
