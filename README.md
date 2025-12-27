# Jeihyuck-rolling-k-auto-trade-KIS-refacored

## Portfolio split architecture
This refactor promotes KOSPI and KOSDAQ trading into parallel engines under a shared portfolio manager while preserving the existing KOSDAQ intraday behavior.

```
portfolio/
  base_engine.py
  kospi_core_engine.py
  kosdaq_alpha_engine.py
  portfolio_manager.py
strategy/
  kospi/{universe.py, rebalance.py, signals.py}
  kosdaq/{universe.py, rolling_entry.py, pullback.py}
trader/
  trader.py (entrypoint)
  state_manager.py
  legacy_kosdaq_runner.py (previous KOSDAQ loop kept intact)
```

## Engine responsibilities
- **KOSPI core engine**: KOSPI market-cap Top-N universe, equal-weight targets, periodic rebalance with market orders and KIS quotes.
- **KOSDAQ alpha engine**: delegates to the legacy rolling-K/VWAP/pullback loop unchanged, using its original state file for backward compatibility.
- **Capital split**: `PortfolioManager` divides `DAILY_CAPITAL` (or supplied total) into KOSPI and KOSDAQ ratios (default 60/40) and runs each engine independently.
- **Performance**: portfolio-level PnL snapshots combine KIS cash/positions with engine allocation ratios for unified reporting without coupling the two engines.
  - Engine-level PnL is an attribution estimate based on capital split ratios because positions are pooled at the account level.

## How to run
```
python -m trader.trader
```
This initializes the portfolio manager, runs KOSPI rebalance if due, then executes the existing KOSDAQ intraday loop without interrupting either engine on errors. The KOSDAQ loop is blocking, so the entrypoint runs a single orchestrated cycle via `run_once()` rather than a repeating scheduler.

Workflow는 bot-state 브랜치에 bot_state/state.json을 커밋하여 런 간 상태를 유지합니다.

## Strategy intent mode (single-account multi-strategy)
- A new `StrategyManager` runs before engine loops and emits **order intents only** into `trader/state/strategy_intents.jsonl` with a cursor in `trader/state/strategy_intents_state.json`.
- All five strategies (`breakout`~`volatility`) are present but **disabled by default**: `ENABLED_STRATEGIES=""` means no strategies run, and missing weights are treated as zero even when listed.
- Enable a subset for testing, e.g. `ENABLED_STRATEGIES="momentum"` with optional weights `STRATEGY_WEIGHTS="momentum=0.10"`. Keep `STRATEGY_MODE=INTENT_ONLY` and `STRATEGY_DRY_RUN=true` (defaults) to avoid any KIS orders.
- PortfolioManager order: strategies → KOSPI → KOSDAQ. During isolated testing use `DISABLE_KOSPI_ENGINE=true` or `DISABLE_KOSDAQ_LOOP=true` to skip respective engines.
- State sync scripts in `scripts/state_pull_plain.sh` and `scripts/state_push_plain.sh` now copy the intent log/cursor alongside `trader/state/state.json` into the `bot-state` branch.

## CI and live-trading safeguards
- CI (pull_request) runs set `DISABLE_LIVE_TRADING=true` so all KIS API calls are blocked and only static checks execute.
- The live trading workflow is restricted to the `main` branch and triggers only via schedule or manual dispatch with the branch guard enabled.
