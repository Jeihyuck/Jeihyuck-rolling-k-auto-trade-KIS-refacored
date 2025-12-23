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

## CI and live-trading safeguards
- CI (pull_request) runs set `DISABLE_LIVE_TRADING=true` so all KIS API calls are blocked and only static checks execute.
- The live trading workflow is restricted to the `main` branch and triggers only via schedule or manual dispatch with the branch guard enabled.

## How legacy recovery works
The KOSDAQ loop reconciles broker holdings into the position state each cycle. If a holding is missing from the state, the bot:
1. Searches the recent trade logs for the latest BUY fill of the same code to recover the strategy ID and engine.
2. Falls back to a rebalance bucket (`REB_YYYYMMDD`) if the code is in today’s targets.
3. Otherwise assigns the holding to `MANUAL`.

This avoids legacy sid placeholders and ensures every holding has an explicit sid bucket.

## State schema
Position state is stored in `trader/state/state.json` and normalized on load. Each strategy entry is guaranteed to include:
```
{
  "code": "<6-digit code>",
  "sid": "<strategy or bucket id>",
  "engine": "<entry engine>",
  "qty": <int>,
  "avg_price": <float>,
  "entry_ts": "<ISO timestamp>",
  "high_watermark": <float>,
  "flags": { ... },
  "last_update_ts": "<ISO timestamp>"
}
```
