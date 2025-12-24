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
  strategy_manager.py (multi-strategy orchestrator)
  strategies/ (strategy interface & concrete implementations)
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
This now initializes the strategy-aware engine, reconciles state with the KIS balance + ledger, and runs a single StrategyManager cycle (entries/exits) for all enabled strategies in parallel. The legacy KOSDAQ loop remains available via the portfolio engines but the default entrypoint focuses on the multi-strategy Rolling-K pipeline.

Workflow는 bot-state 브랜치에 bot_state/state.json을 커밋하여 런 간 상태를 유지합니다.

## Multi-strategy Rolling-K
- `trader/strategies/`: BaseStrategy plus breakout, pullback, momentum, mean-reversion, and volatility variants, each exposing common enter/exit hooks.
- `trader/strategy_manager.py`: evaluates all enabled strategies, allocates capital by configurable weights, and routes buy/sell decisions through `KisAPI` while tagging fills with `strategy_id`.
- Configuration: per-strategy risk knobs are sourced from environment variables (see `trader/config.py` or `settings.py`). Key variables include `BREAKOUT_PROFIT_TARGET_PCT`, `BREAKOUT_STOP_LOSS_PCT`, `PULLBACK_REVERSAL_BUFFER`, `MOMENTUM_MIN_MOMENTUM_PCT`, `MEANREV_BAND_WIDTH_PCT`, `VOLATILITY_THRESHOLD_PCT`, `STRATEGY_WEIGHTS`, and `STRATEGY_WATCHLIST`.
- Logging: entry/exit, stop-loss/take-profit decisions, and reconciliation steps emit INFO logs with code/strategy context for traceability.

## Ledger and reconciliation
- Every confirmed order is appended to `fills/ledger.jsonl` with `{timestamp, code, strategy_id, side, qty, price, meta}` and flushed to disk.
- On startup, the ledger is parsed to recover preferred `strategy_id` mappings for existing holdings before reconciling with the broker’s balance. Holdings absent from the latest balance snapshot are removed from local runtime state.
- Existing CSV fills under `fills/` are preserved; the JSONL ledger is additive and used for state recovery.

## KIS parameter updates
- `trader/kis_wrapper.py` will optionally load TR ID overrides from the latest KIS Excel spec via `KIS_PARAM_EXCEL_PATH` (defaulting to `한국투자증권_오픈API_전체문서_20250717_030000.xlsx` when present). Missing files fall back to the environment variables baked into `TR_MAP`.

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
