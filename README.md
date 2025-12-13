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

## How to run
```
python -m trader.trader
```
This initializes the portfolio manager, runs KOSPI rebalance if due, then executes the existing KOSDAQ intraday loop without interrupting either engine on errors.
