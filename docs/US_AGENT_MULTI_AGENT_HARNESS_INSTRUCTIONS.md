# US Agent Multi-Agent + Harness Engineering Build Instructions

## 0. Branch and Operating Principle

Base branch: `nullim`

Development branch: `us-agent`

Goal: build a US stock paper-trading research and execution agent system on top of the existing rolling-k repository without breaking the existing Korean PB1/nullim trading flows.

The system must not promise guaranteed highest return. The engineering objective is to continuously search for and validate strategies that maximize risk-adjusted, benchmark-relative paper returns in the US market, then promote only validated strategies through strict harness gates.

Core operating principle:

```text
Research Agent may generate ideas.
Backtest Agent must verify ideas.
Shadow Agent must observe ideas without orders.
Risk Gate must approve any paper execution.
Paper Execution Agent may place only KIS US paper orders.
No agent may directly alter live/real trading behavior.
```

The `nullim` branch remains the domestic PB1 operating branch. The `us-agent` branch is the isolated US paper trading lab. Scheduled GitHub Actions can later live in `nullim` as launcher workflows that checkout `us-agent` or a future `us-stable` branch, but the first implementation must be developed and tested inside `us-agent`.

---

## 1. Non-Negotiable Safety Rules

1. Do not modify existing domestic workflows unless explicitly required by a separate PR.
   - Do not change `.github/workflows/trade-am.yml`.
   - Do not change `.github/workflows/trade-afternoon.yml`.
   - Do not change `.github/workflows/trade-close.yml`.
   - Do not change domestic PB1 entry/exit behavior.

2. Do not mix US order logic into the existing domestic KIS wrapper as the first implementation.
   - Avoid large edits to `trader/kis_wrapper.py`.
   - Add US broker code under `trader/brokers/kis_us.py`.

3. All US order paths must require:
   - `MARKET=US`
   - `KIS_ENV=practice`
   - `US_PAPER_ONLY=1`
   - `US_LIVE_TRADING_ENABLED=0`
   - `US_ALLOW_PREMARKET=0` for first release
   - `US_ALLOW_AFTERHOURS=0` for first release

4. Research, backtest, and shadow workflows must not have access to order secrets.

5. Paper execution workflows may use US paper secrets only through a dedicated GitHub Environment such as `us-paper`.

6. US tickers must not be zero-filled.
   - `AAPL` must remain `AAPL`.
   - Never apply domestic `zfill(6)` logic to US symbols.

7. US final candidates must not be stored in the existing PB1 watchlist path that assumes Korean stock codes.
   - Do not use `pb1_watchlist` for US final30.
   - Use a new `strategy_candidates` table.

8. No live US order path is allowed in this phase.
   - Any env value suggesting real/live must hard-fail.

---

## 2. Target Architecture

```text
us-agent branch
├── trader/brokers/kis_us.py
├── trader/brokers/kis_us_models.py
├── trader/markets/us/market_clock.py
├── trader/markets/us/calendar.py
├── trader/markets/us/universe.py
├── trader/markets/us/symbols.py
├── trader/markets/us/fx.py
├── trader/strategies/us/ai_infra_momentum.py
├── trader/strategies/us/qqq_regime_switch.py
├── trader/strategies/us/pullback_leader.py
├── scripts/agents/us_*.py
├── harness/us/*.yaml
├── research/us/*
├── reports/us/*
├── tests/test_us_*.py
└── .github/workflows/us-*.yml
```

The system has three layers:

```text
Layer 1: Harness and Safety Layer
- deterministic tests
- market clock checks
- KIS paper capability probe
- order schema contracts
- no-live-order gates

Layer 2: Research and Discovery Layer
- universe builder
- stock discovery
- alpha idea generation
- backtest
- walk-forward validation
- shadow trading

Layer 3: Paper Execution Layer
- risk gate
- paper order intent
- KIS US paper adapter
- fills/positions reconciliation
- PNL and log reporting
```

---

## 3. Multi-Agent Design

Implement agents first as deterministic Python scripts plus YAML policies. LLM usage is optional and must be limited to report generation, log summarization, and strategy idea drafting. LLM output must never directly place orders.

### 3.1 US Capability Probe Agent

File:

```text
scripts/agents/us_capability_probe_agent.py
```

Purpose:

```text
Verify that the current account, secrets, KIS environment, and selected US symbols can be used in paper mode before any strategy or order logic runs.
```

Required checks:

```text
- MARKET == US
- KIS_ENV == practice
- US_PAPER_ONLY == 1
- US_LIVE_TRADING_ENABLED != 1
- US_CANO / US_ACNT_PRDT_CD are available through environment mapping
- KIS base URL is paper/practice endpoint
- representative symbols are quoteable: AAPL, MSFT, NVDA, QQQ, SPY
- paper order contract is available for at least a safe smoke-test symbol
- no actual order is sent unless US_PAPER_SMOKE_ORDER=1 and explicit manual confirmation is present
```

Output:

```text
reports/us/capability_probe/latest.json
reports/us/capability_probe/latest.md
```

Failure must block all downstream US paper execution.

---

### 3.2 US Market Clock Agent

Files:

```text
trader/markets/us/market_clock.py
trader/markets/us/calendar.py
scripts/agents/us_market_clock_agent.py
```

Purpose:

```text
Determine whether US paper trading is allowed at the current time.
```

Required behavior:

```text
- Detect US regular session.
- Block premarket and after-hours in first release.
- Handle US daylight saving time.
- Distinguish KST date from US trade date.
- Block weekends and known US holidays.
- Support half-day close checks.
```

CLI examples:

```bash
python -m trader.markets.us.market_clock --require-regular-session
python -m trader.markets.us.market_clock --print-status-json
```

---

### 3.3 KIS US Adapter Agent

Files:

```text
trader/brokers/kis_us.py
trader/brokers/kis_us_models.py
scripts/agents/us_kis_adapter_contract_agent.py
```

Purpose:

```text
Implement and validate US paper quote, balance, order, cancel, and fill parsing without changing domestic order behavior.
```

Initial scope:

```text
- quote lookup
- USD cash/orderable amount lookup
- US position balance lookup
- limit buy paper order
- limit sell paper order
- order accepted parser
- order rejected parser
- fills parser
```

Hard rules:

```text
- First release allows LIMIT only.
- Market orders are forbidden.
- Premarket/after-hours orders are forbidden.
- Real endpoint is forbidden.
- Missing exchange code must fail.
- Missing currency must fail.
- Missing client_order_key must fail.
```

---

### 3.4 US Universe Builder Agent

Files:

```text
trader/markets/us/universe.py
trader/markets/us/symbols.py
scripts/agents/us_universe_builder_agent.py
```

Purpose:

```text
Create a controlled, high-quality US investable universe that can be expanded gradually.
```

Universe phases:

```text
Phase 0: Capability symbols
- AAPL, MSFT, NVDA, QQQ, SPY

Phase 1: Curated AI Infra Universe, 30-50 symbols
- AI semiconductor
- optical interconnect
- data center power
- AI server/network
- cloud infrastructure

Phase 2: Liquid US Growth Universe, 200-500 symbols
- NASDAQ 100
- S&P 500 liquid growth names
- high dollar-volume technology and industrial AI infrastructure names

Phase 3: Broad Liquid US Universe
- only after KIS data and order support are proven stable
```

Initial curated seed symbols:

```text
NVDA, AMD, AVGO, MRVL, MU, ARM, TSM, ASML, AMAT, LRCX, KLAC,
CRDO, CIEN, COHR, LITE, AAOI, ANET, VRT, BE, DELL, SMCI,
MSFT, GOOGL, AMZN, META, PLTR, QQQ, QQQM, SPY, SMH, SOXX, XLK, IYW, SHY, SGOV, BIL
```

Output tables:

```text
symbols_master
universe_runs
universe_members
strategy_candidates
```

---

### 3.5 US Stock Discovery Agent

File:

```text
scripts/agents/us_stock_discovery_agent.py
```

Purpose:

```text
Rank US universe symbols and produce candidate lists for each strategy.
```

Initial score formula:

```text
US Alpha Score =
  25% 3-month relative strength
+ 20% 1-month relative strength
+ 15% 52-week high proximity
+ 15% 20-day dollar volume expansion
+ 10% volatility contraction
+ 10% outperformance vs QQQ
+  5% event/liquidity/slippage risk adjustment
```

Outputs:

```text
strategy_candidates:
- us_ai_infra_final30
- us_momentum_final20
- us_pullback_final20
- us_etf_regime_final

reports/us/stock_discovery/latest.md
reports/us/stock_discovery/latest.json
```

---

### 3.6 US Alpha Research Agent

File:

```text
scripts/agents/us_alpha_research_agent.py
```

Purpose:

```text
Generate strategy hypotheses and experiment definitions. This agent must not trade.
```

Initial strategy families:

```text
1. US AI Infra Momentum
2. QQQ Regime Switch
3. 52-Week High Breakout
4. 20MA/50MA Pullback Leader
5. Volatility Contraction Pattern
6. Gap-and-Hold Strategy
7. Earnings Drift Watchlist, shadow only in first release
```

Output:

```text
research/us/ideas/alpha_ideas.yaml
research/us/experiments/experiment_registry.yaml
```

---

### 3.7 US Backtest and Walk-Forward Agent

Files:

```text
scripts/agents/us_backtest_agent.py
scripts/agents/us_walkforward_agent.py
```

Purpose:

```text
Validate strategies before they are allowed into shadow or paper execution.
```

Metrics required:

```text
- traditional trade return: (sell_price - buy_price) / buy_price * 100
- CAGR
- MDD
- win rate
- profit factor
- average win/loss
- turnover
- slippage-adjusted return
- QQQ benchmark excess return
- SPY benchmark excess return
- monthly hit ratio
- worst 10 trades
- regime split
```

No strategy may be promoted unless it passes no-lookahead and benchmark comparison tests.

---

### 3.8 US Shadow Trading Agent

File:

```text
scripts/agents/us_shadow_trading_agent.py
```

Purpose:

```text
Record what the US strategy would have bought or sold without calling KIS order endpoints.
```

Output:

```text
shadow_orders
reports/us/shadow/latest.md
```

Required horizons:

```text
1D, 3D, 5D, 10D
```

Shadow trading must be the required bridge between backtest and paper execution.

---

### 3.9 US Risk Gate Agent

File:

```text
scripts/agents/us_risk_gate_agent.py
```

Purpose:

```text
Block unsafe paper trades.
```

Initial hard risk limits:

```text
US_PAPER_MAX_CAPITAL_USD=10000
US_MAX_POSITIONS=10
US_MAX_POSITION_PCT=0.15
US_MAX_SECTOR_PCT=0.40
US_DAILY_LOSS_LIMIT_PCT=2.0
US_TOTAL_DRAWDOWN_STOP_PCT=8.0
US_ALLOW_PREMARKET=0
US_ALLOW_AFTERHOURS=0
US_ALLOW_FRACTIONAL=0
US_ORDER_TYPE_ALLOWED=LIMIT
```

Block conditions:

```text
- outside US regular session
- not paper environment
- real/live env detected
- USD cash unavailable
- unsupported ticker
- unsupported exchange
- missing price
- missing limit price
- market order attempted
- duplicate client_order_key
- existing pending order for same symbol
- sector concentration exceeded
- position size exceeded
- research/shadow workflow attempting order
```

---

### 3.10 US Paper Execution Agent

Files:

```text
scripts/agents/us_paper_execution_agent.py
trader/strategies/us/run_open.py
trader/strategies/us/run_close.py
```

Purpose:

```text
Submit KIS US paper orders only after all gates pass.
```

Flow:

```text
load candidates
market clock check
capability check
risk gate check
calculate limit order
write order intent
submit KIS US paper order
parse response
update order status
reconcile fills and positions
write ledger event
write PNL report
```

---

### 3.11 US Log Analyst Agent

File:

```text
scripts/agents/us_log_analyst_agent.py
```

Purpose:

```text
Analyze workflow logs and classify failures into actionable categories.
```

Failure categories:

```text
US_CAPABILITY_FAIL
US_MARKET_CLOSED
US_KIS_AUTH_FAIL
US_QUOTE_FAIL
US_ORDER_REJECTED
US_SYMBOL_UNSUPPORTED
US_USD_CASH_UNAVAILABLE
US_RATE_LIMIT
US_DB_TIMEOUT
US_NO_CANDIDATES
US_RISK_GATE_BLOCKED
US_HARNESS_CONTRACT_FAIL
```

Output:

```text
reports/us/incidents/latest.md
reports/us/incidents/latest.json
```

---

### 3.12 US Release Gate Agent

File:

```text
scripts/agents/us_release_gate_agent.py
```

Purpose:

```text
Decide whether a strategy can move from research to shadow, or from shadow to paper.
```

Promotion gates:

```text
Research -> Shadow:
- no-lookahead pass
- benchmark comparison pass
- minimum trade count pass
- slippage-adjusted positive result
- MDD within threshold

Shadow -> Paper:
- minimum 10 trading days shadow data
- QQQ-relative performance positive
- risk gate pass
- no order safety violation
- CEO manual approval flag or protected environment approval
```

No automatic promotion to real trading is allowed.

---

## 4. Database and Schema Instructions

Use the existing Postgres DB through `PBCORE_DB_URL`, but add US-specific namespaces and tables.

Do not create a separate DB in first release.

Add migration:

```text
migrations/00xx_us_agent_schema.sql
```

Required tables:

```sql
CREATE TABLE IF NOT EXISTS symbols_master (
  market TEXT NOT NULL,
  symbol TEXT NOT NULL,
  name TEXT,
  exchange TEXT,
  currency TEXT NOT NULL DEFAULT 'USD',
  sector TEXT,
  industry TEXT,
  theme TEXT,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  kis_paper_quote_ok BOOLEAN,
  kis_paper_order_ok BOOLEAN,
  meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (market, symbol)
);

CREATE TABLE IF NOT EXISTS strategy_candidates (
  env TEXT NOT NULL,
  market TEXT NOT NULL,
  strategy TEXT NOT NULL,
  as_of DATE NOT NULL,
  symbol TEXT NOT NULL,
  rank INTEGER NOT NULL,
  score DOUBLE PRECISION,
  exchange TEXT,
  currency TEXT DEFAULT 'USD',
  style TEXT,
  meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (env, market, strategy, as_of, symbol)
);

CREATE TABLE IF NOT EXISTS shadow_orders (
  shadow_order_id TEXT PRIMARY KEY,
  env TEXT NOT NULL,
  market TEXT NOT NULL,
  strategy TEXT NOT NULL,
  as_of DATE NOT NULL,
  symbol TEXT NOT NULL,
  exchange TEXT,
  currency TEXT DEFAULT 'USD',
  side TEXT NOT NULL,
  qty INTEGER,
  signal_price DOUBLE PRECISION,
  signal_ts TIMESTAMPTZ,
  horizon_days INTEGER,
  status TEXT NOT NULL DEFAULT 'OPEN',
  meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fx_rates (
  base_currency TEXT NOT NULL,
  quote_currency TEXT NOT NULL,
  as_of DATE NOT NULL,
  rate DOUBLE PRECISION NOT NULL,
  source TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (base_currency, quote_currency, as_of)
);
```

Also add indexes where useful:

```sql
CREATE INDEX IF NOT EXISTS ix_strategy_candidates_lookup
ON strategy_candidates (env, market, strategy, as_of, rank);

CREATE INDEX IF NOT EXISTS ix_shadow_orders_lookup
ON shadow_orders (env, market, strategy, as_of, symbol);
```

Existing `price_daily` can be reused with `market='US'` and `code=<ticker>`.

Existing `orders`, `fills`, `positions`, and `ledger_events` already have `market`; all US writes must set `market='US'`.

Do not use existing PB1 watchlist for US candidates.

---

## 5. Harness Engineering Requirements

Add YAML contracts:

```text
harness/us/kis_us_order_contract.yaml
harness/us/us_market_clock_contract.yaml
harness/us/us_safety_policy.yaml
harness/us/us_universe_contract.yaml
harness/us/us_alpha_promotion_gate.yaml
harness/us/us_log_contract.yaml
```

Add tests:

```text
tests/test_us_symbol_not_zfilled.py
tests/test_us_market_clock_dst.py
tests/test_us_market_closed_no_order.py
tests/test_us_paper_only_guard.py
tests/test_us_no_live_order_path.py
tests/test_us_no_order_in_research.py
tests/test_us_kis_order_contract.py
tests/test_us_limit_order_only.py
tests/test_us_strategy_candidates_schema.py
tests/test_us_universe_contract.py
tests/test_us_backtest_uses_traditional_return.py
tests/test_us_no_lookahead_bias.py
tests/test_us_alpha_promotion_gate.py
tests/test_us_shadow_before_paper_required.py
```

All tests must pass before enabling scheduled paper execution.

---

## 6. GitHub Actions Workflows

Create these workflows inside `us-agent` first:

```text
.github/workflows/us-agent-ci.yml
.github/workflows/us-capability-probe.yml
.github/workflows/us-research-nightly.yml
.github/workflows/us-shadow-trading.yml
.github/workflows/us-paper-open.yml
.github/workflows/us-paper-close.yml
```

Initial workflow behavior:

```text
- us-agent-ci: runs on pull_request and workflow_dispatch.
- us-capability-probe: workflow_dispatch only at first.
- us-research-nightly: workflow_dispatch first, then scheduled after stable.
- us-shadow-trading: workflow_dispatch first, then scheduled after stable.
- us-paper-open / close: workflow_dispatch only until manual approval.
```

Only after stable operation should launcher workflows be copied to `nullim`.

The future `nullim` launcher should checkout `us-agent` or preferably `us-stable`:

```yaml
- name: Checkout US agent code
  uses: actions/checkout@v4
  with:
    ref: us-agent
    fetch-depth: 1
```

After the US system stabilizes, create a `us-stable` branch and make `nullim` launchers checkout `us-stable`, not the active development branch.

---

## 7. Daily Operating Flow

### 7.1 Before US Market Open

```text
US Capability Probe Agent
  -> US Market Clock Agent
  -> US Universe Builder Agent
  -> US Stock Discovery Agent
  -> US Risk Gate pre-check
  -> strategy_candidates/us_final30 generated
```

### 7.2 US Market Open

```text
nullim launcher or us-agent manual workflow
  -> checkout us-agent or us-stable
  -> market clock guard
  -> capability probe summary check
  -> load us_final30
  -> risk gate
  -> paper execution agent
  -> KIS US paper limit orders only
  -> order/fill/position updates
```

### 7.3 During Session

```text
Position monitor
  -> risk gate
  -> profit protection / stop rules
  -> paper sell orders only if allowed
```

### 7.4 After Close

```text
Reconcile positions
  -> update PNL
  -> log analyst
  -> incident report
  -> research/backtest/walk-forward
  -> leaderboard update
  -> release gate proposal
```

---

## 8. Initial US Strategies

### 8.1 US AI Infra Momentum

Universe:

```text
Semiconductors, optical interconnect, data center power, AI server/network, cloud infrastructure.
```

Entry features:

```text
- 3-month RS
- 1-month RS
- QQQ-relative strength
- 52-week high proximity
- 20-day dollar volume expansion
- volatility contraction
- price above 20MA/50MA
```

Exit rules:

```text
- initial stop around 7-8%
- profit protection after 8-10% gain
- giveback stop
- QQQ risk-off reduction
```

### 8.2 QQQ Regime Switch

Risk-on:

```text
QQQ, QQQM, SMH, SOXX, XLK
```

Risk-off:

```text
SHY, SGOV, BIL, cash
```

### 8.3 Pullback Leader

Entry:

```text
High RS leader, above 20MA/50MA, controlled pullback, volume contraction, rebound trigger.
```

---

## 9. Acceptance Criteria

The first milestone is complete only when all of the following are true:

```text
1. us-agent branch has isolated US agent code.
2. Existing domestic nullim workflows are untouched.
3. US agent CI passes.
4. US symbols are not zero-filled.
5. US market clock tests pass, including DST cases.
6. Research and shadow workflows cannot access order secrets.
7. Paper execution requires US_PAPER_ONLY=1 and KIS_ENV=practice.
8. US universe builder creates strategy_candidates without pb1_watchlist.
9. Backtest uses traditional return calculation.
10. Shadow trading is required before paper promotion.
11. No real/live US order path exists.
12. Log analyst produces a structured report after every run.
```

---

## 10. Implementation Order

Implement in this exact order:

```text
Step 1: Add US harness policy YAML files.
Step 2: Add US market clock and tests.
Step 3: Add US DB migration for symbols_master, strategy_candidates, shadow_orders, fx_rates.
Step 4: Add US universe builder with static seed symbols.
Step 5: Add KIS US capability probe with no order by default.
Step 6: Add KIS US adapter in mock mode.
Step 7: Add stock discovery and scoring.
Step 8: Add backtest and walk-forward harness.
Step 9: Add shadow trading.
Step 10: Add paper execution with strict risk gate.
Step 11: Add log analyst and incident report.
Step 12: Add release gate.
Step 13: Add GitHub Actions workflows, manual first.
Step 14: After manual validation, add nullim launcher workflow in a separate PR.
```

---

## 11. Final Warning

This system is a trading research and paper execution system. Do not implement self-modifying live trading. Do not allow agents to bypass tests, risk gates, or manual approval. The objective is continuous improvement through controlled experimentation, not uncontrolled autonomous trading.
