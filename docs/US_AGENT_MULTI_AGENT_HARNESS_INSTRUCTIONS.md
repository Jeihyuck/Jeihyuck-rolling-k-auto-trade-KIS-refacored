# US Agent Build Directive — Precise Multi-Agent + Harness Engineering Plan

Branch: `us-agent`

Base branch: `nullim`

Repository goal: add an isolated US stock paper-trading agent system to the existing rolling-k repo. The Korean PB1/nullim trading system must remain untouched and operational.

This directive is written for Codex/Copilot-style implementation. Follow the milestones in order. Do not jump to paper trading before the harness and shadow layers exist.

Important: this project must not claim guaranteed maximum returns. The engineering target is to build an autonomous research-and-validation loop that continuously searches for the best risk-adjusted, benchmark-relative US paper trading strategy, while preventing unsafe orders through deterministic harness gates.

---

## 0. Final System in One Sentence

Build a US paper-trading multi-agent pipeline where:

```text
US Universe Builder -> Stock Discovery -> Alpha Research -> Backtest/Walk-forward -> Shadow Trading -> Risk Gate -> KIS US Paper Execution -> Log Analysis -> Release Gate
```

Only the final Paper Execution Agent may call KIS US paper order APIs, and only after every safety harness passes.

---

## 1. Absolute Do-Not-Touch Rules

Do not modify these domestic production paths in this implementation:

```text
.github/workflows/trade-am.yml
.github/workflows/trade-afternoon.yml
.github/workflows/trade-close.yml
trader/pb1_engine.py
trader/pb1_runner.py
trader/trade_tick.py
```

Do not put US order logic into the domestic KIS wrapper as the first implementation:

```text
Do not add overseas stock order branches into trader/kis_wrapper.py.
Create trader/brokers/kis_us.py instead.
```

Do not store US final candidates in `pb1_watchlist`, because that path assumes Korean six-digit stock codes and may call `zfill(6)`.

Do not let any research/backtest/shadow workflow access order-capable secrets.

No real/live US trading path may exist in this milestone.

---

## 2. Branch Strategy

Development branch:

```text
us-agent
```

Later stable branch, not required in PR 1:

```text
us-stable
```

Later nullim launcher, not required in PR 1:

```text
nullim/.github/workflows/us-*.yml
```

First implement and test inside `us-agent`. After manual validation, create launcher workflows in `nullim` that checkout `us-agent` or preferably `us-stable`.

---

## 3. Agent Definition

In this project, an Agent is not an unconstrained LLM process. An Agent is a deterministic Python module with:

```text
1. explicit input files / DB tables
2. explicit output files / DB tables
3. explicit YAML policy contract
4. explicit tests
5. no hidden side effects
```

LLM usage is allowed only for:

```text
- summarizing logs
- drafting strategy hypotheses
- writing reports
- drafting PR/issue text
```

LLM output must never directly place an order, bypass a risk gate, or mutate the live/paper execution policy.

---

## 4. Directory Structure to Create

Create these directories if missing:

```text
agents/
scripts/agents/
trader/brokers/
trader/markets/us/
trader/strategies/us/
harness/us/
research/us/ideas/
research/us/experiments/
research/us/results/
reports/us/capability_probe/
reports/us/stock_discovery/
reports/us/shadow/
reports/us/incidents/
reports/us/pnl/
```

Add `__init__.py` files in:

```text
trader/brokers/__init__.py
trader/markets/__init__.py
trader/markets/us/__init__.py
trader/strategies/__init__.py
trader/strategies/us/__init__.py
```

---

## 5. Milestone PRs

Implement the project as a sequence of PR-sized milestones.

### PR 1 — US Safety Harness, Market Clock, and Schema

Purpose: build the safety foundation. No KIS order calls.

Create files:

```text
harness/us/us_safety_policy.yaml
harness/us/us_market_clock_contract.yaml
harness/us/us_universe_contract.yaml
harness/us/us_alpha_promotion_gate.yaml
trader/markets/us/market_clock.py
trader/markets/us/calendar.py
trader/markets/us/symbols.py
migrations/00xx_us_agent_schema.sql
tests/test_us_symbol_not_zfilled.py
tests/test_us_market_clock_dst.py
tests/test_us_market_closed_no_order.py
tests/test_us_paper_only_guard.py
tests/test_us_strategy_candidates_schema.py
```

Acceptance command:

```bash
pytest -q tests/test_us_symbol_not_zfilled.py \
          tests/test_us_market_clock_dst.py \
          tests/test_us_market_closed_no_order.py \
          tests/test_us_paper_only_guard.py \
          tests/test_us_strategy_candidates_schema.py
```

PR 1 must not add any KIS order endpoint call.

---

### PR 2 — Universe Builder and Stock Discovery

Purpose: create US candidates without trading.

Create files:

```text
trader/markets/us/universe.py
scripts/agents/us_universe_builder_agent.py
scripts/agents/us_stock_discovery_agent.py
tests/test_us_universe_contract.py
tests/test_us_stock_discovery_output.py
```

Outputs:

```text
reports/us/stock_discovery/latest.json
reports/us/stock_discovery/latest.md
```

DB writes:

```text
symbols_master
universe_runs
universe_members
strategy_candidates
```

No orders. No KIS order secrets.

Acceptance command:

```bash
pytest -q tests/test_us_universe_contract.py tests/test_us_stock_discovery_output.py
python scripts/agents/us_universe_builder_agent.py --mode mock --as-of 2026-04-30
python scripts/agents/us_stock_discovery_agent.py --mode mock --as-of 2026-04-30
```

---

### PR 3 — Backtest, Walk-forward, and Shadow Trading

Purpose: validate strategies before any paper order.

Create files:

```text
scripts/agents/us_alpha_research_agent.py
scripts/agents/us_backtest_agent.py
scripts/agents/us_walkforward_agent.py
scripts/agents/us_shadow_trading_agent.py
trader/strategies/us/ai_infra_momentum.py
trader/strategies/us/qqq_regime_switch.py
trader/strategies/us/pullback_leader.py
tests/test_us_backtest_uses_traditional_return.py
tests/test_us_no_lookahead_bias.py
tests/test_us_alpha_promotion_gate.py
tests/test_us_shadow_before_paper_required.py
```

Outputs:

```text
research/us/ideas/alpha_ideas.yaml
research/us/experiments/experiment_registry.yaml
research/us/results/leaderboard.csv
reports/us/shadow/latest.json
reports/us/shadow/latest.md
```

DB writes:

```text
shadow_orders
```

No KIS order calls.

Acceptance command:

```bash
pytest -q tests/test_us_backtest_uses_traditional_return.py \
          tests/test_us_no_lookahead_bias.py \
          tests/test_us_alpha_promotion_gate.py \
          tests/test_us_shadow_before_paper_required.py
python scripts/agents/us_shadow_trading_agent.py --mode mock --as-of 2026-04-30
```

---

### PR 4 — KIS US Adapter in Mock Mode and Capability Probe

Purpose: create the broker adapter and probe without sending actual orders by default.

Create files:

```text
trader/brokers/kis_us_models.py
trader/brokers/kis_us.py
scripts/agents/us_capability_probe_agent.py
scripts/agents/us_kis_adapter_contract_agent.py
harness/us/kis_us_order_contract.yaml
tests/test_us_kis_order_contract.py
tests/test_us_limit_order_only.py
tests/test_us_no_live_order_path.py
tests/test_us_no_order_in_research.py
```

Acceptance command:

```bash
pytest -q tests/test_us_kis_order_contract.py \
          tests/test_us_limit_order_only.py \
          tests/test_us_no_live_order_path.py \
          tests/test_us_no_order_in_research.py
python scripts/agents/us_capability_probe_agent.py --mode mock
```

Default behavior:

```text
No real HTTP order call.
No order submission unless US_PAPER_SMOKE_ORDER=1 and CONFIRM_US_PAPER_SMOKE_ORDER=RUN_US_PAPER_SMOKE_ORDER.
```

---

### PR 5 — Risk Gate and Paper Execution Agent

Purpose: enable KIS US paper orders only behind strict gates.

Create files:

```text
scripts/agents/us_risk_gate_agent.py
scripts/agents/us_paper_execution_agent.py
trader/strategies/us/run_open.py
trader/strategies/us/run_close.py
tests/test_us_risk_gate_blocks_unsafe_order.py
tests/test_us_paper_execution_requires_all_gates.py
tests/test_us_duplicate_order_key_block.py
```

Paper execution must require:

```text
MARKET=US
KIS_ENV=practice
US_PAPER_ONLY=1
US_LIVE_TRADING_ENABLED=0
US_ALLOW_PREMARKET=0
US_ALLOW_AFTERHOURS=0
US_ORDER_TYPE_ALLOWED=LIMIT
```

Acceptance command:

```bash
pytest -q tests/test_us_risk_gate_blocks_unsafe_order.py \
          tests/test_us_paper_execution_requires_all_gates.py \
          tests/test_us_duplicate_order_key_block.py
```

---

### PR 6 — Log Analyst, Release Gate, and Manual Workflows

Purpose: add operational reporting and manual GitHub Actions.

Create files:

```text
scripts/agents/us_log_analyst_agent.py
scripts/agents/us_release_gate_agent.py
.github/workflows/us-agent-ci.yml
.github/workflows/us-capability-probe.yml
.github/workflows/us-research-nightly.yml
.github/workflows/us-shadow-trading.yml
.github/workflows/us-paper-open.yml
.github/workflows/us-paper-close.yml
```

Initial workflow rule:

```text
All US workflows must be workflow_dispatch only in the first version.
No schedule yet.
```

Acceptance command:

```bash
pytest -q tests/test_us_*.py
```

---

## 6. Required YAML Policy Contents

### 6.1 `harness/us/us_safety_policy.yaml`

Must contain:

```yaml
version: 1
market: US
allowed_envs:
  - us-paper
  - practice
required_env:
  MARKET: US
  KIS_ENV: practice
  US_PAPER_ONLY: '1'
  US_LIVE_TRADING_ENABLED: '0'
trading:
  allow_live: false
  allow_premarket: false
  allow_afterhours: false
  allow_fractional: false
  allowed_order_types:
    - LIMIT
risk_limits:
  paper_max_capital_usd: 10000
  max_positions: 10
  max_position_pct: 0.15
  max_sector_pct: 0.40
  daily_loss_limit_pct: 2.0
  total_drawdown_stop_pct: 8.0
blocked_workflows_for_orders:
  - us-research-nightly
  - us-shadow-trading
  - us-agent-ci
```

### 6.2 `harness/us/us_universe_contract.yaml`

Must contain:

```yaml
version: 1
market: US
symbol_rules:
  uppercase: true
  forbid_zfill: true
  allowed_pattern: '^[A-Z][A-Z0-9.-]{0,9}$'
required_fields:
  - symbol
  - name
  - exchange
  - currency
  - theme
  - active
allowed_exchanges:
  - NASD
  - NYSE
  - AMEX
allowed_currencies:
  - USD
min_seed_count: 30
seed_symbols_required:
  - NVDA
  - MSFT
  - AAPL
  - QQQ
  - SPY
```

### 6.3 `harness/us/us_alpha_promotion_gate.yaml`

Must contain:

```yaml
version: 1
research_to_shadow:
  min_trades: 20
  require_no_lookahead: true
  require_benchmark_comparison: true
  min_slippage_adjusted_return_pct: 0.0
  max_mdd_pct: 25.0
shadow_to_paper:
  min_shadow_trading_days: 10
  require_positive_vs_qqq: true
  require_positive_vs_spy: false
  require_risk_gate_pass: true
  require_manual_approval: true
paper_to_live:
  allowed: false
```

### 6.4 `harness/us/kis_us_order_contract.yaml`

Must contain:

```yaml
version: 1
market: US
paper_only: true
order_contract:
  allowed_sides:
    - BUY
    - SELL
  allowed_order_types:
    - LIMIT
  required_fields:
    - symbol
    - exchange
    - currency
    - side
    - qty
    - limit_price
    - client_order_key
  forbidden_when:
    KIS_ENV:
      - real
      - prod
      - live
    US_LIVE_TRADING_ENABLED:
      - '1'
```

---

## 7. Database Migration Details

Create migration:

```text
migrations/00xx_us_agent_schema.sql
```

Use the next available migration number. Do not overwrite existing migrations.

Required SQL:

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

CREATE INDEX IF NOT EXISTS ix_strategy_candidates_lookup
ON strategy_candidates (env, market, strategy, as_of, rank);

CREATE INDEX IF NOT EXISTS ix_shadow_orders_lookup
ON shadow_orders (env, market, strategy, as_of, symbol);
```

Rules:

```text
- Existing price_daily may be reused with market='US' and code=<ticker>.
- Existing orders/fills/positions/ledger_events may be reused only with market='US'.
- Do not write US candidates into pb1_watchlist.
```

---

## 8. Exact Function Contracts

### 8.1 `trader/markets/us/symbols.py`

Implement:

```python
def normalize_us_symbol(symbol: str) -> str:
    '''Return uppercase US ticker without zero-fill. Raise ValueError on invalid input.'''


def get_seed_symbols() -> list[dict]:
    '''Return curated US seed symbols with symbol, name, exchange, currency, theme.'''
```

Required behavior:

```text
normalize_us_symbol('aapl') == 'AAPL'
normalize_us_symbol(' AAPL ') == 'AAPL'
normalize_us_symbol('005930') raises ValueError unless explicitly allowed as ADR-like symbol; do not zfill.
normalize_us_symbol('') raises ValueError.
```

### 8.2 `trader/markets/us/market_clock.py`

Implement:

```python
def get_us_market_status(now_utc: datetime | None = None) -> dict:
    '''Return status dict with is_regular_open, is_trading_day, us_trade_date, kst_date, reason.'''


def require_regular_session(now_utc: datetime | None = None) -> None:
    '''Raise RuntimeError if US regular session is not open.'''
```

CLI:

```bash
python -m trader.markets.us.market_clock --print-status-json
python -m trader.markets.us.market_clock --require-regular-session
```

### 8.3 `trader/markets/us/universe.py`

Implement:

```python
def build_us_seed_universe(as_of: date, *, env: str = 'us-paper') -> list[dict]:
    '''Return validated seed universe rows.'''


def score_us_candidates(rows: list[dict], *, as_of: date) -> list[dict]:
    '''Return ranked candidate rows with score, rank, style, meta_json.'''
```

### 8.4 `trader/brokers/kis_us.py`

Implement class:

```python
class KisUSPaperBroker:
    def __init__(self, *, app_key: str, app_secret: str, cano: str, acnt_prdt_cd: str, base_url: str, paper_only: bool = True): ...

    def assert_paper_env(self) -> None: ...
    def get_quote(self, symbol: str, exchange: str) -> dict: ...
    def get_us_balance(self) -> dict: ...
    def get_orderable_cash_usd(self) -> float: ...
    def place_limit_order(self, *, symbol: str, exchange: str, side: str, qty: int, limit_price: float, client_order_key: str) -> dict: ...
    def parse_order_response(self, response: dict) -> dict: ...
```

Default mode must allow mock responses without network.

Network mode must be opt-in via:

```text
US_KIS_HTTP_ENABLED=1
```

Order submission must additionally require:

```text
US_PAPER_ORDER_ENABLED=1
CONFIRM_US_PAPER_ORDER=RUN_US_PAPER_ORDER
```

### 8.5 `scripts/agents/us_risk_gate_agent.py`

Implement:

```python
def evaluate_order(order: dict, context: dict, policy_path: str = 'harness/us/us_safety_policy.yaml') -> dict:
    '''Return {ok: bool, reasons: list[str], normalized_order: dict}.'''
```

Block if:

```text
- MARKET != US
- KIS_ENV != practice
- US_PAPER_ONLY != 1
- US_LIVE_TRADING_ENABLED == 1
- side not in BUY/SELL
- order_type != LIMIT
- qty <= 0
- limit_price <= 0
- outside regular session
- client_order_key missing
```

---

## 9. Strategy and Return Rules

Backtest must use traditional trade return:

```python
return_pct = (sell_price - buy_price) / buy_price * 100.0
```

Never use a cumulative product interpretation where 100 means break-even.

Benchmark comparison must include at least:

```text
QQQ
SPY
```

Candidate scoring may rank strategies by:

```text
score = 0.35 * qqq_excess_return_score
      + 0.20 * mdd_score
      + 0.15 * win_rate_score
      + 0.15 * profit_factor_score
      + 0.10 * monthly_hit_score
      + 0.05 * turnover_penalty_score
```

Promotion is not allowed if `max_mdd_pct` breaches policy even when return is high.

---

## 10. Initial Universe

Use this seed list in `trader/markets/us/symbols.py`:

```text
AAPL, MSFT, NVDA, AMD, AVGO, MRVL, MU, ARM, TSM, ASML,
AMAT, LRCX, KLAC, CRDO, CIEN, COHR, LITE, AAOI, ANET,
VRT, BE, DELL, SMCI, GOOGL, AMZN, META, PLTR,
QQQ, QQQM, SPY, SMH, SOXX, XLK, IYW, SHY, SGOV, BIL
```

Each seed row must include:

```text
symbol
name
exchange
currency='USD'
theme
active=true
```

Allowed themes:

```text
mega_cap_ai
semiconductor
optical_interconnect
datacenter_power
ai_server_network
cloud_infra
etf_risk_on
etf_risk_off
```

---

## 11. GitHub Actions Rules

First version workflows must be manual only:

```yaml
on:
  workflow_dispatch:
```

Do not add schedule until manual runs are successful.

Workflows to create in PR 6:

```text
.github/workflows/us-agent-ci.yml
.github/workflows/us-capability-probe.yml
.github/workflows/us-research-nightly.yml
.github/workflows/us-shadow-trading.yml
.github/workflows/us-paper-open.yml
.github/workflows/us-paper-close.yml
```

Workflow environment separation:

```text
us-agent-ci: no secrets
us-research-nightly: no order secrets
us-shadow-trading: no order secrets
us-capability-probe: may use read-only/data KIS secrets if required
us-paper-open: us-paper environment only
us-paper-close: us-paper environment only
```

Paper workflows must print environment proof:

```bash
echo '[US][ENV] MARKET='${MARKET}
echo '[US][ENV] KIS_ENV='${KIS_ENV}
echo '[US][ENV] US_PAPER_ONLY='${US_PAPER_ONLY}
echo '[US][ENV] US_LIVE_TRADING_ENABLED='${US_LIVE_TRADING_ENABLED}
```

Never print secrets.

---

## 12. Nullim Launcher Later, Not Now

After `us-agent` has stable manual runs, add launcher workflows to `nullim` in a separate PR.

Launcher pattern:

```yaml
- name: Checkout US agent code
  uses: actions/checkout@v4
  with:
    ref: us-agent
    fetch-depth: 1
```

After a stable branch exists, replace `ref: us-agent` with:

```yaml
ref: us-stable
```

---

## 13. Completion Checklist

Before saying the milestone is done, verify:

```text
[ ] Existing Korean workflows were not modified.
[ ] US tickers are not zero-filled.
[ ] US candidates are stored in strategy_candidates, not pb1_watchlist.
[ ] US market clock has DST tests.
[ ] Research workflow has no order secret access.
[ ] Shadow workflow has no order secret access.
[ ] Paper execution requires US_PAPER_ONLY=1.
[ ] Paper execution hard-fails on real/live env.
[ ] Backtest uses traditional return calculation.
[ ] Shadow trading exists before paper execution.
[ ] Risk gate blocks unsafe orders.
[ ] Log analyst produces structured JSON and Markdown reports.
[ ] No live US trading path exists.
```

---

## 14. First Codex Task to Execute Now

Start with PR 1 only.

Do not implement KIS HTTP calls yet.

Task:

```text
Implement PR 1: US Safety Harness, Market Clock, and Schema.

Files to add:
- harness/us/us_safety_policy.yaml
- harness/us/us_market_clock_contract.yaml
- harness/us/us_universe_contract.yaml
- harness/us/us_alpha_promotion_gate.yaml
- trader/markets/us/market_clock.py
- trader/markets/us/calendar.py
- trader/markets/us/symbols.py
- migrations/00xx_us_agent_schema.sql
- tests/test_us_symbol_not_zfilled.py
- tests/test_us_market_clock_dst.py
- tests/test_us_market_closed_no_order.py
- tests/test_us_paper_only_guard.py
- tests/test_us_strategy_candidates_schema.py

Do not modify domestic PB1 files.
Do not add order calls.
Run the PR 1 acceptance command and report results.
```

This is the first executable task. Stop after PR 1 unless explicitly instructed to continue.
