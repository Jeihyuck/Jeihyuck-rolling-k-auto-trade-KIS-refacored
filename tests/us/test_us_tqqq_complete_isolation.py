from datetime import date

from trader.us.execution.order_router import route_order
import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.integration import exclude_owned, recover_state_from_broker
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.policy_state import update_adaptive_policy_state
from trader.us.infinite.risk_adapter import effective_regime
from trader.us.infinite.strategy import evaluate
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_router import route_exit_by_book_horizon


def context(state="NORMAL", **extra):
    return {"market_state": state, "tqqq_context_quality": "ok",
            "qqq_completed_close": 100, "qqq_ma50": 99, "qqq_ma200": 98,
            "qqq_ma200_slope": .1, "qqq_20d_return": .02, "qqq_drawdown_252": -.05,
            "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5, **extra}


def test_standard_profit_and_swing_exit_never_create_tqqq_intent():
    position = {"symbol": "TQQQ", "qty": 10, "orderable_qty": 10,
                "avg_price": 100, "current_price": 110, "exchange": "NASDAQ"}
    assert build_profit_capture_intents([position], {"profit_capture_enabled": True},
                                        profit_capture_state={}) == []
    assert route_exit_by_book_horizon(position, 80) is None
    assert exclude_owned([position]) == []


def test_router_rejects_non_owner_before_any_broker_or_db_work():
    result = route_order({"symbol": "TQQQ", "side": "BUY"})
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "tqqq_ownership_rejected"
    assert result["broker_submit"] is False


def test_every_documented_tqqq_regime_has_one_effective_policy():
    states = ["STRONG_RISK_ON", "RISK_ON", "NEUTRAL", "DEFENSIVE", "RISK_OFF",
              "CRASH", "DEFENSE_CRASH", "CHOP_HIGH_VOL", "CAPITAL_PRESERVATION"]
    for state in states:
        effective, multiplier, _reserve, allowed, reason = effective_regime(
            {"market_state": state, "market_regime": "DEFENSIVE" if state == "STRONG_RISK_ON" else state}
        )
        assert effective
        assert multiplier >= 0
        assert isinstance(allowed, bool)
        assert reason


def test_conflicting_defensive_regime_blocks_buy_but_never_safe_sell():
    effective, multiplier, reserve, allowed, _ = effective_regime(
        {"market_state": "STRONG_RISK_ON", "market_regime": "DEFENSIVE"}
    )
    assert (effective, multiplier, reserve, allowed) == ("DEFENSIVE", 0.5, False, True)
    config = InfiniteConfig(unit_usd=1000, max_daily_buy_usd=1000)
    blocked = evaluate(config=config, state=InfiniteState(), position=PositionSnapshot(price=100),
                       trading_date=date(2026, 8, 14), overlay=context("STRONG_RISK_ON"),
                       entry_allowed=allowed, buy_multiplier=multiplier,
                       effective_regime_name=effective)
    assert (blocked.action, blocked.reason) == (Action.BLOCK, "tqqq_regime_new_cycle_block")
    sold = evaluate(config=config, state=InfiniteState(cycle_id="c", core_filled_notional=100),
                    position=PositionSnapshot(qty=1, orderable_qty=1, average_price=100, price=110),
                    trading_date=date(2026, 8, 14), overlay=context("STRONG_RISK_ON"),
                    entry_allowed=False, buy_multiplier=0)
    assert sold.action == Action.SELL


def test_strong_risk_on_multiplier_changes_real_order_size():
    regime, multiplier, _reserve, allowed, _ = effective_regime(
        {"market_state": "STRONG_RISK_ON", "market_regime": "STRONG_RISK_ON"}
    )
    assert (regime, multiplier, allowed) == ("STRONG_RISK_ON", 1.0, True)
    decision = evaluate(
        config=InfiniteConfig(unit_usd=400, max_daily_buy_usd=1000), state=InfiniteState(),
        position=PositionSnapshot(price=100), trading_date=date(2026, 8, 14),
        overlay=context("STRONG_RISK_ON"), entry_allowed=allowed,
        buy_multiplier=multiplier,
    )
    assert (decision.action, decision.qty, decision.notional) == (Action.BUY, 4, 400)


@pytest.mark.parametrize("regime", ["CRASH", "DEFENSE_CRASH", "CAPITAL_PRESERVATION"])
def test_fail_closed_effective_regimes_block_new_buy(regime):
    effective, multiplier, _reserve, allowed, _reason = effective_regime(
        {"market_state": regime, "market_regime": regime}
    )
    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(), position=PositionSnapshot(price=100),
        trading_date=date(2026, 8, 14), overlay=context("STRONG_RISK_ON"),
        entry_allowed=allowed, buy_multiplier=multiplier, effective_regime_name=effective,
    )
    assert decision.action == Action.BLOCK


def test_non_tqqq_symbol_configuration_fails_fast(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_SYMBOL", "QLD")
    with pytest.raises(ValueError, match="must be TQQQ"):
        InfiniteConfig.from_env()


def test_owner_attribution_is_mirrored_to_meta_and_survives_intent_store(monkeypatch):
    from trader.us.db import repos

    monkeypatch.delenv("PBCORE_DB_URL", raising=False)
    repos.reset_memory_stores()
    intent = {
        "symbol": "TQQQ", "side": "BUY", "qty": 1, "notional_usd": 100,
        "limit_price": 100, "trade_date": "2026-08-14", "client_order_key": "TQQQ_INF:test:BUY",
        "strategy": "TQQQ_INFINITE_V3", "strategy_owner": "TQQQ_INFINITE",
        "strategy_name": "TQQQ_INFINITE", "strategy_version": "ADAPTIVE_RUNWAY_V2",
        "sleeve_id": "TQQQ_INFINITE", "meta": {},
    }
    routed = route_order(intent, signal_only=True)
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert routed["intent"]["meta"][field] == routed["intent"][field]
    assert repos.save_order_intent(routed["intent"], trade_date="2026-08-14")
    loaded = repos.load_open_order_intents("2026-08-14")[0]
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert loaded[field] == routed["intent"][field]
        assert loaded["meta"][field] == routed["intent"][field]
    assert repos.save_order_ack({
        **routed["intent"], "order_no": "TQQQ-ACK-1", "qty_requested": 1,
        "qty_filled": 0, "status": "ACK",
    }, trade_date="2026-08-14")
    stored_order = repos._MEM_ORDERS[0]
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert stored_order["meta"][field] == routed["intent"][field]
    assert repos.save_fills([{
        "symbol": "TQQQ", "side": "BUY", "qty": 1, "price_usd": 100,
        "order_no": "TQQQ-ACK-1", "client_order_key": "TQQQ_INF:test:BUY",
        "meta": {"fill_evidence_type": "KIS_ACTUAL"},
    }], trade_date="2026-08-14") == 1
    stored_fill = repos._MEM_FILLS[0]
    for field in ("strategy_owner", "strategy_name", "strategy_version", "sleeve_id"):
        assert stored_fill["meta"][field] == routed["intent"][field]


def _recovery_overlay(state="DEFENSE_CRASH_REBOUND"):
    return {"market_state": state, "market_regime": "RISK_ON", "qqq_completed_close": 110,
            "qqq_ma50": 100, "qqq_ma200": 105, "qqq_ma200_slope": -1,
            "qqq_20d_return": .05, "qqq_drawdown_252": -.2,
            "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5}


def test_regime_does_not_mutate_persistent_reserve_unlock():
    cfg = InfiniteConfig(core_capital_usd=100, reserve_capital_usd=100,
                         total_capital_usd=200, max_total_capital_usd=200)
    locked = InfiniteState(core_filled_notional=100)
    strong = update_adaptive_policy_state(
        state=locked, trading_date=date(2026, 8, 13),
        overlay=_recovery_overlay("STRONG_RISK_ON"), config=cfg,
    )
    assert strong.reserve_unlocked is False

    crashed = InfiniteState(core_filled_notional=100, material_market_crash=True,
                            metadata={"structural_bear_seen": True})
    first = update_adaptive_policy_state(
        state=crashed, trading_date=date(2026, 8, 13), overlay=_recovery_overlay(), config=cfg,
    )
    assert first.reserve_unlocked is False
    confirmed = update_adaptive_policy_state(
        state=first, trading_date=date(2026, 8, 14), overlay=_recovery_overlay(), config=cfg,
    )
    assert confirmed.reserve_unlocked is True
    for index, regime in enumerate(("RISK_ON", "NORMAL", "DEFENSE_CAUTION"), start=15):
        confirmed = update_adaptive_policy_state(
            state=confirmed, trading_date=date(2026, 8, index),
            overlay=_recovery_overlay(regime), config=cfg,
        )
        assert confirmed.reserve_unlocked is True


def test_reserve_buy_needs_state_unlock_and_regime_permission_but_sell_is_first():
    cfg = InfiniteConfig(core_capital_usd=100, reserve_capital_usd=500,
                         total_capital_usd=600, max_total_capital_usd=600,
                         unit_usd=100, max_daily_buy_usd=100)
    locked = InfiniteState(cycle_id="c", core_filled_notional=100)
    common = dict(config=cfg, position=PositionSnapshot(price=50), trading_date=date(2026, 8, 14),
                  overlay=context("STRONG_RISK_ON"))
    assert evaluate(state=locked, regime_reserve_permission=True, **common).reason == "reserve_locked"
    unlocked = InfiniteState(cycle_id="c", core_filled_notional=100, reserve_unlocked=True,
                             material_market_crash=True)
    assert evaluate(state=unlocked, regime_reserve_permission=False, **common).reason == "reserve_locked"
    assert evaluate(state=unlocked, regime_reserve_permission=True, **common).action == Action.BUY
    sell = evaluate(
        config=cfg, state=unlocked, position=PositionSnapshot(qty=1, orderable_qty=1, average_price=100, price=110),
        trading_date=date(2026, 8, 14), overlay={"market_state": "DEFENSE_CRASH_CONFIRMED"},
        entry_allowed=False, regime_reserve_permission=False,
    )
    assert sell.action == Action.SELL


def test_postgres_intent_and_ack_sql_meta_preserve_owner(monkeypatch):
    import json
    from trader.us.db import repos

    calls = []

    class Result:
        def fetchone(self):
            return None

    class Connection:
        def execute(self, statement, params):
            calls.append((str(statement), dict(params)))
            return Result()

    class Begin:
        def __enter__(self):
            return Connection()
        def __exit__(self, *_args):
            return False

    class Engine:
        def begin(self):
            return Begin()

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    attribution = {"strategy_owner": "TQQQ_INFINITE", "strategy_name": "TQQQ_INFINITE",
                   "strategy_version": "ADAPTIVE_RUNWAY_V2", "sleeve_id": "TQQQ_INFINITE"}
    intent = {"client_order_key": "TQQQ_INF:sql:BUY", "symbol": "TQQQ", "side": "BUY",
              "qty": 1, "limit_price": 100, "notional_usd": 100,
              "strategy": "TQQQ_INFINITE_V3", "meta": attribution, **attribution}
    assert repos.save_order_intent(intent, "2026-08-14")
    assert repos.save_order_ack({**intent, "qty_requested": 1, "order_no": "SQL-ACK", "status": "ACK"},
                                "2026-08-14")
    persisted = [json.loads(params["meta"]) for sql, params in calls
                 if "INSERT INTO us_order_intents" in sql or "INSERT INTO us_orders" in sql]
    assert len(persisted) == 2
    for meta in persisted:
        assert all(meta[field] == value for field, value in attribution.items())


def test_kis_balance_recovers_exact_tqqq_state_without_orphan_classification():
    broker = PositionSnapshot(qty=7, orderable_qty=6, average_price=51.25, price=52)
    recovered = recover_state_from_broker(
        config=InfiniteConfig(), broker=broker, trading_date=date(2026, 8, 14),
        cycle_id="existing-cycle",
    )
    assert recovered.cycle_id == "existing-cycle"
    assert recovered.anchor_price == 51.25
    assert recovered.metadata["broker_qty"] == 7
    assert recovered.metadata["broker_orderable_qty"] == 6
    assert recovered.metadata["broker_average_price"] == 51.25
    assert "last_buy_fill_price" not in recovered.metadata
    assert recovered.metadata["buy_reference_price"] == 51.25
    assert recovered.metadata["buy_reference_source"] == "KIS_BROKER_AVG_FALLBACK"
    assert recovered.metadata["ownership_source"] == "SYMBOL_INVARIANT_RECOVERY"
    assert recovered.metadata["strategy_owner"] == "TQQQ_INFINITE"
    decision = evaluate(
        config=InfiniteConfig(), state=None, position=broker,
        trading_date=date(2026, 8, 14), overlay={"market_state": "NORMAL"},
    )
    assert decision.reason != "orphan_position"


def test_uncertain_broker_recovery_allows_only_conservative_core_buy():
    broker = PositionSnapshot(qty=5, orderable_qty=5, average_price=50, price=49)
    recovered = recover_state_from_broker(
        config=InfiniteConfig(), broker=broker, trading_date=date(2026, 8, 1),
        cycle_id="recovered-cycle",
    )
    decision = evaluate(
        config=InfiniteConfig(), state=recovered, position=broker,
        trading_date=date(2026, 8, 14), overlay=context("NORMAL"),
        effective_regime_name="NEUTRAL", buy_multiplier=.75,
        regime_reserve_permission=False,
    )
    assert decision.action == Action.BUY
    assert recovered.metadata.get("last_buy_fill_price") is None

    premium = evaluate(
        config=InfiniteConfig(), state=recovered,
        position=PositionSnapshot(qty=5, orderable_qty=5, average_price=50, price=50.01),
        trading_date=date(2026, 8, 14), overlay=context("STRONG_RISK_ON"),
        effective_regime_name="STRONG_RISK_ON", buy_multiplier=1,
        regime_reserve_permission=True,
    )
    assert (premium.action, premium.reason) == (
        Action.WAIT, "tqqq_recovery_price_above_broker_average",
    )


def test_uncertain_broker_recovery_never_uses_reserve():
    state = InfiniteState(
        cycle_id="recovered-cycle", status=Status.ACTIVE,
        core_filled_notional=7_500, reserve_unlocked=True,
        material_market_crash=True,
        metadata={"recovery_accounting_uncertain": True,
                  "buy_reference_price": 50,
                  "buy_reference_source": "KIS_BROKER_AVG_FALLBACK"},
    )
    decision = evaluate(
        config=InfiniteConfig(), state=state,
        position=PositionSnapshot(qty=150, orderable_qty=150, average_price=50, price=49),
        trading_date=date(2026, 8, 14), overlay=context("STRONG_RISK_ON"),
        effective_regime_name="STRONG_RISK_ON", regime_reserve_permission=True,
    )
    assert (decision.action, decision.reason) == (Action.BLOCK, "reserve_locked")


def test_recovered_tqqq_blocks_partial_strategy_exit():
    broker = PositionSnapshot(qty=7, orderable_qty=6, average_price=50, price=55)
    decision = evaluate(
        config=InfiniteConfig(), state=None, position=broker,
        trading_date=date(2026, 8, 14), overlay={"market_state": "DEFENSE_CRASH_CONFIRMED"},
        entry_allowed=False, regime_reserve_permission=False,
    )
    assert (decision.action, decision.reason) == (Action.BLOCK, "tqqq_full_exit_qty_not_ready")


def test_recovered_tqqq_uses_ten_percent_full_exit():
    broker = PositionSnapshot(qty=10, orderable_qty=10, average_price=50, price=55)
    decision = evaluate(config=InfiniteConfig(), state=None, position=broker,
                        trading_date=date(2026, 8, 14), overlay={})
    assert (decision.action, decision.qty, decision.notional) == (Action.SELL, 10, 550)


@pytest.mark.parametrize(("orderable", "reason"), [
    (None, "tqqq_orderable_qty_missing"), (0, "tqqq_no_orderable_qty"),
])
def test_tqqq_take_profit_fails_closed_without_sellable_quantity(orderable, reason):
    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(cycle_id="c"),
        position=PositionSnapshot(qty=10, orderable_qty=orderable, average_price=50, price=55),
        trading_date=date(2026, 8, 14), overlay={}, entry_allowed=False,
    )
    assert (decision.action, decision.reason) == (Action.BLOCK, reason)


def test_tqqq_dedicated_router_scope_keeps_250_unit_and_real_ack(monkeypatch):
    from unittest.mock import patch
    from trader.us.execution import order_router

    for key, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice", "DRY_RUN": "0",
        "TRADING_REGION": "US", "US_AGENT_ENABLED": "1", "US_PAPER_TRADING_ENABLED": "1",
        "US_MAX_ORDER_USD": "100", "US_MAX_DAILY_NOTIONAL_USD": "100",
        "US_SESSION_WINDOW_VALID": "1", "US_PREP_CONTRACT_OK": "1", "US_BALANCE_AVAILABLE": "1",
    }.items():
        monkeypatch.setenv(key, value)
    order_router._SENT_ORDER_KEYS.clear()

    class Kis:
        calls = []
        def get_orderable_cash(self, **_kwargs): return 1000
        def place_us_buy_order(self, symbol, exchange, qty, price):
            self.calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "TQQQ-BUY-ACK"}}

    intent = {
        "symbol": "TQQQ", "exchange": "NASDAQ", "side": "BUY", "qty": 5,
        "limit_price": 50, "notional_usd": 250, "trade_date": "2026-08-14",
        "client_order_key": "TQQQ_INF_V3:e2e:2026-08-14:BUY",
        "strategy": "TQQQ_INFINITE_V3", "strategy_owner": "TQQQ_INFINITE",
        "strategy_name": "TQQQ_INFINITE", "strategy_version": "ADAPTIVE_RUNWAY_V2",
        "sleeve_id": "TQQQ_INFINITE", "meta": {
            "strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE",
            "tqqq_daily_committed_before_usd": 0, "tqqq_cycle_committed_before_usd": 0,
            "tqqq_max_daily_buy_usd": 250, "tqqq_max_total_capital_usd": 10_000,
        },
    }
    client = Kis()
    with (patch("trader.us.db.repos.save_order_intent", return_value=True),
          patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
          patch("trader.us.db.repos.save_order_ack", return_value=True),
          patch("trader.us.db.repos.mark_order_intent_sent"),
          patch("trader.us.execution.order_journal.append_order_event", return_value=True),
          patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="TQQQ-BUY-ACK"),
          patch("trader.us.execution.risk_gate.check_pending_order"),
          patch("trader.us.execution.risk_gate.check_entry_cutoff")):
        result = route_order(intent, available_cash_usd=1000, kis_client=client)
    assert result["status"] == "ACK"
    assert client.calls == [("TQQQ", "NASDAQ", 5, 50.0)]


def test_run_sleeve_buy_crosses_router_ack_and_fill_persistence(monkeypatch):
    from unittest.mock import patch
    from trader.us.db import repos
    from trader.us.infinite.integration import run_sleeve

    for key, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice", "DRY_RUN": "0",
        "TRADING_REGION": "US", "US_AGENT_ENABLED": "1", "US_PAPER_TRADING_ENABLED": "1",
        "US_MAX_ORDER_USD": "100", "US_MAX_DAILY_NOTIONAL_USD": "100",
        "US_SESSION_WINDOW_VALID": "1", "US_PREP_CONTRACT_OK": "1", "US_BALANCE_AVAILABLE": "1",
        "US_TQQQ_INFINITE_ENABLED": "1", "US_TQQQ_INFINITE_REAL_ORDER": "1",
    }.items(): monkeypatch.setenv(key, value)
    monkeypatch.delenv("PBCORE_DB_URL", raising=False)
    repos.reset_memory_stores()

    class Repo:
        state = InfiniteState()
        def ensure_schema(self): pass
        def load_state(self, **_kwargs): return self.state
        def save_state(self, value): self.state = value
        def pending_sides(self, *_args): return False, False
        def pending_buy_notional(self, *_args): return 0
        def fill_accounting(self, *_args): return 0, 0, 0, None, None
        def reconcile_metadata(self, value, **_kwargs): return value

    class Kis:
        calls = []
        def get_orderable_cash(self, **_kwargs): return 1_000
        def place_us_buy_order(self, symbol, exchange, qty, price):
            self.calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "RUN-SLEEVE-BUY"}}

    repository, client = Repo(), Kis()
    def real_route(intent):
        return route_order(intent, available_cash_usd=1_000, kis_client=client)
    with (patch("trader.us.execution.order_journal.append_order_event", return_value=True),
          patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="RUN-SLEEVE-BUY"),
          patch("trader.us.execution.risk_gate.check_pending_order"),
          patch("trader.us.execution.risk_gate.check_entry_cutoff")):
        result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 14),
                            overlay=context(), repository=repository, route=real_route)
    assert result["status"] == "ACK"
    assert client.calls == [("TQQQ", "NASDAQ", 3, 50.0)]  # NORMAL 0.75 × $250, whole shares
    order = repos._MEM_ORDERS[0]
    assert order["status"] == "ACK" and order["meta"]["strategy_owner"] == "TQQQ_INFINITE"
    assert repos.save_fills([{
        "symbol": "TQQQ", "side": "BUY", "qty": 3, "price_usd": 50,
        "order_no": "RUN-SLEEVE-BUY", "client_order_key": order["client_order_key"],
        "meta": {"fill_evidence_type": "KIS_ACTUAL"},
    }], trade_date="2026-08-14") == 1
    assert repos._MEM_FILLS[0]["meta"]["strategy_owner"] == "TQQQ_INFINITE"


def test_run_sleeve_ten_percent_sell_crosses_real_router_to_kis_ack(monkeypatch):
    from datetime import datetime, timezone
    from unittest.mock import patch
    from trader.us.infinite.integration import run_sleeve

    for key, value in {
        "KIS_ENV": "practice", "STRATEGY_ENV": "practice", "DRY_RUN": "0",
        "TRADING_REGION": "US", "US_AGENT_ENABLED": "1", "US_PAPER_TRADING_ENABLED": "1",
        "US_SESSION_WINDOW_VALID": "1", "US_PREP_CONTRACT_OK": "1", "US_BALANCE_AVAILABLE": "1",
        "US_TQQQ_INFINITE_ENABLED": "1", "US_TQQQ_INFINITE_REAL_ORDER": "1",
    }.items(): monkeypatch.setenv(key, value)

    state = InfiniteState(cycle_id="e2e-sell", core_filled_notional=500)
    class Repo:
        def ensure_schema(self): pass
        def load_state(self, **_kwargs): return state
        def save_state(self, _state): pass
        def pending_sides(self, *_args): return False, False
        def pending_buy_notional(self, *_args): return 0
        def fill_accounting(self, *_args): return 500, 0, 0, None, 50
        def reconcile_metadata(self, value, **_kwargs): return value

    class Kis:
        calls = []
        def get_balance(self, force_refresh=False):
            return {"positions": [{"symbol": "TQQQ", "qty": 10, "orderable_qty": 10,
                                    "avg_price": 50, "currency": "USD"}]}
        def place_us_sell_order(self, symbol, exchange, qty, price):
            self.calls.append((symbol, exchange, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "TQQQ-SELL-ACK"}}

    client = Kis()
    asof = datetime.now(timezone.utc).isoformat()
    positions = [{"symbol": "TQQQ", "qty": 10, "orderable_qty": 10, "avg_price": 50,
                  "broker_avg_price_source": "kis_pchs_avg_pric", "broker_avg_price_asof": asof}]
    routed = []
    def real_route(intent):
        routed.append(intent)
        return route_order(intent, available_cash_usd=1000, current_position_symbols={"TQQQ"},
                           kis_client=client)
    with (patch("trader.us.db.repos.save_order_intent", return_value=True),
          patch("trader.us.db.repos.load_today_order_keys", return_value=set()),
          patch("trader.us.db.repos.save_order_ack", return_value=True),
          patch("trader.us.db.repos.mark_order_intent_sent"),
          patch("trader.us.execution.order_journal.append_order_event", return_value=True),
          patch("trader.us.execution.kis_us_response_parser.extract_order_no", return_value="TQQQ-SELL-ACK"),
          patch("trader.us.execution.risk_gate.check_pending_sell_order_hard")):
        result = run_sleeve(positions=positions, price=55, trading_date=date(2026, 8, 14),
                            overlay={}, repository=Repo(), route=real_route)
    assert result["status"] == "ACK"
    assert client.calls == [("TQQQ", "NASDAQ", 10, 55.0)]
    assert routed[0]["reason"] == "TAKE_PROFIT_TQQQ_INFINITE"
    assert routed[0]["position_action"] == "FULL_EXIT_SELL"
    assert routed[0]["meta"]["tp_threshold_fraction"] == "0.1"


@pytest.mark.parametrize(("market_state", "market_regime", "expected"), [
    ("STRONG_RISK_ON", "DEFENSIVE", "DEFENSIVE"),
    ("NORMAL", "RISK_OFF", "RISK_OFF"),
    ("RISK_ON", "CHOP_HIGH_VOL", "CHOP_HIGH_VOL"),
    ("DEFENSE_CAUTION", "RISK_ON", "DEFENSE_CAUTION"),
    ("DEFENSE_CRASH_CONFIRMED", "RISK_ON", "DEFENSE_CRASH_CONFIRMED"),
    ("DEFENSE_CRASH_REBOUND", "DEFENSIVE", "DEFENSIVE"),
])
def test_regime_conflicts_choose_the_more_conservative_policy(market_state, market_regime, expected):
    effective, _multiplier, _reserve, _entry, _reason = effective_regime(
        {"market_state": market_state, "market_regime": market_regime}
    )
    assert effective == expected


def test_full_exit_threshold_and_pending_contract():
    state = InfiniteState(cycle_id="full-exit")
    below = evaluate(config=InfiniteConfig(), state=state,
                     position=PositionSnapshot(qty=10, orderable_qty=10, average_price=50, price=54.995),
                     trading_date=date(2026, 8, 14), overlay=context())
    assert below.action != Action.SELL
    above = evaluate(config=InfiniteConfig(), state=state,
                     position=PositionSnapshot(qty=10, orderable_qty=10, average_price=50, price=56),
                     trading_date=date(2026, 8, 14), overlay={})
    assert (above.action, above.qty) == (Action.SELL, 10)
    pending = evaluate(config=InfiniteConfig(), state=state,
                       position=PositionSnapshot(qty=10, orderable_qty=6, average_price=50, price=55),
                       trading_date=date(2026, 8, 14), overlay={}, pending_sell=True)
    assert (pending.action, pending.reason, pending.next_status) == (
        Action.BLOCK, "tqqq_full_exit_pending", Status.EXIT_PENDING,
    )


def test_full_exit_partial_cancel_retry_and_completion_state_machine():
    state = InfiniteState(cycle_id="full-exit", status=Status.EXIT_PENDING)
    partial_open = evaluate(
        config=InfiniteConfig(), state=state,
        position=PositionSnapshot(qty=4, orderable_qty=0, average_price=50, price=55),
        trading_date=date(2026, 8, 14), overlay={}, pending_sell=True,
    )
    assert (partial_open.action, partial_open.reason, partial_open.next_status) == (
        Action.BLOCK, "tqqq_full_exit_pending", Status.EXIT_PENDING,
    )
    cancelled_below_target = evaluate(
        config=InfiniteConfig(), state=state,
        position=PositionSnapshot(qty=4, orderable_qty=4, average_price=50, price=54),
        trading_date=date(2026, 8, 14), overlay={}, pending_sell=False,
    )
    assert (cancelled_below_target.action, cancelled_below_target.next_status) == (
        Action.WAIT, Status.EXIT_PENDING,
    )
    cancelled_reached_target = evaluate(
        config=InfiniteConfig(), state=state,
        position=PositionSnapshot(qty=4, orderable_qty=4, average_price=50, price=55),
        trading_date=date(2026, 8, 14), overlay={}, pending_sell=False,
    )
    assert (cancelled_reached_target.action, cancelled_reached_target.qty,
            cancelled_reached_target.next_status) == (Action.SELL, 4, Status.EXIT_PENDING)
    complete = evaluate(
        config=InfiniteConfig(), state=state, position=PositionSnapshot(qty=0, orderable_qty=0, price=55),
        trading_date=date(2026, 8, 14), overlay={}, pending_sell=False,
    )
    assert (complete.action, complete.next_status) == (Action.WAIT, Status.COMPLETE)


def _conflict_decision(market_state, market_regime, *, state, position, trading_date=date(2026, 8, 14)):
    overlay = context(market_state, market_regime=market_regime)
    effective, multiplier, reserve_permission, entry_allowed, _reason = effective_regime(overlay)
    decision = evaluate(
        config=InfiniteConfig(), state=state, position=position, trading_date=trading_date,
        overlay=overlay, effective_regime_name=effective, buy_multiplier=multiplier,
        regime_reserve_permission=reserve_permission, entry_allowed=entry_allowed,
    )
    return effective, reserve_permission, decision


def test_conflicting_regimes_change_actual_buy_decisions_not_only_labels():
    defensive_state = InfiniteState(
        cycle_id="c", status=Status.ACTIVE, core_filled_notional=250,
        last_buy_date=date(2026, 8, 13),
        metadata={"long_trend": "BEAR", "last_buy_fill_price": 100},
    )
    effective, reserve, decision = _conflict_decision(
        "STRONG_RISK_ON", "DEFENSIVE", state=defensive_state,
        position=PositionSnapshot(qty=5, orderable_qty=5, average_price=100, price=94),
    )
    assert (effective, reserve, decision.reason) == ("DEFENSIVE", False, "defense_risk_off_wait")

    for market_state, market_regime, expected in (
        ("NORMAL", "RISK_OFF", "RISK_OFF"),
        ("RISK_ON", "CHOP_HIGH_VOL", "CHOP_HIGH_VOL"),
    ):
        effective, _reserve, decision = _conflict_decision(
            market_state, market_regime, state=InfiniteState(), position=PositionSnapshot(price=50),
        )
        assert effective == expected and decision.action == Action.BLOCK

    caution_state = InfiniteState(cycle_id="c", status=Status.ACTIVE,
                                   last_buy_date=date(2026, 8, 13), core_filled_notional=250)
    effective, _reserve, decision = _conflict_decision(
        "DEFENSE_CAUTION", "RISK_ON", state=caution_state,
        position=PositionSnapshot(qty=5, orderable_qty=5, average_price=100, price=100),
    )
    assert effective == "DEFENSE_CAUTION" and decision.reason == "routine_gap_wait"

    effective, _reserve, decision = _conflict_decision(
        "DEFENSE_CRASH_CONFIRMED", "RISK_ON", state=InfiniteState(),
        position=PositionSnapshot(price=50),
    )
    assert effective == "DEFENSE_CRASH_CONFIRMED" and decision.action == Action.BLOCK

    effective, reserve, decision = _conflict_decision(
        "DEFENSE_CRASH_REBOUND", "DEFENSIVE", state=defensive_state,
        position=PositionSnapshot(qty=5, orderable_qty=5, average_price=100, price=94),
    )
    assert (effective, reserve, decision.reason) == ("DEFENSIVE", False, "defense_risk_off_wait")
