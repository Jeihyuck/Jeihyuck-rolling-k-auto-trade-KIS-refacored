from datetime import date

from trader.us.execution.order_router import route_order
import pytest

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.integration import exclude_owned
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot
from trader.us.infinite.policy_state import update_adaptive_policy_state
from trader.us.infinite.risk_adapter import effective_regime
from trader.us.infinite.strategy import evaluate
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_router import route_exit_by_book_horizon


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
    assert (effective, multiplier, reserve, allowed) == ("DEFENSIVE", 0.0, False, False)
    config = InfiniteConfig(unit_usd=1000, max_daily_buy_usd=1000)
    blocked = evaluate(config=config, state=InfiniteState(), position=PositionSnapshot(price=100),
                       trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
                       entry_allowed=allowed, buy_multiplier=multiplier)
    assert (blocked.action, blocked.reason) == (Action.BLOCK, "tqqq_effective_regime_entry_block")
    sold = evaluate(config=config, state=InfiniteState(cycle_id="c", core_filled_notional=100),
                    position=PositionSnapshot(qty=1, average_price=100, price=110),
                    trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
                    entry_allowed=False, buy_multiplier=0)
    assert sold.action == Action.SELL


def test_strong_risk_on_multiplier_changes_real_order_size():
    regime, multiplier, _reserve, allowed, _ = effective_regime(
        {"market_state": "STRONG_RISK_ON", "market_regime": "STRONG_RISK_ON"}
    )
    assert (regime, multiplier, allowed) == ("STRONG_RISK_ON", 1.25, True)
    decision = evaluate(
        config=InfiniteConfig(unit_usd=400, max_daily_buy_usd=1000), state=InfiniteState(),
        position=PositionSnapshot(price=100), trading_date=date(2026, 8, 14),
        overlay={"market_state": "STRONG_RISK_ON"}, entry_allowed=allowed,
        buy_multiplier=multiplier,
    )
    assert (decision.action, decision.qty, decision.notional) == (Action.BUY, 5, 500)


@pytest.mark.parametrize("regime", ["CRASH", "DEFENSE_CRASH", "CAPITAL_PRESERVATION"])
def test_fail_closed_effective_regimes_block_new_buy(regime):
    _effective, multiplier, _reserve, allowed, _reason = effective_regime(
        {"market_state": regime, "market_regime": regime}
    )
    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(), position=PositionSnapshot(price=100),
        trading_date=date(2026, 8, 14), overlay={"market_state": "STRONG_RISK_ON"},
        entry_allowed=allowed, buy_multiplier=multiplier,
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
                  overlay={"market_state": "STRONG_RISK_ON"})
    assert evaluate(state=locked, regime_reserve_permission=True, **common).reason == "reserve_locked"
    unlocked = InfiniteState(cycle_id="c", core_filled_notional=100, reserve_unlocked=True,
                             material_market_crash=True)
    assert evaluate(state=unlocked, regime_reserve_permission=False, **common).reason == "reserve_locked"
    assert evaluate(state=unlocked, regime_reserve_permission=True, **common).action == Action.BUY
    sell = evaluate(
        config=cfg, state=unlocked, position=PositionSnapshot(qty=1, average_price=100, price=110),
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
