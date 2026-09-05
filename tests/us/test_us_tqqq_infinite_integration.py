from __future__ import annotations

from datetime import date

from trader.us.infinite.config import InfiniteConfig
from trader.us.infinite.integration import exclude_owned, legacy_ownership_reserved, run_sleeve
from trader.us.infinite.models import InfiniteState, Status
from trader.us.infinite.repository import InfiniteRepository
from trader.us.execution.order_router import resolve_entry_metadata_contract_reason


def market(state="NORMAL", **extra):
    return {"market_state": state, "tqqq_context_quality": "ok",
            "qqq_completed_close": 100, "qqq_ma50": 99, "qqq_ma200": 98,
            "qqq_ma200_slope": .1, "qqq_20d_return": .02, "qqq_drawdown_252": -.05,
            "qqq_realized_vol_20d": .2, "qqq_trend_efficiency_20d": .5, **extra}


class FakeRepository:
    def __init__(self, state=None):
        self.state = state
        self.schema_calls = self.loads = self.saves = 0

    def ensure_schema(self): self.schema_calls += 1
    def load_state(self, **_): self.loads += 1; return self.state
    def save_state(self, state): self.saves += 1; self.state = state
    def pending_sides(self, *_): return False, False
    def has_pending_infinite_order(self, *_): return False
    def fill_accounting(self, *_): return 0, 0, 0, None, None
    def reconcile_metadata(self, state, **_): return state


def test_explicit_kill_is_strict_no_db_no_router(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    repo = FakeRepository()
    routed = []
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11), overlay={},
                        repository=repo, route=routed.append)
    assert result == {"status": "OFF", "orders": []}
    assert repo.schema_calls == repo.loads == repo.saves == 0
    assert routed == []


def test_shadow_computes_but_submits_zero_orders(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "0")
    repo = FakeRepository(InfiniteState())
    routed = []
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                        overlay=market("NORMAL"), repository=repo, route=routed.append)
    assert result["status"] == "SHADOW"
    assert result["decision"].action.value == "BUY"
    assert routed == []


def test_tqqq_run_sleeve_reaches_strategy_evaluate_after_open_order_check(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    repo = FakeRepository(InfiniteState(cycle_id="cycle-1"))
    seen = []
    repo.load_open_orders = lambda **kwargs: (seen.append(kwargs) or [])
    monkeypatch.setattr(
        "trader.us.infinite.integration.evaluate",
        lambda **kwargs: (seen.append("evaluate") or type("Decision", (), {
            "action": type("Action", (), {"value": "HOLD"})(), "metadata": {},
            "notional": 0, "reason": "test", "qty": 0,
        })()),
    )
    run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
               overlay=market(), repository=repo, route=lambda _intent: None)
    assert seen == [{"symbol": "TQQQ", "cycle_id": "cycle-1"}, "evaluate"]


def test_tqqq_open_order_check_does_not_isolated_exception(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    repo = FakeRepository(InfiniteState(cycle_id="cycle-1"))
    repo.load_open_orders = lambda **kwargs: []
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                        overlay=market(), repository=repo, route=lambda _intent: {"status": "ACK"})
    assert result["decision"] is not None


def test_order_mode_uses_injected_existing_router_once(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    routed = []
    def route(intent): routed.append(intent); return {"status": "ACK", "intent": intent}
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                        overlay=market("NORMAL"), repository=FakeRepository(InfiniteState()), route=route)
    assert result["status"] == "ACK"
    assert len(routed) == 1
    assert routed[0]["client_order_key"].startswith("TQQQ_INF_V3:")
    assert routed[0]["client_order_key"].endswith(":2026-08-11:BUY")
    assert routed[0]["meta"]["cycle_id"]


def test_invalid_or_stale_quote_never_reaches_router(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    for price, extra in ((0, {}), (float("nan"), {}), (50, {"tqqq_quote_stale": True})):
        routed = []
        result = run_sleeve(positions=[], price=price, trading_date=date(2026, 8, 11),
                            overlay=market("NORMAL", **extra),
                            repository=FakeRepository(InfiniteState()), route=routed.append)
        assert result["reason"] == "tqqq_quote_invalid"
        assert routed == []


def test_runner_marks_db_fallback_stale_date_and_degraded_quotes_unusable():
    from trader.us.runner.trade_tick_runner import _get_tqqq_tick_quote

    class Provider:
        def __init__(self, quote): self.quote = quote
        def get_current_price(self, symbol, exchange):
            assert (symbol, exchange) == ("TQQQ", "NASDAQ")
            return self.quote

    assert _get_tqqq_tick_quote(Provider({"last": "50.00"})) == (50.0, "USDataProvider", False)
    for quote in (
        {"last": "50.00", "_stale_date": "2026-08-08"},
        {"last": "50.00", "stale": True},
        {"last": "50.00", "suspect": True},
        {"last": "50.00", "quality": "degraded"},
    ):
        price, _source, stale = _get_tqqq_tick_quote(Provider(quote))
        assert price == 50.0 and stale is True


def test_production_router_metadata_contract_for_new_add_and_sell(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")

    def capture(positions, price, state):
        intents = []
        result = run_sleeve(positions=positions, price=price, trading_date=date(2026, 8, 11),
                            overlay=market("NORMAL"), repository=FakeRepository(state),
                            route=lambda intent: (intents.append(intent) or {"status": "ACK"}))
        assert result["orders"]
        assert resolve_entry_metadata_contract_reason(intents[0], required=True) is None
        for key in ("theme_cluster", "classification_source", "position_state", "position_action"):
            assert intents[0][key] == intents[0]["meta"][key]
        return intents[0]

    new = capture([], 50, InfiniteState())
    assert new["position_action"] == "NEW_POSITION_BUY" and new["position_state"] == "NOT_HELD"
    owned = InfiniteState(cycle_id="owned", cycle_start_date=date(2026, 8, 1),
                          core_filled_notional=250)
    add = capture([{"symbol": "TQQQ", "qty": 2, "avg_price": 50}], 50, owned)
    assert add["position_action"] == "ADD_TO_EXISTING_BUY" and add["position_state"] == "HELD"
    sell = capture([{"symbol": "TQQQ", "qty": 2, "orderable_qty": 2, "avg_price": 50}], 55, owned)
    assert sell["side"] == "SELL" and sell["theme_cluster"] == "ETF_INDEX"


def test_rebound_decision_block_reject_or_ack_does_not_consume_probe(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    for router_status in ("BLOCK", "REJECT", "ACK"):
        state = InfiniteState(cycle_id="owned", cycle_start_date=date(2026, 8, 1),
                              core_filled_notional=250)
        repo = FakeRepository(state)
        result = run_sleeve(
            positions=[{"symbol": "TQQQ", "qty": 2, "avg_price": 50}], price=50,
            trading_date=date(2026, 8, 11), overlay=market("DEFENSE_CRASH_REBOUND"),
            repository=repo, route=lambda _intent: {"status": router_status},
        )
        assert result["decision"].action.value == "BUY"
        assert "rebound_probe_date" not in repo.state.metadata


def test_rebound_tick_alone_never_unlocks_recovery_reserve(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "0")
    state = InfiniteState(cycle_id="owned", cycle_start_date=date(2026, 8, 1),
                          core_filled_notional=7_500, material_market_crash=True)
    repo = FakeRepository(state)
    run_sleeve(positions=[{"symbol": "TQQQ", "qty": 2, "avg_price": 50}], price=50,
               trading_date=date(2026, 8, 11), overlay=market("DEFENSE_CRASH_REBOUND"),
               repository=repo)
    assert not repo.state.reserve_unlocked


def test_ownership_filter_is_invariant_even_when_sleeve_is_off(monkeypatch):
    rows = [{"symbol": "AAPL"}, {"symbol": "TQQQ"}]
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    assert exclude_owned(rows) == [{"symbol": "AAPL"}]
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    assert exclude_owned(rows) == [{"symbol": "AAPL"}]


def test_repository_or_schema_exception_is_isolated(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    repo = FakeRepository()
    repo.ensure_schema = lambda: (_ for _ in ()).throw(RuntimeError("state table missing"))
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11), overlay={}, repository=repo)
    assert result["status"] == "BLOCK" and result["orders"] == []


def test_runtime_schema_probe_never_runs_migrations(monkeypatch):
    calls = []
    monkeypatch.setattr("trader.db.migrate.run_migrations", lambda *_: calls.append(1))

    class Result:
        def scalar(self): return "us_tqqq_infinite_state"
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_): return None
        def execute(self, *_): return Result()
    class Engine:
        def connect(self): return Connection()

    repo = InfiniteRepository(Engine())
    repo.ensure_schema()
    repo.ensure_schema()
    assert calls == []


def test_reserved_cycle_recovers_unattributed_broker_position_by_symbol_invariant(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "0")
    state = InfiniteState(cycle_id="reserved", cycle_start_date=date(2026, 8, 11))
    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 2, "avg_price": 50, "current_price": 50}],
        price=50, trading_date=date(2026, 8, 11), overlay=market("NORMAL"),
        repository=FakeRepository(state),
    )
    assert result["decision"].reason != "orphan_position"
    assert result["orders"] == []


def test_production_defaults_are_normal_mode(monkeypatch):
    for suffix in ("ENABLED", "REAL_ORDER", "ALLOW_BUY", "ALLOW_SELL"):
        monkeypatch.delenv(f"US_TQQQ_INFINITE_{suffix}", raising=False)
    config = InfiniteConfig.from_env()
    assert config.enabled and config.real_order and config.allow_buy and config.allow_sell


def test_explicit_environment_zero_overrides_default_on(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "0")
    config = InfiniteConfig.from_env()
    assert not config.enabled and not config.real_order


def test_disabled_open_infinite_position_remains_reserved_from_legacy(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    state = InfiniteState(cycle_id="owned", core_filled_notional=250)
    reserved = legacy_ownership_reserved(
        positions=[{"symbol": "TQQQ", "qty": 2}], repository=FakeRepository(state)
    )
    assert reserved
    assert exclude_owned([{"symbol": "TQQQ"}, {"symbol": "AAPL"}], reserved=reserved) == [{"symbol": "AAPL"}]


def test_disabled_pending_infinite_order_remains_reserved_from_legacy(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    repo = FakeRepository()
    repo.has_pending_infinite_order = lambda *_: True
    assert legacy_ownership_reserved(positions=[], repository=repo)


def test_default_on_missing_table_blocks_only_sleeve(monkeypatch):
    for suffix in ("ENABLED", "REAL_ORDER"):
        monkeypatch.delenv(f"US_TQQQ_INFINITE_{suffix}", raising=False)
    repo = FakeRepository()
    repo.ensure_schema = lambda: (_ for _ in ()).throw(RuntimeError("state table missing"))
    routed = []
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                        overlay=market("NORMAL"), repository=repo, route=routed.append)
    assert result["status"] == "BLOCK" and routed == []


def test_pause_reconciles_blocks_buy_and_routes_existing_exit(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_ALLOW_BUY", "0")
    monkeypatch.setenv("US_TQQQ_INFINITE_ALLOW_SELL", "1")
    buy_repo = FakeRepository(InfiniteState())
    buy = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                     overlay=market("NORMAL"), repository=buy_repo)
    assert buy["decision"].reason == "buy_permission_paused"
    assert buy_repo.saves > 0  # state/reconcile lifecycle remains active

    owned = InfiniteState(cycle_id="owned", cycle_start_date=date(2026, 8, 1),
                          core_filled_notional=250)
    routed = []
    def route(intent): routed.append(intent); return {"status": "ACK", "intent": intent}
    sell = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 3, "orderable_qty": 3, "avg_price": 50, "current_price": 55}],
        price=55, trading_date=date(2026, 8, 11), overlay=market("NORMAL"),
        repository=FakeRepository(owned), route=route,
    )
    assert sell["decision"].action.value == "SELL"


def test_cancelled_full_exit_uses_deterministic_retry_key(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    state = InfiniteState(cycle_id="exit-cycle", status=Status.EXIT_PENDING,
                          core_filled_notional=500)
    routed = []
    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 4, "orderable_qty": 4, "avg_price": 50}],
        price=55, trading_date=date(2026, 8, 11), overlay={"market_state": "NORMAL"},
        repository=FakeRepository(state),
        route=lambda intent: (routed.append(intent) or {"status": "ACK"}),
    )
    assert result["decision"].action.value == "SELL"
    assert routed[0]["client_order_key"] == "TQQQ_INF_V3:exit-cycle:2026-08-11:TP1:RETRY:1"
    assert len(routed) == 1
