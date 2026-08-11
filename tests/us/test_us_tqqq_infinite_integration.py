from __future__ import annotations

from datetime import date

from trader.us.infinite.integration import exclude_owned, run_sleeve
from trader.us.infinite.models import InfiniteState
from trader.us.infinite.repository import InfiniteRepository


class FakeRepository:
    def __init__(self, state=None):
        self.state = state
        self.schema_calls = self.loads = self.saves = 0

    def ensure_schema(self): self.schema_calls += 1
    def load_state(self, **_): self.loads += 1; return self.state
    def save_state(self, state): self.saves += 1; self.state = state
    def pending_sides(self, *_): return False, False
    def fill_accounting(self, *_): return 0, 0, 0, None, None
    def reconcile_metadata(self, state, **_): return state


def test_off_is_strict_no_db_no_router(monkeypatch):
    monkeypatch.delenv("US_TQQQ_INFINITE_ENABLED", raising=False)
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
                        overlay={"market_state": "NORMAL"}, repository=repo, route=routed.append)
    assert result["status"] == "SHADOW"
    assert result["decision"].action.value == "BUY"
    assert routed == []


def test_order_mode_uses_injected_existing_router_once(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    routed = []
    def route(intent): routed.append(intent); return {"status": "ACK", "intent": intent}
    result = run_sleeve(positions=[], price=50, trading_date=date(2026, 8, 11),
                        overlay={"market_state": "NORMAL"}, repository=FakeRepository(InfiniteState()), route=route)
    assert result["status"] == "ACK"
    assert len(routed) == 1
    assert routed[0]["client_order_key"].startswith("TQQQ_INF_V3:")
    assert routed[0]["client_order_key"].endswith(":2026-08-11:BUY")
    assert routed[0]["meta"]["cycle_id"]


def test_enabled_ownership_filter_is_central_and_off_preserves_identity(monkeypatch):
    rows = [{"symbol": "AAPL"}, {"symbol": "TQQQ"}]
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "0")
    assert exclude_owned(rows) is rows
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


def test_reserved_cycle_does_not_adopt_unattributed_broker_position(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "0")
    state = InfiniteState(cycle_id="reserved", cycle_start_date=date(2026, 8, 11))
    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 2, "avg_price": 50, "current_price": 50}],
        price=50, trading_date=date(2026, 8, 11), overlay={"market_state": "NORMAL"},
        repository=FakeRepository(state),
    )
    assert result["decision"].reason == "orphan_position"
    assert result["orders"] == []
