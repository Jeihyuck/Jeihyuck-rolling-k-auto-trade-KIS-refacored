from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl, run_sleeve
from trader.us.infinite.models import InfiniteState, PositionSnapshot, Status


NOW = datetime(2026, 9, 15, 2, 0, tzinfo=timezone.utc)


def _order(*, meta=None, qty=3):
    return {
        "trade_date": "2026-09-02",
        "client_order_key": "TQQQ_INF_V3:cycle-stale:2026-09-02:BUY",
        "order_no": "0000000018",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": qty,
        "status": "OPEN",
        "meta": dict(meta or {}),
    }


class TTLRepo:
    def __init__(self, order):
        self.order = order
        self.cancel_marks = []
        self.escalations = []
        self.balance_marks = []
        self.first_unresolved = []

    def load_expired_open_buy_orders(self, **_kwargs):
        return [self.order]

    def mark_ttl_cancel_requested(self, order, *, requested_at, cancel_result=None):
        order.setdefault("meta", {})["tqqq_ttl_cancel_requested_at"] = requested_at.isoformat()
        order["meta"]["tqqq_ttl_cancel_result"] = cancel_result or {}
        self.cancel_marks.append((requested_at, cancel_result or {}))

    def mark_ttl_first_unresolved(self, order, *, first_unresolved_at, reason):
        order.setdefault("meta", {}).setdefault("tqqq_ttl_first_unresolved_at", first_unresolved_at.isoformat())
        order["meta"]["tqqq_ttl_last_unresolved_reason"] = reason
        self.first_unresolved.append((first_unresolved_at, reason))

    def mark_ttl_unresolved_escalated(self, order, *, escalated_at, unresolved_age_sec):
        order.setdefault("meta", {})["tqqq_ttl_unresolved_escalated_at"] = escalated_at.isoformat()
        order["meta"]["manual_reconcile_required"] = True
        self.escalations.append((escalated_at, unresolved_age_sec))

    def apply_ttl_terminal_observation(self, order, observation):
        order["status"] = observation["status"]
        return {"status": "OK"}

    def apply_ttl_balance_delta(self, order, *, current_qty, current_avg_price, observed_at):
        pre = int(order["meta"]["pre_order_holding_qty"])
        requested = int(order["qty_requested"])
        filled = current_qty - pre
        order["status"] = "FILLED" if filled == requested else "PARTIALLY_FILLED"
        order["qty_filled"] = filled
        self.balance_marks.append({"filled": filled, "current_qty": current_qty, "current_avg_price": current_avg_price})
        return {"status": "OK", "order_status": order["status"], "qty_filled": filled}


def test_cancel_exception_starts_unresolved_clock_and_does_not_replay_cancel(monkeypatch):
    repo = TTLRepo(_order())
    cancel_calls = []

    def cancel(**_kwargs):
        cancel_calls.append(1)
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    query = lambda **_kwargs: {"order_no": "0000000018", "symbol": "TQQQ", "side": "BUY", "status": "OPEN"}

    first = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120, cancel_order=cancel, query_order=query,
    )
    assert first["pending"] == 1
    assert len(cancel_calls) == 1
    assert repo.order["meta"].get("tqqq_ttl_first_unresolved_at")
    assert repo.order["meta"].get("tqqq_ttl_cancel_requested_at")
    assert repo.order["meta"]["tqqq_ttl_cancel_result"]["status"] == "ERROR"

    second = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW + timedelta(minutes=5), ttl_seconds=120,
        cancel_order=cancel, query_order=query,
    )
    assert second["pending"] == 1
    assert len(cancel_calls) == 1, "failed cancel must not be hammered every tick"


def test_query_failure_starts_clock_and_escalates_without_inventing_terminal(monkeypatch):
    first_unresolved = NOW - timedelta(minutes=16)
    repo = TTLRepo(_order(meta={"tqqq_ttl_first_unresolved_at": first_unresolved.isoformat()}))

    def query(**_kwargs):
        raise RuntimeError("KIS fills HTTP 500")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: pytest.fail("cancel must not run when broker query itself is unavailable"),
        query_order=query,
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert result["escalated"] == 1
    assert repo.order["status"] == "OPEN"
    assert repo.order["meta"]["manual_reconcile_required"] is True


@pytest.mark.parametrize(
    "post_qty,expected_status,expected_filled",
    [(27, "FILLED", 3), (26, "PARTIALLY_FILLED", 2)],
)
def test_authoritative_kis_balance_delta_resolves_full_or_partial(post_qty, expected_status, expected_filled):
    repo = TTLRepo(_order(meta={
        "pre_order_holding_qty": 24,
        "pre_order_orderable_qty": 24,
        "pre_order_avg_price": 71.0,
        "pre_order_balance_source": "kis_balance_authoritative",
        "pre_order_balance_asof": "2026-09-02T13:30:00+00:00",
    }))
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: pytest.fail("balance truth should resolve before cancel"),
        query_order=lambda **_kwargs: {},
        broker_position=PositionSnapshot(qty=post_qty, orderable_qty=post_qty, average_price=72.0, price=72.0),
        broker_position_authoritative=True,
    )
    assert repo.order["status"] == expected_status
    assert repo.order["qty_filled"] == expected_filled
    assert len(repo.balance_marks) == 1
    assert result["balance_confirmed"] == 1


def test_balance_delta_without_immutable_kis_baseline_never_guesses_fill():
    repo = TTLRepo(_order(meta={"pre_order_position_qty": 24, "pre_order_position_source": "db_position"}))
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: {"status": "ACK"},
        query_order=lambda **_kwargs: {},
        broker_position=PositionSnapshot(qty=27, orderable_qty=27, average_price=72.0, price=72.0),
        broker_position_authoritative=True,
    )
    assert repo.balance_marks == []
    assert repo.order["status"] == "OPEN"
    assert result["balance_confirmed"] == 0


class SleeveRepo:
    def __init__(self):
        self.state = InfiniteState(
            symbol="TQQQ", cycle_id="cycle-buy", cycle_start_date=date(2026, 9, 15),
            anchor_price=70.0, core_filled_notional=1000.0, status=Status.ACTIVE,
            metadata={"strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE"},
        )

    def ensure_schema(self): pass
    def load_state(self, **_kwargs): return self.state
    def save_state(self, state): self.state = state
    def reconcile_metadata(self, state, **_kwargs): return state
    def load_open_orders(self, **_kwargs): return []
    def pending_buy_notional(self, *_args, **_kwargs): return 0.0
    def fill_accounting(self, *_args, **_kwargs): return (1000.0, 0.0, 0.0, None, 70.0)
    def next_full_exit_sequence(self, *_args, **_kwargs): return 1


def _overlay():
    return {
        "market_state": "NORMAL", "market_regime": "NORMAL", "base_market_state": "NORMAL",
        "tqqq_context_quality": "ok", "qqq_completed_close": 100.0, "qqq_ma50": 99.0,
        "qqq_ma200": 98.0, "qqq_ma200_slope": 0.1, "qqq_20d_return": 0.02,
        "qqq_drawdown_252": -0.05, "qqq_realized_vol_20d": 0.2,
        "qqq_trend_efficiency_20d": 0.5, "entry_can_proceed": True,
    }


def test_tqqq_buy_intent_persists_authoritative_preorder_kis_baseline(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    repo = SleeveRepo()
    captured = []

    class Decision:
        action = type("A", (), {"value": "BUY"})()
        qty = 3
        notional = 210.0
        reason = "BUY_DIP_CORE"
        metadata = {}
        next_status = Status.ACTIVE

    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: Decision())
    result = run_sleeve(
        positions=[{
            "symbol": "TQQQ", "exchange": "NASDAQ", "qty": 24, "holding_qty": 24,
            "orderable_qty": 24, "avg_price_usd": 71.0, "current_price_usd": 70.0,
            "authoritative_positions": True, "balance_source": "kis_balance_authoritative",
            "broker_avg_price_asof": "2026-09-15T01:59:00+00:00",
        }],
        price=70.0, trading_date=date(2026, 9, 15), overlay=_overlay(),
        repository=repo, route=lambda intent: captured.append(intent) or {"status": "ACK"},
    )
    assert result["status"] == "ACK"
    intent = captured[0]
    assert intent["pre_order_holding_qty"] == 24
    assert intent["pre_order_orderable_qty"] == 24
    assert intent["pre_order_avg_price"] == 71.0
    assert intent["pre_order_balance_source"] == "kis_balance_authoritative"
    assert intent["meta"]["pre_order_holding_qty"] == 24
    assert intent["meta"]["pre_order_balance_source"] == "kis_balance_authoritative"
