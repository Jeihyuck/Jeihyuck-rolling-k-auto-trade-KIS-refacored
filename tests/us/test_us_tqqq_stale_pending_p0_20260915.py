from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl, run_sleeve
from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
from trader.us.infinite.repository import InfiniteRepository
from trader.us.infinite.ttl_reconcile import fetch_fresh_tqqq_preorder_position


NOW = datetime(2026, 9, 15, 2, 0, tzinfo=timezone.utc)


def _order(*, meta=None, qty=3, trade_date="2026-09-02"):
    return {
        "trade_date": trade_date,
        "client_order_key": f"TQQQ_INF_V3:cycle-stale:{trade_date}:BUY",
        "order_no": "0000000018",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": qty,
        "status": "OPEN",
        "meta": dict(meta or {}),
    }


def _zero_fill_observation(*, status="OPEN", filled=0, remaining=3):
    return {
        "order_no": "0000000018", "symbol": "TQQQ", "side": "BUY",
        "requested_qty": 3, "filled_qty": filled, "cumulative_filled_qty": filled,
        "remaining_qty": remaining, "status": status,
    }


class TTLRepo:
    def __init__(self, order):
        self.order = order
        self.cancel_marks = []
        self.escalations = []
        self.balance_marks = []
        self.terminal_observations = []

    def load_expired_open_buy_orders(self, **_kwargs):
        return [self.order]

    def mark_ttl_unresolved_escalated(self, order, *, escalated_at, unresolved_age_sec):
        order.setdefault("meta", {})["tqqq_ttl_unresolved_escalated_at"] = escalated_at.isoformat()
        order["meta"]["manual_reconcile_required"] = True
        self.escalations.append((escalated_at, unresolved_age_sec))

    def apply_ttl_terminal_observation(self, order, observation):
        order["status"] = observation["status"]
        order["qty_filled"] = int(observation.get("filled_qty") or 0)
        self.terminal_observations.append(dict(observation))
        return {"status": "OK"}

    def apply_ttl_balance_delta(self, order, *, current_qty, current_avg_price, observed_at):
        pre = int(order["meta"]["pre_order_holding_qty"])
        requested = int(order["qty_requested"])
        filled = current_qty - pre
        order["status"] = "FILLED" if filled == requested else "PARTIALLY_FILLED"
        order["qty_filled"] = filled
        self.balance_marks.append({"filled": filled, "current_qty": current_qty, "current_avg_price": current_avg_price})
        return {"status": "OK", "order_status": order["status"], "qty_filled": filled}


def test_cancel_exception_starts_unresolved_clock_and_does_not_replay_cancel():
    repo = TTLRepo(_order())
    cancel_calls = []

    def cancel(**_kwargs):
        cancel_calls.append(1)
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    # Missing requested/remaining quantity is intentionally insufficient for
    # the stronger historical-expiry proof, so this remains pending.
    query = lambda **_kwargs: {"order_no": "0000000018", "symbol": "TQQQ", "side": "BUY", "status": "OPEN"}

    first = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120, cancel_order=cancel, query_order=query,
    )
    assert first["pending"] == 1
    assert first["cancel_attempted"] == 1
    assert len(cancel_calls) == 1
    assert repo.order["meta"].get("tqqq_ttl_first_unresolved_at")
    assert repo.order["meta"].get("tqqq_ttl_cancel_requested_at")
    assert repo.order["meta"]["tqqq_ttl_cancel_result"]["status"] == "ERROR"

    second = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW + timedelta(minutes=5), ttl_seconds=120,
        cancel_order=cancel, query_order=query,
    )
    assert second["pending"] == 1
    assert second.get("cancel_attempted", 0) == 0
    assert len(cancel_calls) == 1, "failed cancel must not be hammered every tick"


def test_sep02_style_double_zero_fill_plus_original_order_not_found_expires_safely():
    repo = TTLRepo(_order())
    query_calls = []

    def query(**_kwargs):
        query_calls.append(1)
        return _zero_fill_observation()

    def cancel(**_kwargs):
        raise RuntimeError("모의투자 원주문번호가 존재하지 않습니다")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120, cancel_order=cancel, query_order=query,
    )
    assert len(query_calls) == 2, "original-date broker truth must be proven before and after cancel failure"
    assert repo.order["status"] == "EXPIRED"
    assert repo.order["qty_filled"] == 0
    assert result["terminal"] == 1
    assert result["historical_expired"] == 1
    assert result["pending"] == 0
    evidence = repo.terminal_observations[-1]
    assert evidence["evidence_type"] == "TQQQ_TTL_HISTORICAL_ZERO_FILL_NOT_LIVE"
    assert evidence["remaining_qty"] == 3


def test_generic_http500_never_uses_historical_expiry_even_with_two_zero_fill_queries():
    repo = TTLRepo(_order())
    query_calls = []

    def query(**_kwargs):
        query_calls.append(1)
        return _zero_fill_observation()

    def cancel(**_kwargs):
        raise RuntimeError("HTTP 500 Internal Server Error")

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120, cancel_order=cancel, query_order=query,
    )
    assert len(query_calls) == 2
    assert repo.order["status"] == "OPEN"
    assert result["terminal"] == 0
    assert result.get("historical_expired", 0) == 0
    assert result["pending"] == 1


def test_same_day_zero_fill_not_found_is_not_force_expired():
    repo = TTLRepo(_order(trade_date="2026-09-15"))

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("모의투자 원주문번호가 존재하지 않습니다")),
        query_order=lambda **_kwargs: _zero_fill_observation(),
    )
    assert repo.order["status"] == "OPEN"
    assert result["terminal"] == 0
    assert result.get("historical_expired", 0) == 0
    assert result["pending"] == 1


def test_changed_retry_evidence_blocks_historical_expiry():
    repo = TTLRepo(_order())
    observations = [
        _zero_fill_observation(),
        _zero_fill_observation(status="PARTIALLY_FILLED", filled=1, remaining=2),
    ]

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("모의투자 원주문번호가 존재하지 않습니다")),
        query_order=lambda **_kwargs: observations.pop(0),
    )
    # The changed retry row is not one of this reconciler's terminal statuses;
    # more importantly, it must never be converted to zero-fill EXPIRED.
    assert repo.order["status"] == "OPEN"
    assert result.get("historical_expired", 0) == 0
    assert result["pending"] == 1


def test_query_failure_starts_clock_and_escalates_without_inventing_terminal():
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
    assert repo.order["meta"]["manual_reconcile_reason"] == "TQQQ_TTL_UNRESOLVED"


def _baseline_order():
    return _order(meta={
        "pre_order_holding_qty": 24,
        "pre_order_orderable_qty": 24,
        "pre_order_avg_price": 71.0,
        "pre_order_balance_source": "kis_balance_authoritative",
        "pre_order_balance_asof": "2026-09-02T13:30:00+00:00",
    })


def test_authoritative_kis_balance_delta_full_fill_skips_cancel():
    repo = TTLRepo(_baseline_order())
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **_kwargs: pytest.fail("full balance proof must resolve before cancel"),
        query_order=lambda **_kwargs: {},
        broker_position=PositionSnapshot(qty=27, orderable_qty=27, average_price=72.0, price=72.0),
        broker_position_authoritative=True,
    )
    assert repo.order["status"] == "FILLED"
    assert repo.order["qty_filled"] == 3
    assert len(repo.balance_marks) == 1
    assert result["balance_confirmed"] == 1
    assert result["terminal"] == 1
    assert result.get("cancel_attempted", 0) == 0


def test_authoritative_kis_balance_delta_partial_fill_cancels_only_remainder_once():
    repo = TTLRepo(_baseline_order())
    cancel_calls = []
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=120,
        cancel_order=lambda **kwargs: cancel_calls.append(kwargs) or {"status": "ACK"},
        query_order=lambda **_kwargs: {},
        broker_position=PositionSnapshot(qty=26, orderable_qty=26, average_price=72.0, price=72.0),
        broker_position_authoritative=True,
    )
    assert repo.order["status"] == "PARTIALLY_FILLED"
    assert repo.order["qty_filled"] == 2
    assert len(repo.balance_marks) == 1
    assert len(cancel_calls) == 1
    assert result["balance_confirmed"] == 1
    assert result["pending"] == 1
    assert result["cancel_requested"] == 1


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
    assert result.get("balance_confirmed", 0) == 0
    assert result["cancel_requested"] == 1


def test_fresh_preorder_balance_accepts_authoritative_zero_tqqq_holding(monkeypatch):
    class Provider:
        def __init__(self, offline=False):
            assert offline is False
        def get_balance(self, force_refresh=False):
            assert force_refresh is True
            return {
                "balance_parse_status": "OK",
                "balance_authoritative": True,
                "balance_complete": True,
                "positions": [],
            }

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", Provider)
    result = fetch_fresh_tqqq_preorder_position()
    assert result["authoritative"] is True
    assert result["position"].qty == 0
    assert result["position"].orderable_qty == 0
    assert result["position"].average_price == 0


def test_fresh_preorder_balance_rejects_unproven_offline_or_partial_snapshot(monkeypatch):
    class Provider:
        def __init__(self, offline=False): pass
        def get_balance(self, force_refresh=False):
            return {"balance_parse_status": "OK", "positions": []}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", Provider)
    result = fetch_fresh_tqqq_preorder_position()
    assert result["authoritative"] is False
    assert result["reason"] == "fresh_kis_balance_not_authoritative"


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


class ProductionLikeRepo(InfiniteRepository):
    """Real repository type boundary without a live DB for BUY-baseline tests."""
    def __init__(self):
        self.state = InfiniteState(
            symbol="TQQQ", cycle_id="cycle-prod", cycle_start_date=date(2026, 9, 15),
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


class BuyDecision:
    action = Action.BUY
    qty = 3
    notional = 210.0
    reason = "BUY_DIP_CORE"
    metadata = {}
    next_status = Status.ACTIVE


def test_tqqq_buy_intent_persists_authoritative_preorder_kis_baseline(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    repo = SleeveRepo()
    captured = []

    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: BuyDecision())
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


def test_production_tqqq_first_buy_uses_fresh_authoritative_zero_baseline(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    repo = ProductionLikeRepo()
    captured = []
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: BuyDecision())
    monkeypatch.setattr(
        "trader.us.infinite.integration.fetch_fresh_tqqq_preorder_position",
        lambda: {
            "authoritative": True, "reason": "ok",
            "position": PositionSnapshot(qty=0, orderable_qty=0, average_price=0.0, price=0.0, exchange="NASDAQ"),
            "observed_at": "2026-09-15T02:00:00+00:00",
        },
    )
    result = run_sleeve(
        positions=[], price=70.0, trading_date=date(2026, 9, 15), overlay=_overlay(),
        repository=repo, route=lambda intent: captured.append(intent) or {"status": "ACK"},
    )
    assert result["status"] == "ACK"
    assert captured[0]["pre_order_holding_qty"] == 0
    assert captured[0]["pre_order_orderable_qty"] == 0
    assert captured[0]["pre_order_balance_source"] == "kis_balance_authoritative"


def test_production_tqqq_buy_blocks_when_fresh_qty_changed_since_decision(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    repo = ProductionLikeRepo()
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: BuyDecision())
    monkeypatch.setattr(
        "trader.us.infinite.integration.fetch_fresh_tqqq_preorder_position",
        lambda: {
            "authoritative": True, "reason": "ok",
            "position": PositionSnapshot(qty=25, orderable_qty=25, average_price=71.0, price=70.0, exchange="NASDAQ"),
            "observed_at": "2026-09-15T02:00:00+00:00",
        },
    )
    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 24, "orderable_qty": 24, "avg_cost": 71.0}],
        price=70.0, trading_date=date(2026, 9, 15), overlay=_overlay(), repository=repo,
        route=lambda _intent: pytest.fail("stale strategy snapshot must not submit BUY"),
    )
    assert result["status"] == "BLOCK"
    assert result["reason"] == "preorder_balance_position_changed"


def test_tqqq_buy_fails_closed_if_only_db_position_snapshot_is_available(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    repo = SleeveRepo()

    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: BuyDecision())
    strict_overlay = {**_overlay(), "enforce_authoritative_preorder_balance": True}
    result = run_sleeve(
        positions=[{
            "symbol": "TQQQ", "exchange": "NASDAQ", "qty": 24,
            "orderable_qty": 24, "avg_cost": 71.0, "balance_source": "kis_balance_authoritative",
        }],
        price=70.0, trading_date=date(2026, 9, 15), overlay=strict_overlay,
        repository=repo, route=lambda _intent: pytest.fail("BUY must not route without live KIS baseline"),
    )
    assert result["status"] == "BLOCK"
    assert result["reason"] == "authoritative_preorder_balance_unavailable"
