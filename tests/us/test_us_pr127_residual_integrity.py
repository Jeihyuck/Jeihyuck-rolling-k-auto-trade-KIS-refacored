from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


def test_stale_quote_provenance_propagates_from_data_provider():
    from trader.us.data_provider import USDataProvider

    class Client:
        def get_us_price(self, symbol, exchange):
            return {
                "output": {"last": "100", "open": "99", "high": "101", "low": "98", "tvol": "10"},
                "_quote_quality": "STALE",
                "_quote_source": "KIS_STALE_CACHE",
                "_quote_asof_epoch": 123.0,
                "_quote_age_sec": 42.0,
            }

    provider = USDataProvider(offline=False)
    provider._client = Client()
    quote = provider.get_current_price("AAPL", "NASDAQ")
    assert quote["stale"] is True
    assert quote["quality"] == "stale"
    assert quote["source"] == "KIS_STALE_CACHE"
    assert quote["age_sec"] == 42.0


def test_pb1_exit_snapshot_rejects_stale_quote_before_state_mutation():
    from trader.us.pb1.us_exit_engine import prepare_exit_position_snapshots

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": "110", "stale": True, "quality": "stale", "source": "DB_STALE", "_stale_date": "2026-09-11"}

    snapshots = prepare_exit_position_snapshots(
        [{"symbol": "AAPL", "qty": 10, "avg_price_usd": 100, "exchange": "NASDAQ"}],
        Provider(),
        now=datetime(2026, 9, 12, tzinfo=timezone.utc),
    )
    assert snapshots == []


def test_sell_balance_snapshot_is_fetched_once_per_tick():
    from trader.us.execution.order_router import _get_broker_position
    from trader.us.execution.tick_context import TickExecutionContext

    class Client:
        def __init__(self):
            self.calls = 0
        def get_balance(self, force_refresh=False):
            self.calls += 1
            assert force_refresh is True
            return {"positions": [
                {"symbol": "AAPL", "qty": 10, "orderable_qty": 10},
                {"symbol": "MRVL", "qty": 15, "orderable_qty": 15},
            ]}

    ctx = TickExecutionContext("2026-09-11", "am", "run", 1, "tick")
    client = Client()
    assert _get_broker_position(client, "AAPL", context=ctx)["qty"] == 10
    assert _get_broker_position(client, "MRVL", context=ctx)["qty"] == 15
    assert client.calls == 1
    assert ctx.counters["sell_balance_fresh_snapshots"] == 1


def test_tqqq_unresolved_cancel_escalates_without_guessing_terminal(monkeypatch):
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    now = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
    order = {
        "trade_date": "2026-09-10",
        "order_no": "123",
        "client_order_key": "TQQQ_INF_V3:cycle:2026-09-10:BUY:1",
        "symbol": "TQQQ", "side": "BUY", "status": "ACK", "qty": 3,
        "meta": {"tqqq_ttl_cancel_requested_at": (now - timedelta(hours=1)).isoformat()},
    }

    class Repo:
        def __init__(self):
            self.escalations = []
        def load_expired_open_buy_orders(self, **kwargs):
            return [order]
        def mark_ttl_unresolved_escalated(self, order, **kwargs):
            self.escalations.append(kwargs)

    repo = Repo()
    monkeypatch.setenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900")
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=now, ttl_seconds=120,
        cancel_order=lambda **kwargs: (_ for _ in ()).throw(AssertionError("cancel must not replay")),
        query_order=lambda **kwargs: {"order_no": "123", "symbol": "TQQQ", "side": "BUY", "status": "OPEN"},
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert result.get("escalated") == 1
    assert len(repo.escalations) == 1


def test_degraded_fills_return_keeps_already_routed_order_truth_in_source():
    source = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    start = source.index("# fills contract/temp error is degraded")
    end = source.index("elif not daily_notional_available", start)
    section = source[start:end]
    assert section.count('"orders": list(orders)') == 2
    assert section.count('"orders_sent": routed_sent') == 2
    assert section.count('"orders_ack": routed_ack') == 2
    assert section.count('"sell_notional_routed": sell_notional_routed') == 2


def test_timeout_contract_is_explicit_240_seconds():
    source = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert "US_TICK_TIMEOUT_MIN_SEC" in source
    assert "TICK_TIMEOUT_CONTRACT" in source
    assert "TICK_TIMEOUT_RAISED" not in source
    for path in [
        "scripts/wsl/run-us-am.sh", "scripts/wsl/run-us-afternoon.sh",
        "scripts/wsl/run-us-prep.sh", "scripts/wsl/run-us-close.sh",
    ]:
        text = Path(path).read_text(encoding="utf-8")
        assert 'US_TICK_TIMEOUT_SEC:-240' in text
        assert 'US_TICK_TIMEOUT_MIN_SEC:-240' in text


def test_pb1_entry_has_stale_quote_fail_closed_on_both_lookup_paths():
    from trader.us.pb1.us_entry_engine import _quote_is_stale
    assert _quote_is_stale({"last": "100", "stale": True, "source": "DB_STALE"}) is True
    assert _quote_is_stale({"last": "100", "quality": "degraded"}) is True
    assert _quote_is_stale({"last": "100", "quality": "fresh"}) is False
    source = Path("trader/us/pb1/us_entry_engine.py").read_text(encoding="utf-8")
    assert source.count('if _quote_is_stale(current):') == 2
    assert source.count('[US_ENTRY][QUOTE_STALE_BLOCK]') == 2



def test_tqqq_query_exception_still_escalates_prior_cancel(monkeypatch):
    from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl

    now = datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc)
    order = {
        "trade_date": "2026-09-10",
        "order_no": "123",
        "client_order_key": "TQQQ_INF_V3:cycle:2026-09-10:BUY:1",
        "symbol": "TQQQ", "side": "BUY", "status": "ACK", "qty": 3,
        "meta": {"tqqq_ttl_cancel_requested_at": (now - timedelta(hours=1)).isoformat()},
    }

    class Repo:
        def __init__(self):
            self.escalations = []
        def load_expired_open_buy_orders(self, **kwargs):
            return [order]
        def mark_ttl_unresolved_escalated(self, order, **kwargs):
            self.escalations.append(kwargs)

    repo = Repo()
    monkeypatch.setenv("US_TQQQ_TTL_UNRESOLVED_ESCALATE_SEC", "900")
    result = reconcile_tqqq_open_buy_ttl(
        repository=repo, now=now, ttl_seconds=120,
        cancel_order=lambda **kwargs: (_ for _ in ()).throw(AssertionError("cancel must not replay")),
        query_order=lambda **kwargs: (_ for _ in ()).throw(TimeoutError("sustained KIS query timeout")),
    )
    assert result["pending"] == 1
    assert result["terminal"] == 0
    assert result.get("escalated") == 1
    assert len(repo.escalations) == 1


def test_routed_order_truth_counts_broker_submission_and_ack_flags():
    from trader.us.runner.trade_tick_runner import _routed_order_truth_counts

    orders = [
        {"status": "ACK_DB_FAILED", "broker_submit": True, "kis_ack": True},
        {"status": "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "broker_submit": True, "kis_ack": True},
        {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "broker_submit": True, "kis_ack": True},
        {"status": "REJECT", "broker_submit": True, "kis_ack": False},
        {"status": "BLOCKED", "broker_submit": False, "kis_ack": False},
    ]
    sent, ack, rejected, blocked = _routed_order_truth_counts(orders)
    assert sent == 4
    assert ack == 3
    assert rejected == 1
    assert blocked == 1



def test_routed_order_notional_uses_same_broker_submission_truth():
    from trader.us.runner.trade_tick_runner import _routed_order_notional

    orders = [
        {"status": "ACK_DB_FAILED", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 100.0}},
        {"status": "ACK_JOURNAL_FAILED_RECONCILE_REQUIRED", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 200.0}},
        {"status": "BROKER_SUBMIT_RESULT_UNKNOWN", "broker_submit": True, "kis_ack": True,
         "intent": {"notional_usd": 300.0}},
        {"status": "REJECT", "broker_submit": True, "kis_ack": False,
         "intent": {"notional_usd": 400.0}},
        {"status": "REJECT", "broker_submit": False, "kis_ack": False,
         "intent": {"notional_usd": 500.0}},
        {"status": "BLOCKED", "broker_submit": False, "kis_ack": False,
         "intent": {"notional_usd": 600.0}},
    ]
    # The first four were actually submitted to the broker.  A local/non-submitted
    # reject and a blocked order must not inflate routed SELL notional.
    assert _routed_order_notional(orders) == 1000.0


def test_normal_completion_reuses_broker_truth_counts():
    source = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert "routed_sent_total, ack_cnt, reject_cnt, blocked_cnt = _routed_order_truth_counts(orders)" in source
    assert "orders_sent = routed_sent_total" in source
    assert '"pending_count": ack_cnt + ack_db_failed_cnt' not in source
