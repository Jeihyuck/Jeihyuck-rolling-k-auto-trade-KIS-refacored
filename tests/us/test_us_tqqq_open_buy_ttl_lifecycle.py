from datetime import datetime, timezone

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl


NOW = datetime(2026, 9, 5, 14, 0, tzinfo=timezone.utc)


def _order(**extra):
    return {
        "trade_date": "2026-09-05",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-05:BUY",
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 2,
        "status": "OPEN",
        "meta": {},
        **extra,
    }


class _Repository:
    def __init__(self, orders=None, terminal_result=None):
        self.orders = list(orders or [_order()])
        self.cancel_requests = []
        self.terminal_observations = []
        self.terminal_result = terminal_result

    def load_expired_open_buy_orders(self, **kwargs):
        assert kwargs["symbol"] == "TQQQ"
        return self.orders

    def mark_ttl_cancel_requested(self, order, **kwargs):
        self.cancel_requests.append((order, kwargs))
        order["meta"]["tqqq_ttl_cancel_requested_at"] = kwargs["requested_at"].isoformat()

    def apply_ttl_terminal_observation(self, order, observation):
        self.terminal_observations.append((order, observation))
        result = self.terminal_result or {"status": "OK"}
        if str(result.get("status") or "").upper() == "OK":
            order["status"] = observation["status"]
        return result


def _run(repo, *, cancel=lambda **_: {"status": "ACK"}, query=lambda **_: {"status": "OPEN"}):
    return reconcile_tqqq_open_buy_ttl(
        repository=repo, now=NOW, ttl_seconds=60,
        cancel_order=cancel, query_order=query,
    )


def test_tqqq_open_buy_blocks_only_tqqq_buy():
    repo = _Repository()
    calls = []
    _run(repo, cancel=lambda **identity: calls.append(identity) or {"status": "ACK"})
    assert calls == [{
        "order_no": "original-broker-order",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-05:BUY",
        "symbol": "TQQQ", "side": "BUY",
    }]
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_open_buy_does_not_block_tqqq_sell():
    repo = _Repository()
    queried = []
    _run(repo, query=lambda **identity: queried.append(identity) or {"status": "OPEN"})
    assert queried[0]["side"] == "BUY"
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_open_buy_does_not_block_standard_sell():
    repo = _Repository()
    standard_sell = {"symbol": "any-current-position", "side": "SELL"}
    _run(repo)
    assert standard_sell == {"symbol": "any-current-position", "side": "SELL"}
    assert repo.terminal_observations == []


def test_tqqq_ttl_cancel_uses_original_order_identity():
    repo = _Repository()
    cancel_calls, query_calls = [], []
    _run(
        repo,
        cancel=lambda **identity: cancel_calls.append(identity) or {"status": "ACK"},
        query=lambda **identity: query_calls.append(identity) or {"status": "OPEN"},
    )
    assert len(cancel_calls) == 1
    assert len(query_calls) == 2
    assert cancel_calls[0] == query_calls[0] == query_calls[1]
    assert cancel_calls[0]["order_no"] == repo.orders[0]["order_no"]
    assert cancel_calls[0]["client_order_key"] == repo.orders[0]["client_order_key"]


def test_tqqq_cancel_ack_does_not_terminalize_order():
    repo = _Repository()
    result = _run(repo, query=lambda **_: {})
    assert result == {"expired": 1, "cancel_requested": 1, "terminal": 0, "pending": 1}
    assert repo.orders[0]["status"] == "OPEN"
    assert repo.terminal_observations == []


def test_tqqq_broker_confirmed_cancel_terminalizes_order():
    repo = _Repository()
    result = _run(repo, query=lambda **_: {
        "order_no": "original-broker-order", "symbol": "TQQQ", "side": "BUY",
        "status": "CANCELLED", "filled_qty": 0,
    })
    assert result["terminal"] == 1
    assert repo.orders[0]["status"] == "CANCELLED"


def test_tqqq_unknown_cancel_result_keeps_pending():
    repo = _Repository()
    result = _run(repo, cancel=lambda **_: {"status": "UNKNOWN"}, query=lambda **_: {"status": "UNKNOWN"})
    assert result["cancel_requested"] == 1
    assert result["pending"] == 1
    assert repo.orders[0]["status"] == "OPEN"


def test_tqqq_production_callbacks_cancel_and_requery_same_broker_identity():
    from trader.us.runner.trade_tick_runner import _build_tqqq_ttl_callbacks

    calls = {"cancel": [], "query": []}

    class Client:
        def cancel_us_order(self, **kwargs):
            calls["cancel"].append(kwargs)
            return {"status": "ACK"}

    class Provider:
        def get_fills_by_order_no(self, **kwargs):
            calls["query"].append(kwargs)
            return {
                "order_no": kwargs["order_no"], "symbol": kwargs["symbol"],
                "side": "BUY", "status": "CANCELLED", "filled_qty": 0,
            }

    cancel, query = _build_tqqq_ttl_callbacks(
        Provider(), Client(), trade_date="2026-09-08", symbol="TQQQ"
    )
    identity = {
        "order_no": "original-broker-order",
        "client_order_key": "TQQQ_INF_V3:cycle-a:2026-09-08:BUY",
        "symbol": "TQQQ", "side": "BUY",
    }
    assert cancel(**identity)["status"] == "ACK"
    observed = query(**identity)
    assert observed["status"] == "CANCELLED"
    assert calls["cancel"] == [{
        "symbol": "TQQQ", "exchange": "NASDAQ", "order_no": "original-broker-order",
    }]
    assert calls["query"] == [{
        "order_no": "original-broker-order", "symbol": "TQQQ", "trade_date": "2026-09-08",
    }]


def _sep24_cancel_ack():
    return {
        "rt_cd": "0",
        "msg_cd": "40630000",
        "msg1": "모의투자 취소주문이 완료 되었습니다.",
        "output": {"ODNO": "0000001518", "ORD_TMD": "230935"},
    }


def _sep24_zero_remaining(order_no="original-broker-order"):
    return {
        "order_no": order_no,
        "symbol": "TQQQ",
        "side": "BUY",
        "status": "ACK_PENDING",
        "requested_qty": 2,
        "filled_qty": 0,
        "remaining_qty": 0,
    }


def test_sep24_cancel_ack_plus_exact_zero_remaining_terminalizes_next_tick():
    order = _order(
        status="ACK",
        meta={
            "tqqq_ttl_cancel_requested_at": NOW.isoformat(),
            "tqqq_ttl_cancel_result": _sep24_cancel_ack(),
        },
    )
    repo = _Repository([order])
    result = _run(repo, query=lambda **_: _sep24_zero_remaining())

    assert result["terminal"] == 1
    assert result["pending"] == 0
    assert result["cancel_confirmed"] == 1
    assert repo.orders[0]["status"] == "CANCELLED"
    assert repo.terminal_observations[0][1]["evidence_type"] == "TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING"
    assert repo.terminal_observations[0][1]["cancel_broker_order_no"] == "0000001518"


def test_sep24_successful_cancel_requeries_and_terminalizes_same_tick():
    repo = _Repository()
    observations = iter([
        {
            "order_no": "original-broker-order", "symbol": "TQQQ", "side": "BUY",
            "status": "OPEN", "requested_qty": 2, "filled_qty": 0, "remaining_qty": 2,
        },
        _sep24_zero_remaining(),
    ])

    result = _run(
        repo,
        cancel=lambda **_: _sep24_cancel_ack(),
        query=lambda **_: next(observations),
    )

    assert result["cancel_requested"] == 1
    assert result["terminal"] == 1
    assert result["pending"] == 0
    assert repo.orders[0]["status"] == "CANCELLED"


def test_zero_remaining_without_explicit_kis_cancel_ack_stays_fenced():
    repo = _Repository()
    observations = iter([
        {
            "order_no": "original-broker-order", "symbol": "TQQQ", "side": "BUY",
            "status": "OPEN", "requested_qty": 2, "filled_qty": 0, "remaining_qty": 2,
        },
        _sep24_zero_remaining(),
    ])
    result = _run(
        repo,
        cancel=lambda **_: {"status": "ACK"},
        query=lambda **_: next(observations),
    )
    assert result["terminal"] == 0
    assert result["pending"] == 1
    assert repo.orders[0]["status"] == "OPEN"


def test_cancel_ack_with_wrong_original_order_identity_stays_fenced():
    order = _order(
        status="ACK",
        meta={
            "tqqq_ttl_cancel_requested_at": NOW.isoformat(),
            "tqqq_ttl_cancel_result": _sep24_cancel_ack(),
        },
    )
    repo = _Repository([order])
    result = _run(repo, query=lambda **_: _sep24_zero_remaining("different-order"))
    assert result["terminal"] == 0
    assert result["pending"] == 1
    assert repo.orders[0]["status"] == "ACK"


def test_repository_preserves_broker_zero_remaining_for_cancel(monkeypatch):
    captured = {}

    def apply(**kwargs):
        captured.update(kwargs)
        return {"status": "OK", "order_status": "CANCELLED"}

    monkeypatch.setattr("trader.us.db.repos.apply_broker_order_observation", apply)
    from trader.us.infinite.repository import InfiniteRepository

    repository = object.__new__(InfiniteRepository)
    result = repository.apply_ttl_terminal_observation(
        _order(status="ACK"),
        {
            **_sep24_zero_remaining(),
            "status": "CANCELLED",
            "evidence_type": "TQQQ_TTL_CANCEL_ACK_ZERO_REMAINING",
        },
    )
    assert result["status"] == "OK"
    assert captured["remaining_qty"] == 0
    assert captured["filled_qty"] == 0


def test_cancel_ack_zero_remaining_without_explicit_fill_qty_stays_fenced():
    from trader.us.data_provider import normalize_us_order_status_row

    order = _order(
        status="ACK",
        meta={
            "tqqq_ttl_cancel_requested_at": NOW.isoformat(),
            "tqqq_ttl_cancel_result": _sep24_cancel_ack(),
        },
    )
    repo = _Repository([order])
    observation = normalize_us_order_status_row({
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "requested_qty": 2,
        "remaining_qty": 0,
        "status": "CANCELLED",
    })
    assert observation["filled_qty"] is None
    assert observation["filled_qty_present"] is False

    result = _run(repo, query=lambda **_: observation)

    assert result["terminal"] == 0
    assert result["pending"] == 1
    assert result.get("cancel_confirmed", 0) == 0
    assert repo.orders[0]["status"] == "ACK"
    assert repo.terminal_observations == []


def test_explicit_partial_fill_cancel_terminal_preserves_fill_evidence():
    repo = _Repository()
    result = _run(repo, query=lambda **_: {
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "status": "CANCELLED",
        "requested_qty": 2,
        "filled_qty": 1,
        "remaining_qty": 0,
        "avg_price": 77.25,
    })

    assert result["terminal"] == 1
    assert repo.orders[0]["status"] == "CANCELLED"
    assert repo.terminal_observations[0][1]["filled_qty"] == 1

def test_invalid_normalized_cancel_fill_never_terminalizes_tqqq():
    from trader.us.data_provider import normalize_us_order_status_row

    order = _order(
        status="ACK",
        meta={
            "tqqq_ttl_cancel_requested_at": NOW.isoformat(),
            "tqqq_ttl_cancel_result": _sep24_cancel_ack(),
        },
    )
    repo = _Repository([order])
    observation = normalize_us_order_status_row({
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "requested_qty": 2,
        "filled_qty": "bad",
        "remaining_qty": 0,
        "status": "CANCELLED",
    })
    assert observation["normalization_result"] == "quarantined"
    assert observation["filled_qty_raw_present"] is True
    assert observation["filled_qty_present"] is False

    result = _run(repo, query=lambda **_: observation)

    assert result["terminal"] == 0
    assert result["pending"] == 1
    assert repo.orders[0]["status"] == "ACK"
    assert repo.terminal_observations == []

def test_terminal_observation_persistence_pending_is_not_counted_terminal():
    repo = _Repository(terminal_result={
        "status": "PENDING",
        "reason": "cancel_partial_fill_price_missing",
    })
    result = _run(repo, query=lambda **_: {
        "order_no": "original-broker-order",
        "symbol": "TQQQ",
        "side": "BUY",
        "status": "CANCELLED",
        "requested_qty": 2,
        "filled_qty": 1,
        "remaining_qty": 0,
        "avg_price": 0,
    })

    assert result["terminal"] == 0
    assert result["pending"] == 1
    assert result["terminal_persist_pending"] == 1
    assert repo.orders[0]["status"] == "OPEN"
    assert repo.orders[0]["meta"]["tqqq_ttl_last_unresolved_reason"] == (
        "TERMINAL_PERSIST_cancel_partial_fill_price_missing"
    )

