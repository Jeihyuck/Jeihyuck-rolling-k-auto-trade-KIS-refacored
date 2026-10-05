from trader.kr.pb1_stability import same_day_semantic_sell_exists


def row(status="ACK", family="TRAIL_STOP_HIT", **kwargs):
    return {
        "code": "035720",
        "side": "SELL",
        "status": status,
        "stage": family,
        "request_json": {
            "strategy_owner": "KR_STANDARD",
            "position_lifecycle_id": "sid:1:mode:1",
            "reason_family": family,
            **kwargs.pop("request_json", {}),
        },
        **kwargs,
    }


def exists(rows, *, stage=None, family="TRAIL_STOP_HIT"):
    return same_day_semantic_sell_exists(
        rows=rows,
        symbol="035720",
        strategy_owner="KR_STANDARD",
        reason_family=family,
        lifecycle_id="sid:1:mode:1",
        action_stage=stage,
    )


def test_ack_same_semantic_sell_is_blocked(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    assert exists([row()])


def test_unresolved_broker_truth_remains_fenced(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    for status in ("UNRESOLVED_ACK", "RECONCILE_ERROR", "PARTIALLY_FILLED"):
        assert exists([row(status)])


def test_explicit_reject_and_authoritative_cancel_are_retryable(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    assert not exists([row("REJECTED")])
    assert not exists([row(
        "CANCELLED",
        response_json={"tot_ccld_qty": "0"},
    )])
    assert not exists([row(
        "CANCELLED",
        response_json={"tot_ccld_qty": "2"},
    )])
    assert not exists([row("CANCELLED_ZERO_FILL")])


def test_cancel_without_cumulative_fill_remains_fenced(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    assert exists([row("CANCELLED")])
    assert exists([row(
        "CANCELLED",
        response_json={"tot_ccld_qty": "not-a-quantity"},
    )])


def test_legacy_cancelled_full_fill_without_claim_remains_fenced(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    completed = row(
        "CANCELLED",
        response_json={"tot_ccld_qty": "5"},
        qty=5,
    )

    assert exists([completed])


def test_tp_stage_identity_keeps_later_take_profits_independent(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    tp1 = row("FILLED", "PROFIT_CAPTURE", request_json={"exit_stage": "TP1"})
    assert exists([tp1], stage="TP1", family="PROFIT_CAPTURE")
    assert not exists([tp1], stage="TP2", family="PROFIT_CAPTURE")


def test_staged_order_uses_persisted_reason_family_for_semantic_fence(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    staged = row(
        "ACK",
        "TP1",
        request_json={"reason_family": "PROFIT_CAPTURE"},
    )
    assert exists([staged], stage="TP1", family="PROFIT_CAPTURE")
    assert not exists([staged], stage="TP1", family="HARD_STOP")


def test_semantic_repeat_override(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", "1")
    assert not exists([row()])
