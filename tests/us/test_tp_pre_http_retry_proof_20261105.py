"""TP stage retry must be proven pre-HTTP and persist before routing.

These tests never call a broker or create real orders.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

from trader.us.market_state_overlay import build_profit_capture_intents


NOW = datetime(2026, 11, 5, 15, 0, tzinfo=timezone.utc)
POSITION = {
    "symbol": "HCA",
    "qty": 20,
    "position_lifecycle_id": "cycle-hca-1",
    "current_price": 110.0,
    "entry_price": 100.0,
}
DATE = "2026-11-05"


def _evaluate(state, *, persist_side_effect=None):
    def _avg(_pos, **_kwargs):
        from decimal import Decimal
        return Decimal("100"), {"broker_avg_price_source": "kis_pchs_avg_pric"}
    with patch("trader.us.profit_capture.authoritative_broker_avg", side_effect=_avg):
        with patch("trader.us.db.repos.mark_us_profit_capture_stage", side_effect=persist_side_effect) as mark:
            out = build_profit_capture_intents(
                [dict(POSITION)], {"profit_capture_enabled": True},
                now=NOW, trade_date=DATE,
                profit_capture_state={"HCA": state},
            )
    return out, mark


def test_first_tp_with_persisted_stage_routes_once():
    out, mark = _evaluate({})
    assert len(out) == 1
    assert out[0]["reason"] == "TAKE_PROFIT_TP1"
    assert out[0]["client_order_key"].endswith("_TAKE_PROFIT_TP1")
    mark.assert_called_once()


def test_db_stage_write_failure_does_not_route_broker_intent():
    out, mark = _evaluate({}, persist_side_effect=RuntimeError("db down"))
    assert out == []
    mark.assert_called_once()


def test_old_unknown_attempt_does_not_create_new_r_key():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_stage": "tp1",
            "last_status": "FAILED",
            "last_evidence_type": "UNVERIFIED",
        },
    }
    out, mark = _evaluate(state)
    assert out == []
    mark.assert_not_called()


def test_proven_pre_http_abort_can_create_new_attempt():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_stage": "tp1",
            "last_status": "FAILED",
            "last_evidence_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        },
    }
    out, mark = _evaluate(state)
    assert len(out) == 1
    assert out[0]["client_order_key"].endswith("_R2")
    mark.assert_called_once()


def test_broker_confirmed_cancelled_partial_allows_only_remainder():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "tp1_qty": 5,
            "tp1_filled_qty": 2,
            "last_stage": "tp1",
            "last_status": "CANCELLED",
            "last_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        },
    }
    out, mark = _evaluate(state)
    assert len(out) == 1
    assert out[0]["qty"] == 3
    assert out[0]["client_order_key"].endswith("_R2")
    mark.assert_called_once()


def test_unverified_cancelled_never_retries():
    state = {
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "tp1_qty": 5,
            "tp1_filled_qty": 2,
            "last_stage": "tp1",
            "last_status": "CANCELLED",
            "last_evidence_type": "UNVERIFIED",
        },
    }
    out, mark = _evaluate(state)
    assert out == []
    mark.assert_not_called()


def test_ack_with_order_number_blocks_pre_http_retry_even_if_failed_marker():
    state = {
        "tp1_broker_order_no": "0000012345",
        "meta": {
            "tp1_order_keys": ["old_attempt"],
            "last_stage": "tp1",
            "last_status": "FAILED",
            "last_evidence_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        },
    }
    out, mark = _evaluate(state)
    assert out == []
    mark.assert_not_called()

def test_real_router_ack_persists_exact_tp_lifecycle_not_done():
    from trader.us.runner.trade_tick_runner import persist_profit_capture_ack_from_route
    intent = {
        "symbol": "HCA", "side": "SELL", "qty": 5, "notional_usd": 550,
        "client_order_key": "US_PC_2026-11-05_HCA_cycle-hca-1_TAKE_PROFIT_TP1",
        "position_lifecycle_id": "cycle-hca-1",
        "meta": {"profit_capture_stage": "tp1"},
    }
    with patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
        assert persist_profit_capture_ack_from_route(
            intent, {"status": "ACK", "ack": {"ODNO": "0000099999"}}, DATE,
        )
    mark.assert_called_once()
    assert mark.call_args.kwargs["position_lifecycle_id"] == "cycle-hca-1"
    assert mark.call_args.kwargs["broker_order_no"] == "0000099999"
    assert mark.call_args.kwargs["status"] == "ACK"


def test_dry_run_never_persists_phantom_tp_ack_or_pending():
    from trader.us.runner.trade_tick_runner import persist_profit_capture_ack_from_route
    intent = {
        "symbol": "HCA", "side": "SELL", "qty": 5,
        "client_order_key": "dry-run", "position_lifecycle_id": "cycle-hca-1",
        "meta": {"profit_capture_stage": "tp1"},
    }
    with patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
        assert not persist_profit_capture_ack_from_route(
            intent, {"status": "DRY_RUN"}, DATE,
        )
    mark.assert_not_called()


def test_ack_missing_lifecycle_is_not_written_under_legacy_or_empty_identity():
    from trader.us.runner.trade_tick_runner import persist_profit_capture_ack_from_route
    intent = {
        "symbol": "HCA", "side": "SELL",
        "client_order_key": "unknown", "meta": {"profit_capture_stage": "tp1"},
    }
    with patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
        assert not persist_profit_capture_ack_from_route(
            intent, {"status": "ACK", "ack": {"ODNO": "9999"}}, DATE,
        )
    mark.assert_not_called()


def test_missing_tp_state_store_is_fail_closed_before_any_stage_write():
    with patch(
        "trader.us.db.repos.load_us_profit_capture_state",
        side_effect=RuntimeError("db unavailable"),
    ) as load:
        with patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
            out = build_profit_capture_intents(
                [dict(POSITION)], {"profit_capture_enabled": True},
                now=NOW, trade_date=DATE,
            )
    assert out == []
    load.assert_called_once()
    mark.assert_not_called()


def test_durable_order_key_without_attempt_lineage_never_looks_like_first_tp():
    out, mark = _evaluate({
        "tp1_order_key": "US_PC_old_TAKE_PROFIT_TP1",
        "meta": {"last_stage": "tp1", "last_status": "CANCELLED",
                 "last_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL"},
    })
    assert out == []
    mark.assert_not_called()


def test_durable_broker_order_number_without_attempt_lineage_never_reissues():
    out, mark = _evaluate({
        "tp1_broker_order_no": "0000036019",
        "meta": {"last_stage": "tp1", "last_status": "CANCELLED",
                 "last_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL"},
    })
    assert out == []
    mark.assert_not_called()


def test_disagreeing_durable_key_and_attempt_history_needs_reconciliation():
    out, mark = _evaluate({
        "tp1_order_key": "unlisted_latest_attempt",
        "meta": {
            "tp1_order_keys": ["older_attempt"],
            "tp1_qty": 5, "tp1_filled_qty": 2,
            "last_stage": "tp1", "last_status": "CANCELLED",
            "last_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
        },
    })
    assert out == []
    mark.assert_not_called()


def test_router_top_level_order_number_is_persisted_on_tp_ack():
    from trader.us.runner.trade_tick_runner import persist_profit_capture_ack_from_route
    intent = {
        "symbol": "HCA", "side": "SELL", "qty": 5,
        "client_order_key": "US_PC_2026-11-05_HCA_cycle-hca-1_TAKE_PROFIT_TP1",
        "position_lifecycle_id": "cycle-hca-1",
        "meta": {"profit_capture_stage": "tp1"},
    }
    with patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
        assert persist_profit_capture_ack_from_route(
            intent, {"status": "ACK", "order_no": "0000098877"}, DATE,
        )
    assert mark.call_args.kwargs["broker_order_no"] == "0000098877"


def test_pre_submit_tp_block_releases_only_after_exact_durable_proof():
    from trader.us.runner.trade_tick_runner import release_proven_tp_pre_submit_block
    intent = {
        "symbol": "HCA", "side": "SELL",
        "client_order_key": "tp-key", "position_lifecycle_id": "cycle-hca-1",
        "meta": {"profit_capture_stage": "tp1"},
    }
    result = {
        "status": "BLOCKED", "pre_submit_tp_block": True,
        "safe_to_release_tp_pending": True,
    }
    with patch("trader.us.db.repos.release_us_profit_capture_pre_http_abort", return_value=True) as release:
        assert release_proven_tp_pre_submit_block(intent, result, DATE)
    release.assert_called_once_with(
        trade_date=DATE, symbol="HCA", stage="tp1",
        position_lifecycle_id="cycle-hca-1", client_order_key="tp-key",
    )


def test_unverified_or_failed_pre_submit_release_never_authorizes_retry():
    from trader.us.runner.trade_tick_runner import release_proven_tp_pre_submit_block
    intent = {
        "symbol": "HCA", "side": "SELL",
        "client_order_key": "tp-key", "position_lifecycle_id": "cycle-hca-1",
        "meta": {"profit_capture_stage": "tp1"},
    }
    flag = {"status": "BLOCKED", "pre_submit_tp_block": True}
    with patch("trader.us.db.repos.release_us_profit_capture_pre_http_abort") as release:
        assert not release_proven_tp_pre_submit_block(
            intent, {**flag, "safe_to_release_tp_pending": False}, DATE,
        )
        release.assert_not_called()
    with patch("trader.us.db.repos.release_us_profit_capture_pre_http_abort", return_value=False) as release:
        assert not release_proven_tp_pre_submit_block(
            intent, {**flag, "safe_to_release_tp_pending": True}, DATE,
        )
        release.assert_called_once()
    with patch("trader.us.db.repos.release_us_profit_capture_pre_http_abort", side_effect=RuntimeError("db down")):
        assert not release_proven_tp_pre_submit_block(
            intent, {**flag, "safe_to_release_tp_pending": True}, DATE,
        )


def test_pre_submit_release_refuses_missing_identity_and_unknown_http():
    from trader.us.runner.trade_tick_runner import release_proven_tp_pre_submit_block
    intent = {
        "symbol": "HCA", "side": "SELL", "client_order_key": "tp-key",
        "meta": {"profit_capture_stage": "tp1"},
    }
    with patch("trader.us.db.repos.release_us_profit_capture_pre_http_abort") as release:
        assert not release_proven_tp_pre_submit_block(
            intent, {"status": "BLOCKED", "pre_submit_tp_block": True,
                     "safe_to_release_tp_pending": True}, DATE,
        )
        assert not release_proven_tp_pre_submit_block(
            {**intent, "position_lifecycle_id": "lc"},
            {"status": "BROKER_SUBMIT_RESULT_UNKNOWN",
             "pre_submit_tp_block": True,
             "safe_to_release_tp_pending": True}, DATE,
        )
        release.assert_not_called()


def test_missing_or_synthetic_position_lifecycle_never_creates_tp(monkeypatch):
    from trader.us.db import repos

    for bad in (None, "", "None", "NA", "UNKNOWN"):
        candidate = dict(POSITION)
        candidate["position_lifecycle_id"] = bad
        with patch(
            "trader.us.profit_capture.authoritative_broker_avg",
            return_value=(
                __import__("decimal").Decimal("100"),
                {"broker_avg_price_source": "kis_pchs_avg_pric"},
            ),
        ), patch.object(repos, "mark_us_profit_capture_stage") as mark:
            result = build_profit_capture_intents(
                [candidate], {"profit_capture_enabled": True},
                now=NOW, trade_date=DATE, profit_capture_state={"HCA": {}},
            )
        assert result == []
        mark.assert_not_called()


def test_failed_db_stage_transaction_never_creates_process_local_pending(monkeypatch):
    import pytest
    from trader.us.db import repos

    key = (DATE, "HCA", "cycle-hca-1")
    # Read the user's real state only through a DB transaction; fake the
    # failure here. No broker or network is invoked.
    class RejectWriteEngine:
        def begin(self):
            raise RuntimeError("simulated db transaction error")

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: RejectWriteEngine())
    monkeypatch.setattr(repos, "load_us_profit_capture_state", lambda *_a, **_k: {})
    repos._MEM_PROFIT_CAPTURE_STATE.pop(key, None)
    with pytest.raises(RuntimeError, match="simulated db"):
        repos.mark_us_profit_capture_stage(
            DATE, "HCA", "tp1", position_lifecycle_id="cycle-hca-1",
            order_key="first-attempt", qty=5, status="PENDING",
        )
    assert key not in repos._MEM_PROFIT_CAPTURE_STATE


def test_memory_only_tp_stage_backend_remains_available(monkeypatch):
    from trader.us.db import repos

    key = (DATE, "HCA", "cycle-hca-1")
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "load_us_profit_capture_state", lambda *_a, **_k: {})
    try:
        repos.mark_us_profit_capture_stage(
            DATE, "HCA", "tp1", position_lifecycle_id="cycle-hca-1",
            order_key="offline-only", qty=5, status="PENDING",
        )
        assert repos._MEM_PROFIT_CAPTURE_STATE[key]["tp1_pending"] is True
    finally:
        repos._MEM_PROFIT_CAPTURE_STATE.pop(key, None)


def test_native_tp_pre_http_freshness_rejects_historical_276_min_delay():
    from trader.us.execution.order_router import _tp_decision_fresh_for_native_http
    from datetime import timedelta

    for stage in ("tp1", "tp2", "tp3"):
        old = NOW - timedelta(minutes=276)
        intent = {
            "symbol": "HCA", "side": "SELL",
            "client_order_key": f"US_PC_{DATE}_HCA_cycle-hca-1_TAKE_PROFIT_{stage.upper()}",
            "meta": {
                "profit_capture_stage": stage,
                "last_profit_capture_at": old.isoformat(),
            },
        }
        valid, reason = _tp_decision_fresh_for_native_http(
            intent, now_utc=NOW, max_age_sec=30,
        )
        assert valid is False
        assert reason == "US_TP_DECISION_EXPIRED_BEFORE_HTTP"


def test_native_tp_pre_http_accepts_only_recent_aware_original_signal():
    from trader.us.execution.order_router import _tp_decision_fresh_for_native_http
    from datetime import timedelta

    intent = {
        "symbol": "HCA", "side": "SELL",
        "client_order_key": "US_PC_2026-11-05_HCA_cycle-hca-1_TAKE_PROFIT_TP2",
        "meta": {
            "profit_capture_stage": "tp2",
            "last_profit_capture_at": (NOW - timedelta(seconds=12)).isoformat(),
        },
    }
    assert _tp_decision_fresh_for_native_http(
        intent, now_utc=NOW, max_age_sec=30,
    ) == (True, "US_TP_DECISION_FRESH")

    for bad_time in ("", "2026-11-05T14:59:50", "bad-date"):
        intent["meta"]["last_profit_capture_at"] = bad_time
        valid, reason = _tp_decision_fresh_for_native_http(
            intent, now_utc=NOW, max_age_sec=30,
        )
        assert not valid and reason == "US_TP_DECISION_TIME_UNVERIFIED"

    intent["meta"]["last_profit_capture_at"] = (
        NOW + timedelta(minutes=2)
    ).isoformat()
    assert _tp_decision_fresh_for_native_http(
        intent, now_utc=NOW, max_age_sec=30,
    )[0] is False


def test_non_tp_protective_sell_has_no_new_pre_http_age_gate():
    from trader.us.execution.order_router import _tp_decision_fresh_for_native_http
    # Hard stops and TQQQ orders do not inherit a TP-specific time fence.
    for stage_intent in (
        {"side": "SELL", "client_order_key": "protective-stop",
         "meta": {"exit_type": "hard_stop_loss"}},
        {"side": "SELL", "client_order_key": "TQQQ_INFINITE_TP1",
         "meta": {"profit_capture_stage": "tp1"}},
        {"side": "BUY", "client_order_key": "US_PC_test",
         "meta": {"profit_capture_stage": "tp1"}},
    ):
        assert _tp_decision_fresh_for_native_http(
            stage_intent, now_utc=NOW,
        ) == (True, "NOT_NATIVE_PROFIT_CAPTURE_TP")


def test_ko_kr_tp2_missing_pre_http_proof_is_stage_scoped_and_never_resubmits(caplog):
    """2026-11-12 incident: preserve the live no-duplicate SELL safety gate.

    KO/KR ledger contains a durable attempt key but no broker-send/abort
    certification. A DB-only absence of orders or an old failure string is
    never authorization to submit a second SELL.
    """
    import logging
    from decimal import Decimal
    for symbol, prior_status in (
        ("KO", "US_RECOVERY_SCOPE_UNVERIFIED"),
        ("KR", "ORDER_DISABLED_DURABLE_LEDGER_UNAVAILABLE"),
    ):
        position = {
            **POSITION, "symbol": symbol,
            "position_lifecycle_id": f"original-cycle-{symbol}",
        }
        key = f"US_PC_2026-11-04_{symbol}_original-cycle-{symbol}_TAKE_PROFIT_TP2"
        state = {
            "tp1_done": True,
            "meta": {
                "tp1_done": True,
                "tp2_order_keys": [key],
                "tp2_qty": 5,
                "tp2_filled_qty": 0,
                "last_stage": "tp2",
                "last_status": prior_status,
                "last_evidence_type": None,
            },
        }
        caplog.clear()
        with patch(
            "trader.us.profit_capture.authoritative_broker_avg",
            return_value=(Decimal("100"), {"broker_avg_price_source": "kis_pchs_avg_pric"}),
        ), patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
            with caplog.at_level(logging.WARNING):
                decisions = build_profit_capture_intents(
                    [position], {"profit_capture_enabled": True},
                    now=NOW, trade_date=DATE,
                    profit_capture_state={symbol: state},
                )
        assert decisions == [], symbol
        mark.assert_not_called()
        fence = [rec.message for rec in caplog.records
                 if "[US_PROFIT_CAPTURE][RETRY_FENCED]" in rec.message]
        assert len(fence) == 1, symbol
        assert f"symbol={symbol}" in fence[0]
        assert "stage=tp2" in fence[0]
        assert f"last_key={key}" in fence[0]
        assert f"last_status={prior_status}" in fence[0]
        assert "action=RECONCILE_REQUIRED scope=THIS_TP_STAGE" in fence[0]


def test_proven_pre_http_abort_remains_only_safe_tp_retry_evidence():
    """No blanket release of an attempt merely because a stage is marked failed."""
    for stg in ("tp1", "tp2", "tp3"):
        stem = {"tp1_done": True, "tp2_done": True}
        state = {**stem, "meta": {
            f"{stg}_order_keys": ["durable-first-attempt"],
            "last_stage": stg, "last_status": "FAILED",
            "last_evidence_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        }}
        # A claimed pre-HTTP abort coupled with a real broker order number
        # must still be fenced: broker evidence dominates stale status.
        state[f"{stg}_broker_order_no"] = "0000043554"
        with patch(
            "trader.us.profit_capture.authoritative_broker_avg",
            return_value=(__import__("decimal").Decimal("100"),
                          {"broker_avg_price_source": "kis_pchs_avg_pric"}),
        ), patch("trader.us.db.repos.mark_us_profit_capture_stage") as mark:
            decisions = build_profit_capture_intents(
                [dict(POSITION)], {"profit_capture_enabled": True},
                now=NOW, trade_date=DATE, profit_capture_state={"HCA": state},
            )
        assert decisions == []
        mark.assert_not_called()
