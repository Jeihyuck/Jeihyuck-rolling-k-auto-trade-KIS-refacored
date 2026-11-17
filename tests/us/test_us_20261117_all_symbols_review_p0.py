"""2026-11-16 US incidents: all-symbol TP clock and candidate-isolation regression.

Read-only unit tests: no KIS POST, no physical DB writes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from trader.us.execution.order_router import (
    _tp_decision_fresh_for_native_http,
    _tp_live_quote_threshold_proven,
)
from trader.us.market_state_overlay import (
    build_profit_capture_intents,
    filter_entry_intents_for_market_state,
)
from trader.us.runner.trade_tick_runner import (
    is_us_global_postfilter_contract_unverified,
)


TICK_START = datetime(2026, 11, 16, 14, 31, 1, tzinfo=timezone.utc)


def _make_tp(*, symbol="NOVA", live=False, proof=None):
    position = {
        "symbol": symbol, "qty": 20, "current_price": 110.0,
        "entry_price": 100.0, "position_lifecycle_id": "cycle-test-001",
        "exit_quote_evidence": proof or {},
    }
    with (
        patch("trader.us.profit_capture.authoritative_broker_avg",
              return_value=(Decimal("100"), {"broker_avg_price_source": "kis_pchs_avg_pric"})),
        patch("trader.us.execution.us_legacy_nine_active_sell.enrich_legacy_nine_v2_decision_position",
              side_effect=lambda p, **_: p),
    ):
        return build_profit_capture_intents(
            [position], {"profit_capture_enabled": True},
            now=TICK_START, trade_date="2026-11-16",
            profit_capture_state={symbol: {}},
            defer_pending_until_claim=True,
            decision_clock_real_time=live,
        )


@pytest.mark.parametrize("symbol", ["NOVA", "AXYZ", "BETA"])
def test_live_tp_decision_time_is_not_stale_tick_start(symbol):
    before = datetime.now(timezone.utc)
    intents = _make_tp(symbol=symbol, live=True)
    after = datetime.now(timezone.utc)
    assert len(intents) == 1
    assert intents[0]["strategy_owner"] == "US_STANDARD"
    decision = datetime.fromisoformat(intents[0]["meta"]["last_profit_capture_at"])
    assert before <= decision <= after
    assert decision > TICK_START
    # Nov16 incident: an actual 28s routing delay must not look like 161s.
    assert _tp_decision_fresh_for_native_http(
        intents[0], now_utc=decision + timedelta(seconds=28), max_age_sec=30
    ) == (True, "US_TP_DECISION_FRESH")
    assert _tp_decision_fresh_for_native_http(
        intents[0], now_utc=decision + timedelta(seconds=31), max_age_sec=30
    ) == (False, "US_TP_DECISION_EXPIRED_BEFORE_HTTP")


def test_replay_keeps_injected_clock_separate_from_live_wall_clock():
    intents = _make_tp(live=False)
    assert len(intents) == 1
    assert intents[0]["meta"]["last_profit_capture_at"] == TICK_START.isoformat()


def test_quote_source_and_asof_are_not_fabricated_by_new_signal_clock():
    proof = {
        "source": "KIS_LIVE", "asof": "2026-11-16T14:33:14+00:00",
        "asof_epoch": 1794839594.0,
        "received_at_utc": "2026-11-16T14:33:15+00:00",
        "quality": "FRESH",
    }
    i = _make_tp(live=True, proof=proof)[0]
    meta = i["meta"]
    assert meta["decision_quote_evidence"] == {"price": 110.0, **proof}
    unknown = _make_tp(live=True)[0]["meta"]["decision_quote_evidence"]
    assert unknown["asof"] is None
    assert unknown["asof_epoch"] is None
    assert unknown["source"] == "UNVERIFIED"
    # A new decision time does NOT bypass the final original KIS quote fence.
    live_quote = {
        "_quote_quality": "FRESH", "_quote_source": "KIS_LIVE",
        "_quote_asof_epoch": 100.0, "output": {"last": "109.00"},
    }
    assert _tp_live_quote_threshold_proven(
        i, live_quote, now_epoch=107.0
    ) == (False, "US_TP_LIVE_QUOTE_EXPIRED")
    live_quote["_quote_asof_epoch"] = 104.0
    live_quote["output"]["last"] = "102.00"
    assert _tp_live_quote_threshold_proven(
        i, live_quote, now_epoch=107.0
    ) == (False, "US_TP_LIVE_THRESHOLD_LOST")


def test_per_symbol_intraday_rotation_block_preserves_other_eligible_symbol():
    overlay = {
        "market_state": "NORMAL", "intraday_market_overlay": "NORMAL",
        "intraday_rotation_overlay": "INTRADAY_SEMI_CRASH",
        "intraday_overlay_available": True,
    }
    candidates = [
        {"symbol": "AXYZ", "side": "BUY", "qty": 10, "theme_cluster": "AI_SEMI",
         "score_final": 80, "trend_score": 90, "rank_final30": 1},
        {"symbol": "BETA", "side": "BUY", "qty": 10, "theme_cluster": "HEALTHCARE",
         "score_final": 80, "trend_score": 90, "rank_final30": 2},
    ]
    kept, blocked = filter_entry_intents_for_market_state(candidates, overlay)
    assert [row["symbol"] for row in kept] == ["BETA"]
    assert [row["symbol"] for row in blocked] == ["AXYZ"]
    assert not is_us_global_postfilter_contract_unverified(overlay)


@pytest.mark.parametrize("overlay", [
    None,
    {"intraday_market_overlay": "INTRADAY_DATA_UNKNOWN"},
    {"intraday_overlay_available": False},
    {"intraday_error_reason": "LIVE_RISK_EVALUATION_FAILED"},
    {"global_risk_integrity_failed": True},
])
def test_unverified_global_risk_still_fences_all_standard_buys(overlay):
    assert is_us_global_postfilter_contract_unverified(overlay)


def test_per_symbol_metadata_mismatch_does_not_change_other_candidates():
    overlay = {"market_state": "NORMAL", "intraday_market_overlay": "NORMAL",
               "intraday_overlay_available": True}
    data = [
        {"symbol": "AXYZ", "side": "BUY", "qty": 5, "theme_cluster": "HEALTHCARE",
         "meta": {"theme_cluster": "AI_SEMI"}},
        {"symbol": "BETA", "side": "BUY", "qty": 5, "theme_cluster": "HEALTHCARE"},
    ]
    kept, blocked = filter_entry_intents_for_market_state(data, overlay)
    assert [row["symbol"] for row in kept] == ["BETA"]
    assert blocked[0]["reason"] == "ENTRY_METADATA_INVARIANT_FAIL"
    assert not is_us_global_postfilter_contract_unverified(overlay)


class _RowSet:
    def __init__(self, *, one=None, rows=None, rowcount=0):
        self.one = one
        self.rows = rows if rows is not None else []
        self.rowcount = rowcount

    def mappings(self):
        return self

    def one_or_none(self):
        return self.one

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.one


class _PreIoDb:
    def __init__(self, *, intent_attempt="attempt-1", include_broker_possible=False):
        self.intent_attempt = intent_attempt
        self.include_broker_possible = include_broker_possible
        self.updated = []
        self.stage_released = False
        self.committed = False
        self.rolled_back = False

    def execute(self, statement, params):
        sql = str(statement).lower()
        if "from us_profit_capture_lifecycle" in sql:
            if "select stage_status, state" in sql:
                return _RowSet(one={"stage_status": "NOT_TRIGGERED" if self.stage_released else "PENDING", "state": {}})
            return _RowSet(one={"stage_status": "PENDING",
                                "cumulative_filled_qty": 0, "requested_qty": 2})
        if "from us_order_events" in sql:
            rows = [{"event_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
                     "submit_attempt_id": "attempt-1"}]
            if self.include_broker_possible:
                rows.append({"event_type": "BROKER_CALL_STARTED",
                             "submit_attempt_id": "attempt-1"})
            return _RowSet(rows=rows)
        if "from us_execution_attempts a" in sql:
            return _RowSet(rows=[{
                "attempt_state": "ABANDONED_PRE_SUBMIT", "authoritative": True,
                "requested_qty": 2, "action_key": "action",
                "action": "TP1", "target_qty": 2,
                "filled_qty_before_attempt": 0,
                "lifecycle_id": "cycle-1",
                "account_id": "account-1", "action_state": "OPEN",
                "active_attempt_id": None,
            }])
        if "from us_orders" in sql:
            return _RowSet(one=None)
        if "select status, meta" in sql and "from us_order_intents" in sql:
            if self.updated:
                return _RowSet(one={"status": "BLOCKED", "meta": {"pre_io_abort_attempt_id": self.intent_attempt}})
            return _RowSet(one={"status": "PENDING",
                                "meta": {"submit_attempt_id": self.intent_attempt}})
        if "update us_order_intents" in sql:
            self.updated.append(params)
            return _RowSet(rowcount=1)
        raise AssertionError("Unexpected SQL in pre-HTTP release test: " + sql[:130])

    def begin(self):
        from contextlib import contextmanager

        @contextmanager
        def transaction():
            try:
                yield self
            except Exception:
                self.rolled_back = True
                raise
            else:
                self.committed = True
        return transaction()


def _run_proven_abort(db, *, mark_failure=None):
    from trader.us.db import repos
    state = {
        "NOVA": {
            "tp1_order_key": "US_PC_TEST_NOVA",
            "tp1_pending": True,
            "tp1_done": False,
            "meta": {"last_status": "PENDING"},
        }
    }
    def mark_stage(*args, **kwargs):
        if mark_failure:
            raise mark_failure
        db.stage_released = True
    with (
        patch.object(repos, "_get_engine_or_none", return_value=db),
        patch.object(repos, "_active_us_epoch", return_value="epoch-1"),
        patch.object(repos, "load_us_profit_capture_state", return_value=state),
        patch.object(repos, "mark_us_profit_capture_stage", side_effect=mark_stage) as mark,
    ):
        result = repos.release_us_profit_capture_pre_http_abort(
            trade_date="2026-11-16", symbol="NOVA", stage="tp1",
            position_lifecycle_id="cycle-1", client_order_key="US_PC_TEST_NOVA",
        )
    return result, mark


def test_proven_pre_http_abort_closes_intent_and_stage_in_one_transaction():
    db = _PreIoDb()
    result, mark = _run_proven_abort(db)
    assert result is True
    assert db.committed and not db.rolled_back
    assert len(db.updated) == 1
    assert db.updated[0]["epoch"] == "epoch-1"
    mark.assert_called_once()


def test_pre_http_abort_never_releases_foreign_attempt_or_post_possible():
    for db in (
        _PreIoDb(intent_attempt="foreign-attempt"),
        _PreIoDb(include_broker_possible=True),
    ):
        result, mark = _run_proven_abort(db)
        assert result is False
        assert not db.updated
        mark.assert_not_called()


def test_pre_http_abort_intent_and_stage_roll_back_together_on_failure():
    db = _PreIoDb()
    with pytest.raises(RuntimeError, match="injected_stage_failure"):
        _run_proven_abort(db, mark_failure=RuntimeError("injected_stage_failure"))
    assert db.rolled_back and not db.committed
    assert len(db.updated) == 1  # transaction rollback, not a committed UPDATE
