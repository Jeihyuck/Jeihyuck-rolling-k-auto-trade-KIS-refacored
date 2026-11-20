"""US Stage4 cumulative-fill proof: permutation, mutation, fail-closed persistence.

All fixtures are synthetic. This module performs no KIS GET/POST and no
operating database writes; native broker execution still needs practice proof.
"""
from __future__ import annotations

from copy import deepcopy

import pytest

from trader.us.execution import authoritative_reconcile_worker as stage4
from trader.us.execution.stage4_fill_memo import (
    VerifiedFillPersistenceMemo, fills_digest,
)


def _fills():
    return [
        {"symbol": "HCA", "order_no": "NATIVE-1", "side": "SELL",
         "cumulative_filled_qty": 2, "price_usd": "150.25"},
        {"symbol": "ABBV", "order_no": "NATIVE-2", "side": "SELL",
         "cumulative_filled_qty": 3, "price_usd": "205.10"},
    ]


def test_identical_native_cumulative_multiset_has_same_digest_in_any_order():
    original = _fills()
    inverse = list(reversed(deepcopy(original)))
    assert fills_digest(original) == fills_digest(inverse)
    inverse[0]["observed_at"] = "2026-11-20T10:00:00Z"
    inverse[1]["save_status"] = "OK"
    assert fills_digest(original) == fills_digest(inverse)


@pytest.mark.parametrize("mutation", [
    lambda rows: rows.__setitem__(0, {**rows[0], "cumulative_filled_qty": 3}),
    lambda rows: rows.__setitem__(1, {**rows[1], "price_usd": "205.11"}),
    lambda rows: rows.__setitem__(0, {**rows[0], "order_no": "ANOTHER-ORDER"}),
    lambda rows: rows.pop(),
    lambda rows: rows.append(deepcopy(rows[0])),
])
def test_changed_native_evidence_must_invalidate_permutation_memo(mutation):
    old = _fills()
    changed = deepcopy(old)
    mutation(changed)
    assert fills_digest(changed) != fills_digest(old)


def test_duplicate_rows_retained_not_deduplicated():
    source = _fills()
    assert fills_digest(source + [source[0]]) != fills_digest(source)
    assert fills_digest(source + [source[0]]) == fills_digest(
        [source[0], source[1], source[0]]
    )


def test_memo_ttl_failure_restart_and_mutated_db_objects():
    clock = [100.0]
    memo = VerifiedFillPersistenceMemo(refresh_sec=120, clock=lambda: clock[0])
    native = _fills()
    digest = fills_digest(native)
    assert memo.should_persist(native)
    memo.mark_persisted_digest(digest, status="OK")
    native[0]["matched_local_order_id"] = 99  # DB writer mutates local copy
    assert memo.should_persist(list(reversed(_fills()))) is False
    clock[0] = 219.99
    assert memo.should_persist(_fills()) is False
    clock[0] = 220.0
    assert memo.should_persist(_fills()) is True
    memo.mark_persisted_digest(fills_digest(_fills()), status="ERROR")
    assert memo.should_persist(_fills()) is True
    restarted = VerifiedFillPersistenceMemo(clock=lambda: clock[0])
    assert restarted.should_persist(_fills()) is True


def test_stage4_reordered_native_rows_are_not_repersisted_but_changes_are(monkeypatch):
    """Full worker function with all external I/O substituted by explicit fakes."""
    from trader.us.db import repos
    from trader.us.execution import order_journal, reconcile
    from trader.us.execution import reservation_reconcile_worker
    from trader.us.runner import trade_tick_runner

    monkeypatch.setattr(stage4, "_STAGE4_FILL_MEMOS", {})
    saved = []
    statuses = ["OK", "OK", "ERROR", "OK"]

    def save(rows, *, trade_date):
        assert trade_date == "2026-11-20"
        saved.append(deepcopy(rows))
        rows[0]["matched_local_order_id"] = len(saved)
        return {"status": statuses.pop(0)}

    monkeypatch.setattr(repos, "save_fills_with_result", save)
    def replay(*args, **kwargs):
        assert kwargs["active_claims_only"] is True
        assert kwargs["active_claim_date_scope"] == "current"
        return {"status": "OK", "unresolved_count": 0}
    monkeypatch.setattr(order_journal, "replay_order_journal", replay)
    monkeypatch.setattr(
        reconcile, "reconcile_ack_orders_with_balance",
        lambda **kwargs: {"status": "OK", "pending_count": 0, "unresolved_count": 0},
    )
    monkeypatch.setattr(
        reservation_reconcile_worker, "reconcile_proven_reservations_once",
        lambda **kwargs: {"unresolved": 0},
    )
    monkeypatch.setattr(
        trade_tick_runner, "_mark_trend_stages_from_records",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        trade_tick_runner, "_mark_soft_stop_stages_from_records",
        lambda *args, **kwargs: None,
    )
    def cycle(rows):
        return stage4.run_reconcile_once(
            provider=object(), trade_date="2026-11-20",
            session_run_id="offline-multiset", writer_generation="offline",
            conn=object(), scope={},
            prefetched_fills={"status": "OK", "fills": deepcopy(rows)},
        )

    first = cycle(_fills())
    assert first["reconcile_status"] == "OK" and len(saved) == 1
    again = cycle(list(reversed(_fills())))
    assert again["replay_status"] == "OK" and len(saved) == 1
    changed = deepcopy(_fills())
    changed[0]["cumulative_filled_qty"] = 3
    cycle(changed)
    assert len(saved) == 2
    more = deepcopy(changed)
    more[0]["cumulative_filled_qty"] = 4
    with pytest.raises(RuntimeError, match="US_STAGE4_FILLS_PERSIST_UNVERIFIED"):
        cycle(more)
    assert len(saved) == 3
    cycle(more)
    assert len(saved) == 4, "failed save must never be treated as durable evidence"


@pytest.mark.parametrize("reason", [
    "FILL_ORIGINAL_SCOPE_UNVERIFIED",
    "FILL_ORIGINAL_ORDER_NOT_FOUND",
    "FILL_ORDER_NOT_FOUND",
    "FILL_EPOCH_COLLISION",
])
def test_unstored_original_fill_is_not_memoized_as_success(monkeypatch, reason):
    """Simulate production repo's deliberate original-order validation skip.

    Failing to classify these values led Stage4 to report save=OK despite no
    durable fill, then memo-hit future KIS cycles as if DB had saved it.
    """
    from trader.us.db import repos

    def rejected_save(fills, trade_date):
        assert trade_date == "2026-11-20"
        fills[0]["save_status"] = reason
        return 0

    monkeypatch.setattr(repos, "save_fills", rejected_save)
    monkeypatch.setattr(repos, "_LAST_SAVE_FILLS_ERROR", None)
    native = [deepcopy(_fills()[0])]
    result = repos.save_fills_with_result(native, trade_date="2026-11-20")
    assert result["status"] == reason
    assert result["inserted_count"] == 0
    assert result["error_status_counts"] == {reason: 1}

    memo = VerifiedFillPersistenceMemo()
    memo.mark_persisted_digest(fills_digest(native), status=result["status"])
    assert memo.should_persist(native), "missing native fill must be retried"
