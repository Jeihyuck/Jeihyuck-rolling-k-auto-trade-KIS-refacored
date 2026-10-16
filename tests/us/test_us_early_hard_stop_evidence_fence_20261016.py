"""Strict evidence fence for opt-in early PB1 HARD STOP only.

The normal end-of-tick exit path preserves its existing fallback semantics.
An unavailable cross-day TP claim or pending SELL lookup cannot authorize
an *early* broker submit.
"""
import pytest

from trader.us.runner.trade_tick_runner import _suppress_pending_sell_exit_intents


def _candidate():
    return {
        "symbol": "AMD",
        "side": "SELL",
        "qty": 2,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": "lifecycle-1",
        "strategy": "us_pb1_exit",
        "exit_type": "hard_stop",
        "exit_reason": "hard_stop_full_exit",
        "meta": {},
    }


def test_strict_early_path_denies_if_historical_claim_read_fails(monkeypatch):
    from trader.us import protective_tp_recovery
    from trader.us.db import repos

    def fail(*_args, **_kwargs):
        raise RuntimeError("historical claim DB unavailable")

    monkeypatch.setattr(
        protective_tp_recovery,
        "load_crossday_unresolved_tp_lifecycles", fail,
    )
    monkeypatch.setattr(
        repos, "has_pending_order_for_symbol_side", lambda **kwargs: False,
    )
    candidate = _candidate()
    assert _suppress_pending_sell_exit_intents(
        [candidate], "2026-10-16", env="practice", strict=True,
    ) == []
    # Pre-existing late-route fallback semantics remain unchanged.
    assert _suppress_pending_sell_exit_intents(
        [candidate], "2026-10-16", env="practice",
    ) == [candidate]


def test_strict_early_path_denies_if_pending_lookup_fails(monkeypatch):
    from trader.us import protective_tp_recovery
    from trader.us.db import repos

    monkeypatch.setattr(
        protective_tp_recovery,
        "load_crossday_unresolved_tp_lifecycles",
        lambda *_a, **_kw: set(),
    )
    monkeypatch.setattr(
        repos, "has_pending_order_for_symbol_side",
        lambda **kwargs: (_ for _ in ()).throw(OSError("DB timeout")),
    )
    candidate = _candidate()
    assert _suppress_pending_sell_exit_intents(
        [candidate], "2026-10-16", strict=True,
    ) == []
    assert _suppress_pending_sell_exit_intents(
        [candidate], "2026-10-16",
    ) == [candidate]


@pytest.mark.parametrize("is_crossday_fenced,is_pending", [
    (True, False), (False, True), (False, False),
])
def test_strict_early_path_respects_existing_claims_and_clean_gate(
    monkeypatch, is_crossday_fenced, is_pending,
):
    from trader.us import protective_tp_recovery
    from trader.us.db import repos

    monkeypatch.setattr(
        protective_tp_recovery,
        "load_crossday_unresolved_tp_lifecycles",
        lambda *_args, **_kwargs: {"lifecycle-1"} if is_crossday_fenced else set(),
    )
    monkeypatch.setattr(
        repos, "has_pending_order_for_symbol_side",
        lambda **kwargs: is_pending,
    )
    candidate = _candidate()
    actual = _suppress_pending_sell_exit_intents(
        [candidate], "2026-10-16", strict=True,
    )
    assert actual == ([] if is_crossday_fenced or is_pending else [candidate])
