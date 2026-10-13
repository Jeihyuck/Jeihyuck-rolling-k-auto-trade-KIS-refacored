"""PR-C: prevent repeated broker-routing of exact unresolved crossday TP claims."""
from __future__ import annotations

from trader.us.runner.trade_tick_runner import _suppress_pending_sell_exit_intents
from trader.us.db import repos
from trader.us import protective_tp_recovery as recovery


def _intent(symbol: str, lifecycle: str, *, owner="US_STANDARD", producer="us_pb1_exit"):
    return {
        "symbol": symbol, "side": "SELL", "qty": 1,
        "strategy_owner": owner, "strategy": producer,
        "position_lifecycle_id": lifecycle,
        "exit_type": "hard_stop_loss",
    }


def test_crossday_claim_suppresses_only_exact_standard_lifecycle_before_router(monkeypatch):
    calls = []
    monkeypatch.setattr(
        recovery, "load_crossday_unresolved_tp_lifecycles",
        lambda ids, date, *, env: calls.append((list(ids), date, env)) or {"mrvl-life"},
    )
    monkeypatch.setattr(
        repos, "has_pending_order_for_symbol_side",
        lambda **kwargs: False,
    )
    intents = [
        _intent("MRVL", "mrvl-life"),
        _intent("MRVL", "reentry-life"),
        _intent("AMD", "amd-life"),
        _intent("TQQQ", "tqqq-life", owner="TQQQ_INFINITE", producer="infinite"),
        _intent("MRVL", "mrvl-life", owner="TQQQ_INFINITE", producer="infinite"),
    ]
    kept = _suppress_pending_sell_exit_intents(
        intents, "2026-10-12", env="practice",
    )
    assert calls == [(["mrvl-life", "reentry-life", "amd-life"], "2026-10-12", "practice")]
    assert intents[0] not in kept
    assert intents[1:] == kept


def test_crossday_claim_query_failure_never_invents_terminal_or_releases_fence(monkeypatch):
    monkeypatch.setattr(
        recovery, "load_crossday_unresolved_tp_lifecycles",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kw: True)
    # Existing pending-order and broker submit risk gates remain authoritative.
    assert _suppress_pending_sell_exit_intents(
        [_intent("MRVL", "mrvl-life")], "2026-10-12", env="practice"
    ) == []


def test_unknown_owner_and_missing_lifecycle_not_assumed_crossday(monkeypatch):
    called = []
    monkeypatch.setattr(
        recovery, "load_crossday_unresolved_tp_lifecycles",
        lambda *a, **kw: called.append(True) or set(),
    )
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kw: False)
    intent = _intent("AMD", "", owner="", producer="unknown")
    assert _suppress_pending_sell_exit_intents([intent], "2026-10-12") == [intent]
    assert called == []
