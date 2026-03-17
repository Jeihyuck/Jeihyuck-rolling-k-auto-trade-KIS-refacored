from __future__ import annotations

import json

from trader.pb1_engine import _primary_no_trade_reason
from trader.pb1_runner import generate_run_summary_json


class DummyEngine:
    reject_reason_counts = {}
    _debug_summary = {"order_candidate_codes": ["009150"], "submit_attempt_count": 0}
    _run_summary_payload = {
        "scanned": 30,
        "setup_ok": 6,
        "relax_ok": 6,
        "score_ok": 6,
        "risk_ok": 4,
        "sized_ok": 3,
        "buyable_ok": 0,
        "order_candidates": 0,
        "submitted": 0,
        "blocked_reasons_counter": {
            "BUYABLE_TODAY_BUY_EXISTS": 2,
            "BUYABLE_COOLDOWN": 1,
            "atr_pct_too_high": 2,
            "SIZING_CAP_BELOW_ONE_SHARE": 1,
            "MIN_ORDER_KRW": 1,
        },
        "blocked_by": "BUYABLE_TODAY_BUY_EXISTS:2,atr_pct_too_high:2,SIZING_CAP_BELOW_ONE_SHARE:1,BUYABLE_COOLDOWN:1,MIN_ORDER_KRW:1",
        "no_trade_reason": "BUYABLE_TODAY_BUY_EXISTS",
        "entry_decision_result": "SKIP",
        "entry_decision_reason": "NO_ORDER_INTENTS",
    }


def test_primary_no_trade_reason_prefers_buyable_gate() -> None:
    reason = _primary_no_trade_reason(
        {
            "BUYABLE_TODAY_BUY_EXISTS": 2,
            "BUYABLE_COOLDOWN": 1,
            "MIN_ORDER_KRW": 5,
        },
        ok_count=6,
        order_candidates=0,
    )

    assert reason == "BUYABLE_TODAY_BUY_EXISTS"


def test_generate_run_summary_json_uses_engine_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("trader.pb1_runner.runtime_path", lambda *_parts: tmp_path / "runtime" / "summary")

    summary_path = generate_run_summary_json(
        run_id="run-1",
        trace_id="trace-1",
        env="practice",
        engine=DummyEngine(),
        as_of_requested="2026-03-17",
        as_of_used="2026-03-16",
        watchlist_as_of="2026-03-16",
        universe_as_of="2026-03-16",
        fallback_used=False,
    )

    payload = json.loads((tmp_path / "runtime" / "summary" / "run_run-1.json").read_text(encoding="utf-8"))
    assert summary_path is not None
    assert payload["counts"]["scanned"] == 30
    assert payload["counts"]["setup_ok_count"] == 6
    assert payload["counts"]["after_risk_count"] == 4
    assert payload["counts"]["after_sizing_count"] == 3
    assert payload["counts"]["order_candidate_count"] == 0
    assert payload["counts"]["submit_success_count"] == 0
    assert payload["no_trade_reason"] == "BUYABLE_TODAY_BUY_EXISTS"
    assert payload["entry_decision_result"] == "SKIP"