"""PR356: real pre-submit timing ENFORCE may only approve capped LIMIT buys."""
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo


def test_ready_minute_proof_cannot_authorize_unbounded_market_buy(monkeypatch):
    import trader.pb1_engine as pb1

    now = datetime(2026, 11, 13, 9, 35, 12, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setattr(pb1, "now_kst", lambda: now)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(
        pb1, "evaluate_proof_for_candidate",
        lambda **kwargs: {
            "state": "READY_SHADOW", "reason": "PULLBACK_REBOUND_CONFIRMED",
            "max_entry_price": 10100.0,
        },
    )
    engine = pb1.PB1Engine.__new__(pb1.PB1Engine)
    engine.env = "practice"
    engine._now_kst = now
    candidate = SimpleNamespace(
        code="035760", setup_ok=True,
        features={"entry_style_selected": "ENTRY_PULLBACK",
                  "setup_ok": True, "trigger_ok": False},
    )
    for order_type in ("MARKET", "01", "", "MKT"):
        decision = engine._resolve_entry_pre_submit(
            cf=candidate, stage="PB1-AM",
            order_price=10000.0, order_type=order_type,
        )
        assert not decision.ok
        assert decision.blocking_stage == "pre_submit_timing"
        assert decision.reason_codes == [
            "KR_ENTRY_TIMING_PRICE_CAP_UNENFORCEABLE_ORDER_TYPE"
        ]
