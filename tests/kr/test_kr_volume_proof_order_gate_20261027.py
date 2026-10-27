from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from trader.kr.volume_proof import build_kr_volume_proof

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 10, 27, 10, 0, 0, tzinfo=KST)


def _bars(include_today=False):
    days = pd.bdate_range(end="2026-10-26", periods=30)
    records = [{"date": d, "volume": 1000.0} for d in days]
    if include_today:
        records.append({"date": "2026-10-27", "volume": 999999999})
    return pd.DataFrame(records)


def _proof(**overrides):
    args = {
        "bars": _bars(),
        "snapshot": {"acml_vol": 1200, "stck_cntg_hour": "095959"},
        "trade_date": "2026-10-27",
        "observed_at": NOW,
        "code": "005930",
        "market": "KOSPI",
        "entry_style": "MOMENTUM",
    }
    args.update(overrides)
    return build_kr_volume_proof(**args)


def test_completed_day_average_does_not_include_intraday_bar():
    proof = _proof(bars=_bars(include_today=True))
    assert proof["confirmation_state"] == "VERIFIED"
    assert proof["daily_avg_volume20"] == 1000
    assert proof["proof_date"] == "2026-10-26"


def test_missing_and_zero_cumulative_fail_closed():
    for value in (None, 0, float("nan")):
        proof = _proof(snapshot={"acml_vol": value, "stck_cntg_hour": "095959"})
        assert proof["confirmation_state"] == "UNKNOWN"


def test_stale_or_missing_exchange_time_fails_closed():
    assert _proof(snapshot={"acml_vol": 1000})["confirmation_state"] == "UNKNOWN"
    assert _proof(snapshot={"acml_vol": 1000, "stck_cntg_hour": "091000"})["confirmation_state"] == "UNKNOWN"


def test_twenty_completed_bars_required():
    assert _proof(bars=_bars().tail(19))["failure_reason"] == "insufficient_completed_bars"


def test_breakout_requires_price_and_volume_trigger_independently():
    assert _proof(entry_style="BREAKOUT", trigger_ok=False, trigger_info={"vol_ok": True})["confirmation_state"] == "FAILED"
    assert _proof(entry_style="BREAKOUT", trigger_ok=True, trigger_info={"vol_ok": None})["confirmation_state"] == "UNKNOWN"
    proof = _proof(entry_style="BREAKOUT", trigger_ok=True, trigger_info={"vol_ok": True})
    assert proof["confirmation_state"] == "VERIFIED"
    assert proof["breakout_volume_confirmed"] is True


def test_cumulative_regression_and_quote_date_mismatch_reject():
    assert _proof(previous_cumulative=1500)["confirmation_state"] == "FAILED"
    assert _proof(observed_at=NOW + timedelta(days=1))["confirmation_state"] == "UNKNOWN"


def test_missing_daily_volume_does_not_synthesize_default():
    bars = _bars()
    bars.loc[bars.index[-1], "volume"] = None
    assert _proof(bars=bars)["confirmation_state"] == "UNKNOWN"


def _order_plan(style: str, proof: dict):
    return {
        "code": "005930", "market": "J", "side": "BUY", "stage": "PB1-AM",
        "entry_style": style, "entry_family": "ENTRY_" + style,
        "entry_reason": "ENTRY_" + style, "trigger_policy": "BREAKOUT_TRIGGER" if style == "BREAKOUT" else "MOMENTUM_CONTINUATION",
        "entry_price": 10000, "order_price": 10000, "limit_price": 10000,
        "stop_price": 9500, "qty": 1, "trigger_ok": True, "volume_proof": proof,
    }


def test_final_buy_boundary_rejects_missing_volume_proof_even_with_all_other_gates():
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    ok, reasons = engine._validate_entry_plan(_order_plan("MOMENTUM", {}))
    assert not ok
    assert "KR_VOLUME_PROOF_NOT_VERIFIED" in reasons


def test_final_buy_boundary_rejects_breakout_without_confirmed_volume():
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    good = _proof(entry_style="BREAKOUT", trigger_ok=True, trigger_info={"vol_ok": True})
    plan = _order_plan("BREAKOUT", good)
    ok, reasons = engine._validate_entry_plan(plan)
    assert ok, reasons
    plan["trigger_ok"] = False
    ok, reasons = engine._validate_entry_plan(plan)
    assert not ok
    assert "KR_BREAKOUT_NOT_VOLUME_CONFIRMED" in reasons


def test_final_buy_boundary_preserves_existing_pullback_policy():
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    plan = _order_plan("PULLBACK", {})
    plan["trigger_policy"] = "PULLBACK_OVERRIDE"
    ok, reasons = engine._validate_entry_plan(plan)
    assert ok, reasons
