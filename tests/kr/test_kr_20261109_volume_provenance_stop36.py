"""KR Nov09 volume proof source audit and unchanged 3/6 SELL hard protection."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from trader.kr.volume_proof import build_kr_volume_proof
from trader.kr.paper_stop36_policy import evaluate_kr_pb1_paper_stop36

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 9, 10, 0, tzinfo=KST)


def _bars(last_day="2026-11-06"):
    dates = pd.bdate_range(end=last_day, periods=50)
    return pd.DataFrame([{"date": day, "volume": 1000.0} for day in dates])


def _proof(**kwargs):
    args = dict(
        bars=_bars(), snapshot={"acml_vol": 1400, "stck_cntg_hour": "095900"},
        trade_date="2026-11-09", observed_at=NOW, code="005930",
        market="KOSPI", entry_style="MOMENTUM",
    )
    args.update(kwargs)
    return build_kr_volume_proof(**args)


def test_stale_completed_bar_explains_actual_age_and_source():
    proof = _proof(bars=_bars("2026-10-20"))
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "completed_bar_stale"
    assert proof["completed_bar_age_calendar_days"] == 20
    assert proof["failure_category"] == "DATA_UNVERIFIED"


def test_quote_staleness_contains_exchange_timestamp_without_authorizing_buy():
    proof = _proof(snapshot={"acml_vol": 1400, "stck_cntg_hour": "093000"})
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["exchange_quote_age_seconds"] == 1800
    assert proof["quote_as_of"] == "2026-11-09T09:30:00+09:00"
    assert proof["failure_reason"] == "kis_exchange_quote_stale_or_future"


def test_explicitly_invalid_primary_volume_cannot_be_hidden_by_raw_cache():
    proof = _proof(snapshot={
        "acml_vol": 0, "raw": {"acml_vol": 5500},
        "stck_cntg_hour": "095900",
    })
    assert proof["confirmation_state"] == "UNKNOWN"
    assert proof["failure_reason"] == "kis_cumulative_volume_missing"
    assert proof["quote_volume_field"] == "snapshot.acml_vol"


def test_valid_raw_volume_fallback_is_audited():
    proof = _proof(snapshot={
        "source": "KIS_REST_INQUIRE_PRICE",
        "source_identity": "AUTHENTICATED_REST_REQUEST_BOUND",
        "requested_symbol": "005930", "received_at": NOW.timestamp(),
        "raw": {"acml_vol": 1500, "stck_cntg_hour": "095900"},
    })
    assert proof["confirmation_state"] == "VERIFIED"
    assert proof["quote_volume_field"] == "snapshot.raw.acml_vol"
    assert proof["exchange_quote_age_seconds"] == 60


def test_breakout_without_price_trigger_is_policy_not_data_failure():
    proof = _proof(entry_style="ENTRY_BREAKOUT",
                   trigger_ok=False, trigger_info={"vol_ok": True})
    assert proof["confirmation_state"] == "FAILED"
    assert proof["failure_category"] == "STRATEGY_TRIGGER_NOT_MET"
    assert proof["failure_reason"] == "breakout_price_trigger_missing"


def test_nano_old_soft_tick_metadata_never_blocks_residual_hard_stop():
    position = {
        "strategy": "pb1_pullback_close", "exit_policy_family": "SWING_STAGED_EXIT",
        "code": "121600", "position_cycle_id": "nano-cycle",
        "portfolio_epoch_id": "nano-portfolio",
        "avg_buy_price": 100.0, "stop_price_at_entry": 92.0,
        "orderable_qty": 15, "qty": 15,
        "entry_exit_plan_json": {"exit_policy_family": "SWING_STAGED_EXIT"},
        "position_meta": {
            "kr_paper_stop36_cycle": "nano-cycle",
            "kr_paper_stop36_last_tick": "old-tick",
            "kr_paper_stop36_soft_done": True,
            "kr_paper_stop36_soft_done_cycle": "nano-cycle",
            "kr_paper_stop36_breach_ticks": 2,
        },
    }
    decision = evaluate_kr_pb1_paper_stop36(
        position, mark=94.0, env="practice",
        strategy_owner="pb1_pullback_close", tick_id="fresh-tick",
    )
    assert decision.hard_touched
    assert decision.sell_qty == 15
    assert decision.update_meta["kr_paper_stop36_last_tick"] == "fresh-tick"
