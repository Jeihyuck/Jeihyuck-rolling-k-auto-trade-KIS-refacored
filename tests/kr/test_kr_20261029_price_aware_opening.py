"""Only verified KIS completed minute OHLCV can authorize future KR PB1 timing.

All prices in this test are synthetic scenarios; Mico 10/29 sparse tick
logs are NOT rewritten as complete 1-minute OHLCV.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from trader.kr import opening_bar_contract as timing
from trader.pb1_engine import PB1Engine
from scripts.kr_opening_observer import final30_codes, run_once


KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 10, 29, 9, 35, tzinfo=KST)


def _bars(*, date="20261029", vols=1000):
    start = datetime(2026, 10, 29, 9, 0, tzinfo=KST)
    result = []
    for i in range(36):
        dt = start + timedelta(minutes=i)
        close = 25700 + (i % 5) * 20
        if i == 3:
            close = 26200
        if i == 8:
            close = 25400
        if i == 29:
            close = 26050
        if i == 31:
            close = 25550
        if i == 32:
            close = 25600
        if i == 33:
            close = 25700
        if i in (34,35):
            close = 26000
        result.append({
            "date": date, "time": dt.strftime("%H%M%S"),
            "open": close-10, "high": close+10,
            "low": close-20, "close": close,
            "volume": (1800 if i in (32,33) else vols),
        })
    return result


def test_sparse_mico_tick_prices_are_not_completed_bars():
    rows = [{"date": "20261029", "time": "090000",
             "price": 25700, "volume": 1000}]
    proof = timing.evaluate_price_aware_entry(
        style="ENTRY_MOMENTUM", rows=rows,
        now=NOW, vwap=25500, vwap_verified=True,
        order_price=26050,
    )
    assert proof["state"] == "UNKNOWN"
    assert proof["reason"] == "OPENING_BARS_MISSING_OR_INCOMPLETE"


def test_mico_0930_requires_fresh_post_opening_signal():
    proof = timing.evaluate_price_aware_entry(
        style="ENTRY_MOMENTUM", rows=_bars(),
        now=datetime(2026, 10, 29, 9, 31, tzinfo=KST),
        vwap=25800, vwap_verified=True, order_price=26050,
    )
    assert proof["state"] == "WAIT"
    assert proof["reason"] == "POST_OPENING_CONFIRMATION_PENDING"


def test_momentum_reclaim_requires_vwap_volume_risk_room_and_rising_bars():
    proof = timing.evaluate_price_aware_entry(
        style="ENTRY_MOMENTUM", rows=_bars(),
        now=datetime(2026, 10, 29, 9, 34, tzinfo=KST),
        vwap=25500, vwap_verified=True, order_price=25700,
    )
    assert proof["state"] == "READY_SHADOW"
    assert proof["reason"] == "MOMENTUM_PULLBACK_RECLAIM"
    assert proof["max_entry_price"] >= 25700
    assert proof["not_order_authorization"] is True
    blocked = timing.evaluate_price_aware_entry(
        style="ENTRY_MOMENTUM", rows=_bars(),
        now=datetime(2026, 10, 29, 9, 34, tzinfo=KST),
        vwap=None, vwap_verified=False, order_price=25700,
    )
    assert blocked["state"] == "UNKNOWN"


def test_price_cap_rejects_chasing_a_finished_rebound():
    proof = timing.evaluate_price_aware_entry(
        style="ENTRY_MOMENTUM", rows=_bars(),
        now=datetime(2026, 10, 29, 9, 34, tzinfo=KST),
        vwap=25500, vwap_verified=True, order_price=26200,
    )
    assert proof["state"] == "WAIT"
    assert proof["reason"] == "ORDER_PRICE_MOVED_FROM_CONFIRMED_BAR"


def test_wrong_day_future_and_incomplete_minute_never_enter_opening():
    bad = _bars(date="20261028")
    assert timing.clean_completed_bars(bad, NOW) == []
    rows = _bars()
    cleaned = timing.clean_completed_bars(rows, NOW)
    assert len(cleaned) == 35  # 09:35 unfinished, 09:00..09:34 complete


def test_missing_true_ohlcv_never_infers_low_from_close():
    rows = _bars()
    rows[3].pop("high")
    assert len(timing.clean_completed_bars(rows, NOW)) == 34
    assert timing.evaluate_price_aware_entry(
        style="ENTRY_PULLBACK", rows=rows, now=NOW,
        vwap=None, vwap_verified=False,
    )["state"] == "WAIT"  # support confirmation must be independent


def test_unknown_style_and_opening_window_never_create_buy():
    assert timing.evaluate_price_aware_entry(
        style="KR_INFINITE", rows=_bars(), now=NOW,
        vwap=None, vwap_verified=False,
    )["state"] == "NOT_APPLICABLE"
    assert timing.evaluate_price_aware_entry(
        style="ENTRY_PULLBACK", rows=_bars(),
        now=datetime(2026,10,29,9,20,tzinfo=KST),
        vwap=None, vwap_verified=False,
    )["state"] == "WAIT"


def test_persisted_proof_is_account_day_code_scoped_and_tamper_checked(monkeypatch,tmp_path):
    monkeypatch.setattr(timing, "runtime_path", lambda *p: tmp_path.joinpath(*p))
    now=datetime(2026,10,29,9,34,tzinfo=KST)
    saved = timing.save_opening_proof(
        env="practice",account_scope="test-only",code="059090",
        now=now,raw_bars=_bars(),
        vwap=25500,vwap_source="KIS_NATIVE_WEIGHTED_AVG",
    )
    assert saved["bars"]==34
    loaded=timing.load_opening_proof(
        env="practice",account_scope="test-only",code="059090",now=now,
    )
    assert loaded and len(loaded["bars"])==34
    assert timing.load_opening_proof(
        env="practice",account_scope="different",code="059090",now=now,
    ) is None
    file=tmp_path.joinpath(*saved["path"].replace(str(tmp_path)+"/","").split("/"))
    file.write_text(file.read_text().replace("25500.0","25000.0"),encoding="utf-8")
    assert timing.load_opening_proof(
        env="practice",account_scope="test-only",code="059090",now=now,
    ) is None


def test_enforced_pre_submit_fails_closed_but_cannot_place_order(monkeypatch):
    import trader.pb1_engine as pb1
    now=datetime(2026,10,29,9,35,tzinfo=KST)
    monkeypatch.setattr(pb1, "now_kst", lambda: now)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE","ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED","1")
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate",lambda **kw: {
        "state":"UNKNOWN","reason":"NO_FRESH_VERIFIED_KIS_MINUTE_BAR_PROOF",
    })
    e=PB1Engine.__new__(PB1Engine)
    e.env="practice"
    e._now_kst=now
    cf=SimpleNamespace(code="059090",features={
        "entry_style_selected":"ENTRY_MOMENTUM","setup_ok":True,"trigger_ok":False,
    },setup_ok=True)
    result=e._resolve_entry_pre_submit(cf=cf,stage="PB1-AM",
        order_price=26050.0,order_type="LIMIT")
    assert result.ok is False
    assert result.reason_codes==["KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH"]


def test_observer_final30_input_rejects_wrong_environment(tmp_path):
    file=tmp_path/"final30.json"
    file.write_text('{"trade_date":"2026-10-29","market":"KR","env":"real","rows":[{"code":"059090"}]}')
    with pytest.raises(RuntimeError,match="SCOPE_MISMATCH"):
        final30_codes(trade_date="2026-10-29",filename=str(file))


def test_observer_never_calls_broker(monkeypatch,tmp_path):
    import scripts.kr_opening_observer as obs
    monkeypatch.setattr(obs,"get_account_key",lambda **kw:"testonly")
    monkeypatch.setattr(obs,"now_kst",lambda: NOW)
    monkeypatch.setattr(timing,"runtime_path",lambda *p:tmp_path.joinpath(*p))
    class ReadOnlyKis:
        def get_intraday_candles_today(self, code, start_hhmm):
            return _bars()
        def get_price_snapshot(self, code, market):
            return {"wghn_avrg_stck_prc":25500}
        def buy_stock(self,*args,**kwargs):
            raise AssertionError("broker BUY not allowed")
        def sell_stock_market(self,*args,**kwargs):
            raise AssertionError("broker SELL not allowed")
    result=run_once(kis=ReadOnlyKis(),codes=["059090"],
        observed_at=NOW,sleep_between=0)
    assert result["completed"]==1
