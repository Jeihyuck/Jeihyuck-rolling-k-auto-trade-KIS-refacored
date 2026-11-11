"""P0 KR PB1 V3 runner: native KIS provenance and staged SELL permission."""
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

from trader.kr import us_v3_runner_active as native
from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract


DAY = date(2026, 11, 11)


def bars():
    current = date(2026, 9, 20)
    result = []
    while current < DAY:
        if current.weekday() < 5:
            result.append({"date": current.isoformat(), "high": 129,
                           "low": 125, "close": 127})
        current += timedelta(days=1)
    assert len(result) > 24
    result[-3]["high"] = 130
    result[-2]["high"] = 130
    result[-1]["high"] = 130
    return result


def contract(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    return build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:test", trading_epoch_id="epoch-test",
        position_cycle_id="cycle-test", runner_version="US_V3", code="005930",
    )


def completed():
    return {
        **{s: {"status": "FILLED", "broker_fill_proven": True}
           for s in ("TP1", "TP2", "TP3")},
        "PROFIT_DEFENSE_PARTIAL": {"status": "NOT_TRIGGERED"},
        "RUNNER_FINAL": {"status": "NOT_TRIGGERED"},
    }


def run(monkeypatch, *, price=115.0, states=None, daily=None, source="kis", entry=None, qty=46):
    contract_value = contract(monkeypatch)
    return native.evaluate_kr_v3_runner(
        contract=contract_value, stage_states=states or completed(),
        position={"entry_date": entry or "2026-10-20"},
        verified_entry_price=100., executable_price=price,
        holding_qty=qty, orderable_qty=qty,
        completed_daily=bars() if daily is None else daily,
        completed_daily_source=source, today=DAY, unresolved_sell=False,
    )


def test_post_tp3_uses_original_us_policy_for_partial(monkeypatch):
    out = run(monkeypatch)
    assert out["action"] == "ACTIVE_RUNNER"
    assert out["stage"] == "PROFIT_DEFENSE_PARTIAL"
    assert out["qty"] == 23
    assert out["order_authorized"] is False


def test_post_tp3_final_uses_original_us_policy_and_all_shares(monkeypatch):
    out = run(monkeypatch, price=108)
    assert out["action"] == "ACTIVE_RUNNER"
    assert out["stage"] == "RUNNER_FINAL"
    assert out["qty"] == 46
    assert out["order_authorized"] is False


@pytest.mark.parametrize("invalid,source", [
    (None, "db"), ([], "kis"),
    ([{"date": "2026-11-10", "high": 130, "low": 125, "close": 127}], "kis"),
])
def test_runner_missing_original_native_bars_never_authorizes(monkeypatch, invalid, source):
    assert run(monkeypatch, daily=invalid, source=source)["action"] == "HOLD"


def test_runner_unknown_entry_or_unproven_tp3_never_trades(monkeypatch):
    assert run(monkeypatch, entry="2030-01-01")["action"] == "HOLD"
    state = completed()
    state["TP3"] = {"status": "ACK"}
    assert run(monkeypatch, states=state)["action"] == "BLOCK"


def test_broker_filled_partial_is_us_done_and_final_can_fire(monkeypatch):
    states = completed()
    states["PROFIT_DEFENSE_PARTIAL"] = {
        "status": "FILLED", "broker_fill_proven": True, "broker_odno": "00001234",
        "target_qty": 23, "cumulative_filled_qty": 23,
    }
    out = run(monkeypatch, states=states, price=108, qty=23)
    assert out["action"] == "ACTIVE_RUNNER"
    assert out["stage"] == "RUNNER_FINAL"
    assert out["qty"] == 23


@pytest.mark.parametrize("state", [
    {"status": "FILLED", "broker_fill_proven": False,
     "broker_odno": "00001234", "target_qty": 23, "cumulative_filled_qty": 23},
    {"status": "FILLED", "broker_fill_proven": True,
     "target_qty": 23, "cumulative_filled_qty": 23},
    {"status": "FILLED", "broker_fill_proven": True,
     "broker_odno": "00001234", "target_qty": 23, "cumulative_filled_qty": 13},
])
def test_unproven_partial_filled_does_not_unlock_final(monkeypatch, state):
    states = completed()
    states["PROFIT_DEFENSE_PARTIAL"] = state
    out = run(monkeypatch, states=states, price=108, qty=23)
    assert out["action"] == "HOLD"
    assert "KIS_FILL_INTEGRITY" in out["reason"]


def test_runner_unknown_partial_blocks_full_exit(monkeypatch):
    state = completed()
    state["PROFIT_DEFENSE_PARTIAL"] = {"status": "ACK"}
    out = run(monkeypatch, price=108, states=state)
    assert out["action"] == "HOLD"
    assert "PARTIAL_ORDER_UNSETTLED" in out["reason"]


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeBroker:
    env = "practice"
    CANO = "00000KIS"

    def __init__(self, response=None):
        self.calls = []
        self.response = response

    def _headers(self, tr):
        return {"tr_id": tr}

    def _safe_request(self, method, url, *, headers, params, timeout):
        self.calls.append((method, url, headers, params, timeout))
        if self.response is not None:
            return self.response
        raw = [
            {
                "stck_bsop_date": bar["date"].replace("-", ""),
                "stck_hgpr": str(bar["high"]),
                "stck_lwpr": str(bar["low"]),
                "stck_clpr": str(bar["close"]),
            }
            for bar in bars()
        ]
        return FakeResponse({"rt_cd": "0", "output2": raw})


def test_direct_kis_daily_proof_no_generic_ohlcv_or_db(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr("trader.kis_wrapper.kis_http_enabled", lambda: True)
    monkeypatch.setattr("trader.kis_wrapper._pick_tr", lambda *args: ["FHKST03010100"])
    native._NATIVE_CACHE.clear()
    kis = FakeBroker()
    rows, source = native.load_kr_kis_native_completed_daily(kis=kis, code="005930", today=DAY)
    assert source == "kis" and len(rows) >= 24
    assert len(kis.calls) == 1
    assert kis.calls[0][2]["tr_id"] == "FHKST03010100"
    again, again_source = native.load_kr_kis_native_completed_daily(kis=kis, code="005930", today=DAY)
    assert again == rows and again_source == "kis"
    assert len(kis.calls) == 1


def test_kis_http_stub_fails_closed(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr("trader.kis_wrapper.kis_http_enabled", lambda: False)
    native._NATIVE_CACHE.clear()
    kis = FakeBroker()
    assert native.load_kr_kis_native_completed_daily(kis=kis, code="005930", today=DAY) == (None, None)
    assert not kis.calls


def test_kis_native_bad_response_does_not_claim_provenance(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr("trader.kis_wrapper.kis_http_enabled", lambda: True)
    monkeypatch.setattr("trader.kis_wrapper._pick_tr", lambda *args: ["FHKST03010100"])
    native._NATIVE_CACHE.clear()
    kis = FakeBroker(FakeResponse({"rt_cd": "-1", "output2": bars()}))
    assert native.load_kr_kis_native_completed_daily(kis=kis, code="005930", today=DAY) == (None, None)
