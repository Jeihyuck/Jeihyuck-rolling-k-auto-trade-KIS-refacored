from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from trader.kr import entry_timing_shadow as shadow

KST = ZoneInfo("Asia/Seoul")


def _at(hhmm: str) -> datetime:
    return datetime.fromisoformat("2026-10-29T" + hhmm + ":00+09:00")


def _sample(hhmm: str, price: float, *, vwap=None, fresh=True):
    return {
        "exchange_ts": _at(hhmm).isoformat(),
        "price": price, "vwap": vwap, "quote_fresh": fresh,
    }


# Reconstructed from 2026-10-29 KR logs; snapshots, NOT continuous minute bars.
MICO_OPENING = [
    _sample("09:00", 25700), _sample("09:03", 26200),
    _sample("09:05", 26050), _sample("09:08", 25400),
    _sample("09:11", 25550), _sample("09:14", 25650),
    _sample("09:16", 25825), _sample("09:19", 25900),
    _sample("09:22", 26050), _sample("09:25", 26100),
    _sample("09:27", 26050),
]


def test_mico_0930_is_wait_not_automatic_buy():
    samples = [*MICO_OPENING, _sample("09:30", 26050)]
    decision = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples, observed_at=_at("09:30"),
        trigger_ok=False,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": None},
    )
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "VWAP_OR_TIME_ADJUSTED_RVOL_UNKNOWN"
    assert decision["opening_observed_high"] == 26200
    assert decision["opening_observed_low"] == 25400
    assert decision["not_order_authorization"] is True


def test_clock_passage_does_not_create_timing_proof():
    decision = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=MICO_OPENING,
        observed_at=_at("09:30"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": 2.0},
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "QUOTE_STALE_OR_MISSING"


def test_no_opening_coverage_means_wait_not_ready():
    samples = [_sample("09:25", 99), _sample("09:27", 100),
               _sample("09:29", 101), _sample("09:30", 110)]
    decision = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples,
        observed_at=_at("09:30"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": 2.0},
    )
    assert decision["reason"] == "INSUFFICIENT_OPENING_COVERAGE"


def test_historical_volume_profile_is_required_not_raw_daily_ratio():
    samples = [
        _sample("09:00", 100), _sample("09:03", 103),
        _sample("09:08", 98), _sample("09:14", 99),
        _sample("09:20", 101), _sample("09:25", 102),
        _sample("09:27", 101), _sample("09:31", 103, vwap=100),
        _sample("09:34", 104, vwap=100), _sample("09:37", 105, vwap=100),
    ]
    missing_rvol = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples,
        observed_at=_at("09:37"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "volume_expansion_ratio": 2.5},
    )
    assert missing_rvol["state"] == "WAIT"
    assert missing_rvol["reason"] == "VWAP_OR_TIME_ADJUSTED_RVOL_UNKNOWN"
    verified = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples,
        observed_at=_at("09:37"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": 1.5},
    )
    assert verified["state"] == "READY_SHADOW"
    assert verified["not_order_authorization"] is True


def test_future_price_is_not_used_to_create_a_signal():
    samples = [*MICO_OPENING, _sample("09:30", 26050),
               _sample("10:00", 50000, vwap=25000)]
    decision = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples,
        observed_at=_at("09:30"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": 1.5},
    )
    assert decision["state"] == "WAIT"
    assert decision["last_price"] == 26050


def test_stale_quote_returns_unknown():
    samples = [*MICO_OPENING, _sample("09:30", 26050, fresh=False)]
    decision = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_MOMENTUM", samples=samples,
        observed_at=_at("09:30"), trigger_ok=True,
        volume_proof={"confirmation_state": "VERIFIED",
                      "relative_volume_at_time": 2.0},
    )
    assert decision["state"] == "UNKNOWN"
    assert decision["reason"] == "QUOTE_UNVERIFIED"


def test_other_strategy_owners_are_not_changed():
    result = shadow.evaluate_kr_timing_shadow(
        style="KR_INFINITE", samples=MICO_OPENING,
        observed_at=_at("09:30"), trigger_ok=True,
    )
    assert result["state"] == "NOT_APPLICABLE"
    assert result["not_order_authorization"] is True


def test_pullback_does_not_guess_support_from_sparse_ticks():
    samples = [*MICO_OPENING, _sample("09:30", 26050)]
    result = shadow.evaluate_kr_timing_shadow(
        style="ENTRY_PULLBACK", samples=samples,
        observed_at=_at("09:30"), trigger_ok=False,
    )
    assert result["state"] == "UNKNOWN"
    assert result["reason"] == "PULLBACK_NEEDS_COMPLETED_BARS_AND_SUPPORT"


def test_shadow_persists_history_scoped_to_runtime(monkeypatch, tmp_path):
    monkeypatch.setattr(
        shadow, "runtime_path",
        lambda *parts: tmp_path.joinpath(*parts),
    )
    first = shadow.observe_kr_timing_shadow(
        code="059090", env="practice", account_scope="test_account",
        observed_at=_at("09:00"), price=25700,
        selected_style="ENTRY_MOMENTUM",
        quote={"stck_cntg_hour": "090000", "stck_bsop_date": "20261029"},
        trigger_ok=False,
    )
    second = shadow.observe_kr_timing_shadow(
        code="059090", env="practice", account_scope="test_account",
        observed_at=_at("09:03"), price=26200,
        selected_style="ENTRY_MOMENTUM",
        quote={"stck_cntg_hour": "090300", "stck_bsop_date": "20261029"},
        trigger_ok=False,
    )
    assert first["state"] == second["state"] == "WAIT"
    assert second["history_rows"] == 2
    assert "test_account" not in str(list(tmp_path.rglob("059090.jsonl"))[0])
