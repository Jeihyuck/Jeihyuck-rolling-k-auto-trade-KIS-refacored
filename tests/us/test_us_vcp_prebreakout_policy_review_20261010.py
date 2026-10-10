"""Opt-in VCP pre-breakout watchlist arming does not bypass live trigger."""
import pytest

from trader.us.watchlist_builder import _select_entry_style
from trader.us.pb1.us_entry_engine import _verify_us_vcp_live_breakout
from trader.us.db.repos import _merge_us_daily_metrics_meta
from trader.us.score_columns import canonicalize_us_watchlist_row
from trader.us.entry_exit_contract import build_us_entry_exit_contract


def _row(price=98.0, source="completed_daily_ohlcv"):
    return {
        "symbol": "AAPL", "entry_style_selected": "vcp",
        "vcp_pass": True, "trend_template_pass": True,
        "pivot_price": 100.0, "close": price,
        "vcp_evidence_source": source,
        "vcp_daily_avg_volume20": 100000,
    }


def _select(row):
    return _select_entry_style(
        row, pb1_score=.60, momentum_score=.50,
        pullback_score=.40, breakout_score=.50, vcp_score=.95,
    )


def test_prebreakout_arming_off_keeps_prior_selection(monkeypatch):
    monkeypatch.delenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", raising=False)
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _row(98)
    assert _select(row) == "pb1_pullback"
    assert "vcp_setup_armed_before_breakout" not in row


def test_arming_only_works_with_live_broker_verification_enabled(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "0")
    assert _select(_row(98)) == "pb1_pullback"


@pytest.mark.parametrize(
    ("prep_close", "evidence_source", "vcp_expected"),
    [
        (95.0, "completed_daily_ohlcv", True),
        (98.0, "completed_daily_ohlcv", True),
        (99.99, "completed_daily_ohlcv", True),
        (100.0, "completed_daily_ohlcv", True),
        (104.99, "completed_daily_ohlcv", True),
        (94.0, "completed_daily_ohlcv", False),
        (105.10, "completed_daily_ohlcv", False),
        (98.0, "stale_or_unknown", False),
    ],
)
def test_prebreakout_band_respects_existing_five_percent_extension(
    monkeypatch, prep_close, evidence_source, vcp_expected,
):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _row(prep_close, evidence_source)
    style = _select(row)
    assert (style == "vcp") is vcp_expected
    if vcp_expected:
        assert row["vcp_setup_armed_before_breakout"] is (prep_close < 100)


def test_setup_is_only_armed_live_price_and_volume_still_needed(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _row(98)
    assert _select(row) == "vcp"
    quote = {
        "last": "99", "tvol": "200000", "source": "KIS_LIVE",
        "age_sec": 0, "quality": "fresh", "stale": False,
    }
    assert _verify_us_vcp_live_breakout(row, quote)[0] is False
    quote["last"] = "101"
    quote["tvol"] = "140000"
    assert _verify_us_vcp_live_breakout(row, quote)[0] is False
    quote["tvol"] = "180000"
    assert _verify_us_vcp_live_breakout(row, quote)[0] is True


def test_armed_identity_survives_prep_db_and_frozen_contract(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = {**_row(98), "exchange": "NASDAQ", "score": .90, "score_final": .90}
    assert _select(row) == "vcp"
    row["entry_style_selected"] = "vcp"
    meta = _merge_us_daily_metrics_meta(row)
    canonical = canonicalize_us_watchlist_row({
        "symbol": "AAPL", "score": .90, "meta": meta,
    })
    assert canonical["vcp_setup_armed_before_breakout"] is True
    frozen = build_us_entry_exit_contract(canonical)
    assert frozen["entry_provenance"]["vcp_setup_armed_before_breakout"] is True


@pytest.mark.parametrize("invalid_field", ["vcp_evidence_source", "vcp_daily_avg_volume20"])
def test_armed_vcp_without_completed_daily_volume_proof_does_not_displace_momentum(
    monkeypatch, invalid_field,
):
    """PREP arming must not turn an invalid VCP into a top-ranked blocking row."""
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = {
        **_row(98.0),
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "momentum_pass": True,
        "breakout_pass": False,
    }
    row.pop(invalid_field)
    style = _select_entry_style(
        row, pb1_score=.60, momentum_score=.90, pullback_score=.40,
        breakout_score=.50, vcp_score=.95,
    )
    assert style == "momentum"
    assert row["independent_proof_status"]["vcp"] is False
    assert row["independent_proof_status"]["momentum"] is True
    assert "vcp_setup_armed_before_breakout" not in row


def test_vcp_outside_arming_band_cannot_suppress_qualified_momentum(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    row = {
        **_row(94.0),
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "momentum_pass": True,
        "breakout_pass": False,
    }
    selected = _select_entry_style(
        row, pb1_score=.60, momentum_score=.90,
        pullback_score=.40, breakout_score=.50, vcp_score=.99,
    )
    assert selected == "momentum"
    assert "vcp" not in row["independent_eligible_entry_styles"]
    assert row["independent_proof_status"]["momentum"] is True
    assert "vcp_setup_armed_before_breakout" not in row
