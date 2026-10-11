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


# Regressions added after integrating #205's final four-family arbiter.
from trader.us.four_family_arbitration import (
    eligible_family_scores, is_vcp_standby_only, rank_verified_families,
    verified_vcp_arming_eligibility, prioritize_qualified_buy_candidates,
)


def _arbiter_row(close=98.0, **extra):
    return {
        **_row(price=close),
        "score_final": .85,
        "pb1_score": .60,
        "momentum_score": .90,
        "breakout_score": .30,
        "vcp_score": .99,
        "independent_entry_contract_v1": True,
        "entry_signal_proof_source": "completed_daily_ohlcv",
        "pullback_pass": False,
        "momentum_pass": False,
        "breakout_pass": False,
        "reason_json": {},
        **extra,
    }


@pytest.mark.parametrize("close, expected", [(94., False), (95., True), (98., True), (100., True), (105., True), (105.10, False)])
def test_selector_and_final_arbiter_use_identical_near_pivot_rule(monkeypatch, close, expected):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "1")
    row = _arbiter_row(close=close)
    is_qualified, is_standby = verified_vcp_arming_eligibility(row)
    assert is_qualified is expected
    assert is_standby is (expected and close < 100)
    style = _select_entry_style(row, pb1_score=.60, momentum_score=.50, pullback_score=.40, breakout_score=.50, vcp_score=.99)
    assert (style == "vcp") is expected
    scores = eligible_family_scores(row)
    assert ("vcp" in scores) is expected
    assert bool(row.get("vcp_setup_armed_before_breakout")) is (expected and close < 100)


def test_prebreakout_standby_does_not_override_ready_momentum_on_same_symbol(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "1")
    row = _arbiter_row(
        momentum_pass=True, standalone_momentum_score=.83,
    )
    retained, funnel = rank_verified_families([row])
    assert len(retained) == 1
    assert row["independent_eligible_entry_styles"] == ["momentum", "vcp"]
    assert row["entry_style_selected"] == "momentum"
    assert row["vcp_setup_armed_before_breakout"] is True
    assert is_vcp_standby_only(row) is False
    assert funnel["family_qualified"]["vcp"] == 1


def test_standby_only_vcp_can_enter_spare_capacity_but_never_proves_live_buy(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _arbiter_row()
    retained, _ = rank_verified_families([row])
    assert len(retained) == 1
    assert row["entry_style_selected"] == "vcp"
    assert is_vcp_standby_only(row) is True
    assert row["reason_json"]["vcp_setup_armed_before_breakout"] is True
    passed, reason, evidence = _verify_us_vcp_live_breakout(
        row, {"last": "99", "tvol": "200000", "source": "KIS_LIVE",
              "age_sec": 0, "quality": "fresh", "stale": False},
    )
    assert passed is False
    assert reason == "vcp_pivot_volume_or_chase_failed"


@pytest.mark.parametrize("field,value", [
    ("vcp_pass", False),
    ("trend_template_pass", False),
    ("vcp_evidence_source", "stale"),
    ("vcp_daily_avg_volume20", 0),
    ("pivot_price", None),
    ("close", "nan"),
])
def test_incomplete_vcp_cannot_displace_ready_pullback_in_arbiter(monkeypatch, field, value):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _arbiter_row(pullback_pass=True, pullback_completed_close=98.)
    row[field] = value
    qualified = eligible_family_scores(row)
    assert list(qualified) == ["pb1_pullback"]
    assert row.get("vcp_setup_armed_before_breakout") is None
    rank_verified_families([row])
    assert row["entry_style_selected"] == "pb1_pullback"


def test_arming_flag_off_keeps_existing_final_arbiter_qualification(monkeypatch):
    monkeypatch.delenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", raising=False)
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    row = _arbiter_row(close=94.)
    assert "vcp" in eligible_family_scores(row)
    assert row.get("vcp_setup_armed_before_breakout") is None


def test_standby_respects_top50_and_final30_active_capacity(monkeypatch):
    monkeypatch.setenv("US_VCP_PREBREAKOUT_ARMING_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    active = [
        _arbiter_row(
            close=101., symbol=f"ACTIVE{i}", score_final=.20 + i / 1000.,
            momentum_pass=True, standalone_momentum_score=.90,
        )
        for i in range(35)
    ]
    standby = [
        _arbiter_row(close=98., symbol=f"WAIT{i}", score_final=.99)
        for i in range(30)
    ]
    ranked, counts = rank_verified_families(active + standby)
    assert counts["selected"]["momentum"] == 35
    assert counts["selected"]["vcp"] == 30
    assert len(ranked) == 65
    candidate50 = prioritize_qualified_buy_candidates(ranked)[:50]
    assert sum(not is_vcp_standby_only(row) for row in candidate50) == 35
    # The final bucket selector receives only these active entries first.
    active_pool = [row for row in candidate50 if not is_vcp_standby_only(row)]
    assert len(active_pool) == 35
    assert all(row["entry_style_selected"] == "momentum" for row in active_pool)
    # Standby competes only after active entries are taken.
    assert len([row for row in candidate50 if is_vcp_standby_only(row)]) == 15
