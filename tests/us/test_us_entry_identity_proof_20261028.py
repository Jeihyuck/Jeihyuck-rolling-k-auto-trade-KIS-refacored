"""2026-10-27 US PB1 KR/7 pullbacks: identity is not score argmax."""
from trader.us.pb1.entry_identity_proof import build_entry_identity_proof


def test_kr_momentum_remains_momentum_despite_higher_pullback_score():
    meta = {"entry_style_selected": "ENTRY_MOMENTUM", "momentum_score": 0.4,
            "pullback_score": 0.6, "breakout_score": 0.4,
            "entry_signal_proof_source": "completed_daily_ohlcv"}
    intent = {"strategy_owner": "US_STANDARD", "entry_strategy": "us_pb1",
              "entry_signal_type": "momentum", "trade_date": "2026-10-27",
              "position_action": "NEW_POSITION_BUY"}
    result = build_entry_identity_proof(meta, intent=intent, observed_price=65.99)
    assert result["selected_style"] == "ENTRY_MOMENTUM"
    assert result["selected_below_score_max"] is True
    assert result["selected_score"] == 0.4
    assert result["max_component_score"] == 0.6
    assert result["observed_trigger"] is None
    assert result["volume_proof_status"] == "UNVERIFIED"


def test_pullback_score_does_not_imply_verified_volume():
    intent = {"strategy_owner": "US_STANDARD", "entry_strategy": "us_pb1",
              "entry_signal_type": "pullback"}
    result = build_entry_identity_proof(
        {"entry_style_selected": "ENTRY_PULLBACK", "pullback_score": 0.9},
        intent=intent, observed_price=232.74,
    )
    assert result["selected_score"] == 0.9
    assert result["selected_below_score_max"] is False
    assert result["volume_proof_status"] == "UNVERIFIED"
    assert result["trigger_timestamp"] is None


def test_invalid_score_is_unknown_not_zero():
    intent = {"strategy_owner": "US_STANDARD", "entry_strategy": "us_pb1",
              "entry_signal_type": "momentum"}
    result = build_entry_identity_proof(
        {"entry_style_selected": "ENTRY_MOMENTUM",
         "momentum_score": float("nan"), "pullback_score": "invalid"},
        intent=intent,
    )
    assert result["selected_score"] is None
    assert result["max_component_score"] is None


def test_owner_is_not_inferred_from_signal_style():
    intent = {"strategy_owner": "TQQQ_INFINITE", "entry_strategy": "other"}
    result = build_entry_identity_proof(
        {"entry_style_selected": "ENTRY_PULLBACK"}, intent=intent
    )
    assert result["strategy_owner"] == "TQQQ_INFINITE"
