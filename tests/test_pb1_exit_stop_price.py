from __future__ import annotations

from trader.kr.pb1.exit_stop_price import resolve_exit_stop_price
from trader.pb1_engine import CandidateFeature, PB1Engine


def _candidate(**features):
    return CandidateFeature(
        code="032830",
        market="KOSPI",
        features=features,
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
    )


def test_resolve_exit_stop_price_uses_entry_metadata_first() -> None:
    result = resolve_exit_stop_price(
        code="032830",
        close_px=231500.0,
        atr_val=15617.9,
        ma20=None,
        ma50=None,
        stop_price_at_entry=220000.0,
        pivot_val=None,
        tight_low=None,
        entry_style_selected="BREAKOUT",
        order_px=231500.0,
    )

    assert result == (220000.0, "entry_metadata", 0, None)


def test_resolve_exit_stop_price_uses_atr_fallback_for_momentum() -> None:
    result = resolve_exit_stop_price(
        code="032830",
        close_px=231500.0,
        atr_val=15617.9,
        ma20=None,
        ma50=None,
        stop_price_at_entry=None,
        pivot_val=None,
        tight_low=None,
        entry_style_selected="MOMENTUM",
        order_px=231500.0,
    )

    assert result[1] == "atr_fallback"
    assert result[0] is not None
    assert result[0] < 231500.0


def test_pb1engine_build_stop_price_wrapper_matches_helper() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    candidate = _candidate(
        close=231500.0,
        atr14=15617.9,
        ma20=None,
        ma50=None,
        entry_style_selected="BREAKOUT",
        stop_price_at_entry=220000.0,
    )

    helper = resolve_exit_stop_price(
        code=candidate.code,
        close_px=231500.0,
        atr_val=15617.9,
        ma20=None,
        ma50=None,
        stop_price_at_entry=220000.0,
        pivot_val=None,
        tight_low=None,
        entry_style_selected="BREAKOUT",
        order_px=231500.0,
    )
    wrapper = engine._build_stop_price(candidate, 231500.0)

    assert helper == wrapper == (220000.0, "entry_metadata", 0, None)
