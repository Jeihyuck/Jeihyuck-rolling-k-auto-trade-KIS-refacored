from __future__ import annotations

from trader.pb1_engine import PB1Engine
from trader.kr.pb1.exit_family import normalize_entry_reason, resolve_exit_family


def test_resolve_exit_family_maps_breakout_pullback_momentum():
    assert normalize_entry_reason("BREAKOUT") == "ENTRY_BREAKOUT"
    assert normalize_entry_reason("ENTRY_BREAKOUT_CONFIRMED") == "ENTRY_BREAKOUT"
    assert normalize_entry_reason("PULLBACK") == "ENTRY_PULLBACK"
    assert normalize_entry_reason("ENTRY_PULLBACK_OVERRIDE") == "ENTRY_PULLBACK"
    assert normalize_entry_reason("MOMENTUM") == "ENTRY_MOMENTUM"
    assert normalize_entry_reason("ENTRY_MOMENTUM_CONTINUATION") == "ENTRY_MOMENTUM"
    assert normalize_entry_reason(None) == "ENTRY_GENERIC"
    assert normalize_entry_reason("") == "ENTRY_GENERIC"

    assert resolve_exit_family("ENTRY_BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert resolve_exit_family(None, "ENTRY_PULLBACK") == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert resolve_exit_family("ENTRY_MOMENTUM", "ENTRY_PULLBACK") == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert resolve_exit_family("BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert resolve_exit_family("ENTRY_BREAKOUT_CONFIRMED", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert resolve_exit_family("PULLBACK", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert resolve_exit_family("ENTRY_PULLBACK_OVERRIDE", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert resolve_exit_family("MOMENTUM", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert resolve_exit_family("ENTRY_MOMENTUM_CONTINUATION", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")


def test_resolve_exit_family_falls_back_to_generic_exit():
    assert resolve_exit_family("other", None) == ("ENTRY_GENERIC", "GENERIC_EXIT")
    assert resolve_exit_family(None, None) == ("ENTRY_GENERIC", "GENERIC_EXIT")


def test_pb1engine_resolve_exit_family_preserves_baseline_aliases():
    assert PB1Engine._resolve_exit_family("BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_BREAKOUT_CONFIRMED", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert PB1Engine._resolve_exit_family("PULLBACK", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_PULLBACK", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_PULLBACK_OVERRIDE", None) == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert PB1Engine._resolve_exit_family("MOMENTUM", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_MOMENTUM", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert PB1Engine._resolve_exit_family("ENTRY_MOMENTUM_CONTINUATION", None) == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")
    assert PB1Engine._resolve_exit_family("other", None) == ("ENTRY_GENERIC", "GENERIC_EXIT")
    assert PB1Engine._resolve_exit_family(None, None) == ("ENTRY_GENERIC", "GENERIC_EXIT")
