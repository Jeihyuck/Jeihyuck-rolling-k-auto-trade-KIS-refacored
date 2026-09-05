from __future__ import annotations

from trader.kr.pb1.exit_family import resolve_exit_family


def test_resolve_exit_family_maps_breakout_pullback_momentum():
    assert resolve_exit_family("ENTRY_BREAKOUT", None) == ("ENTRY_BREAKOUT", "BREAKOUT_EXIT")
    assert resolve_exit_family(None, "ENTRY_PULLBACK") == ("ENTRY_PULLBACK", "PULLBACK_EXIT")
    assert resolve_exit_family("ENTRY_MOMENTUM", "ENTRY_PULLBACK") == ("ENTRY_MOMENTUM", "MOMENTUM_EXIT")


def test_resolve_exit_family_falls_back_to_generic_exit():
    assert resolve_exit_family("other", None) == ("OTHER", "GENERIC_EXIT")
