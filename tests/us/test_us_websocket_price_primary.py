from __future__ import annotations

from pathlib import Path


def test_us_current_price_is_websocket_first_and_rest_fallback_preserved():
    source = Path("trader/us/data_provider.py").read_text(encoding="utf-8")
    ws_pos = source.index('ws_service.subscribe_us(symbol, exchange)')
    rest_pos = source.index('result = self._get_client().get_us_price(symbol, exchange)')
    assert ws_pos < rest_pos
    assert '"source": "KIS_WEBSOCKET"' in source
    assert "[US_DATA][FALLBACK_STALE]" in source


def test_us_global_rest_governor_remains_enabled_as_fallback():
    source = Path("trader/us/__init__.py").read_text(encoding="utf-8")
    assert "install_kis_http_governor()" in source
    governor = Path("trader/us/kis_http_governor.py").read_text(encoding="utf-8")
    assert 'US_KIS_PRACTICE_GLOBAL_MIN_INTERVAL_SEC' in governor


def test_us_offline_path_precedes_websocket():
    source = Path("trader/us/data_provider.py").read_text(encoding="utf-8")
    guard_pos = source.index("if not self._offline:")
    ws_pos = source.index("ws_service.subscribe_us(symbol, exchange)")
    rest_pos = source.index("result = self._get_client().get_us_price(symbol, exchange)")
    assert guard_pos < ws_pos < rest_pos
