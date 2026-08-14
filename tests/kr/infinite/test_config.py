import pytest
from trader.kr.infinite.config import InfiniteConfig
def test_defaults_are_safe(monkeypatch):
 monkeypatch.delenv("KR_INFINITE_ENABLED",raising=False);monkeypatch.delenv("KR_INFINITE_LIVE",raising=False);c=InfiniteConfig.from_env();assert not c.enabled and not c.live and c.symbol=="122630"
def test_invalid_symbol_and_units_fail_closed():
 with pytest.raises(ValueError,match="BLOCK_UNSUPPORTED_SYMBOL"):InfiniteConfig(symbol="005930").validate()
 with pytest.raises(ValueError):InfiniteConfig(total_units=41).validate()
