import pytest
from trader.kr.infinite.config import InfiniteConfig
def test_defaults_are_safe(monkeypatch):
 monkeypatch.delenv("KR_INFINITE_ENABLED",raising=False);c=InfiniteConfig.from_env();assert c.enabled and c.symbol=="122630"
def test_emergency_disable(monkeypatch):
 monkeypatch.setenv("KR_INFINITE_ENABLED","0");assert not InfiniteConfig.from_env().enabled
def test_inherits_real_session_gates():
 c=InfiniteConfig()
 armed={"STRATEGY_MODE":"LIVE","DRY_RUN":"0","DISABLE_LIVE_TRADING":"0","LIVE_TRADING_ENABLED":"1","KR_LIVE_TRADING_ENABLED":"1","KR_ORDER_ARMED":"1"}
 assert c.orders_allowed("practice",{}) and c.orders_allowed("real",armed)
 assert not c.orders_allowed("real",{**armed,"KR_ORDER_ARMED":"0"})
def test_configured_symbol_is_allowed_but_malformed_symbol_fails_closed():
 InfiniteConfig(symbol="005930").validate()
 with pytest.raises(ValueError,match="INVALID_SYMBOL"):InfiniteConfig(symbol="ABC").validate()
 with pytest.raises(ValueError):InfiniteConfig(total_units=41).validate()
