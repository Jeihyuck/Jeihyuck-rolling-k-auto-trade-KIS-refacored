import importlib
import os
import sys


def test_kr_volume_thresholds_unified_to_125(monkeypatch):
    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("REGION", "KR")
    monkeypatch.setenv("KR_PB1_VOL_MAX", "1.25")
    monkeypatch.setenv("KR_PB1_VOLU_MAX", "1.25")
    monkeypatch.setenv("KR_PB1_VOLU_MAX_INTRADAY", "1.25")
    sys.modules.pop("trader.config", None)
    cfg = importlib.import_module("trader.config")
    assert cfg.PB1_VOL_MAX == 1.25
    assert cfg.PB1_VOLU_MAX == 1.25
    assert cfg.PB1_VOLU_MAX_INTRADAY == 1.25
