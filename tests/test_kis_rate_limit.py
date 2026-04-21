import importlib
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trader.kis_wrapper as kis_wrapper


def test_load_price_policy_prefers_kis_specific_env(monkeypatch):
    monkeypatch.setenv("KIS_PRICE_MIN_INTERVAL_MS", "500")
    monkeypatch.setenv("KIS_PRICE_CACHE_TTL_SEC", "7")
    monkeypatch.setenv("KIS_RATE_LIMIT_COOLDOWN_SEC", "11")
    monkeypatch.setenv("KIS_PRICE_JITTER_MAX_SEC", "0.25")
    policy = kis_wrapper._load_price_policy_config()

    assert policy["ttl_sec"] == 7.0
    assert policy["qps"] == 2.0
    assert policy["circuit_sec"] == 11.0
    assert policy["jitter_sec"] == 0.25


def test_mark_price_rate_limited_opens_circuit(monkeypatch):
    importlib.reload(kis_wrapper)
    try:
        before = kis_wrapper._price_cache.circuit_until
        kis_wrapper._mark_price_rate_limited("inquire-price", "005930", "EGW00201", "too many")

        assert kis_wrapper._price_cache.circuit_until > before
        assert kis_wrapper.get_price_runtime_stats()["price_http_fail_count"] >= 1
    finally:
        importlib.reload(kis_wrapper)