import time
import random
import pytest
from unittest.mock import Mock, patch

from trader.kis_wrapper import KisAPI


def test_get_quote_success():
    api = KisAPI()
    # Mock successful inquire-price with ask/bid
    with patch.object(api, "get_price_quote") as mock_price:
        mock_price.return_value = {
            "stck_prpr": "1000",
            "askp1": "1010",
            "bidp1": "990",
        }
        result = api.get_quote("005930")
        assert result == {"last": 1000, "ask": 1010, "bid": 990, "src": "inquire-price"}


def test_get_quote_fallback_hoga():
    api = KisAPI()
    # Mock inquire-price without ask/bid, then hoga success
    with patch.object(api, "get_price_quote") as mock_price, patch.object(
        api, "_get_hoga_quote"
    ) as mock_hoga:
        mock_price.return_value = {"stck_prpr": "1000"}  # No ask/bid
        mock_hoga.return_value = {"askp1": "1010", "bidp1": "990"}
        result = api.get_quote("005930")
        assert result == {"last": 1000, "ask": 1010, "bid": 990, "src": "hoga-fallback"}


def test_get_quote_hoga_only():
    api = KisAPI()
    # Mock inquire-price fail, hoga success
    with patch.object(api, "get_price_quote", side_effect=Exception("Fail")), patch.object(
        api, "_get_hoga_quote"
    ) as mock_hoga:
        mock_hoga.return_value = {"stck_prpr": "1000", "askp1": "1010", "bidp1": "990"}
        result = api.get_quote("005930")
        assert result == {"last": 1000, "ask": 1010, "bid": 990, "src": "hoga-only"}


def test_rate_limit_backoff_example():
    """
    This is a unit-test example of "backoff sleeps" behavior.
    It does NOT call the real KIS APIs.

    If you later implement logic like:
      - on rate-limit response -> sleep(base + jitter)
    then this test can be adapted to assert the exact sleep seconds.
    """
    with patch("time.sleep") as mock_sleep, patch("random.uniform", return_value=3.0):
        limiter = Mock()
        limiter.acquire.side_effect = [0.0, 0.0, 0.0, 2.0]  # 4th call suggests waiting

        # simulate typical limiter usage
        for _ in range(4):
            wait = limiter.acquire()
            if wait > 0:
                time.sleep(wait + random.uniform(0.0, 1.0))

        # wait=2.0 + uniform=3.0 => sleep(5.0)
        mock_sleep.assert_called_once_with(5.0)
