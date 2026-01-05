from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd
import requests
from pykrx.stock import get_market_cap_by_ticker

from trader.universe.krx_safe import patch_pykrx_logging

logger = logging.getLogger(__name__)


def safe_get_market_cap_by_ticker(date_str: str, market: str) -> pd.DataFrame:
    patch_pykrx_logging()
    try:
        return get_market_cap_by_ticker(date_str, market=market)
    except (
        requests.exceptions.JSONDecodeError,
        json.JSONDecodeError,
        IndexError,
        KeyError,
        ValueError,
        requests.RequestException,
    ) as e:
        logger.warning("KRX fetch failed for %s %s: %s", market, date_str, repr(e))
        return pd.DataFrame()
    except Exception as e:  # pragma: no cover - unexpected edge cases
        logger.warning("KRX fetch failed for %s %s (unexpected): %s", market, date_str, repr(e))
        return pd.DataFrame()
