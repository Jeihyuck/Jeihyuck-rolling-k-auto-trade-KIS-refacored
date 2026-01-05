from __future__ import annotations

import json
import logging
import time

import pandas as pd
import requests
from pykrx.stock import get_market_cap_by_ticker

logger = logging.getLogger(__name__)


class KRXTemporaryError(Exception):
    """Raised when KRX endpoints fail temporarily after retries."""


def _safe_pykrx_info(*args: object, **kwargs: object) -> None:
    """
    Replace pykrx util.logging.info to avoid TypeError on tuple formatting.

    pykrx occasionally calls logging.info(args, kwargs) which breaks the stdlib logger.
    We coerce arguments to strings and forward them to a dedicated logger.
    """
    message_parts = [str(a) for a in args if a is not None]
    if kwargs:
        message_parts.append(str(kwargs))
    message = " ".join(message_parts) if message_parts else ""
    logging.getLogger("pykrx").info("%s", message)


def patch_pykrx_logging() -> None:
    """Monkey-patch pykrx logging wrappers to avoid noisy formatting errors."""
    try:
        import pykrx.website.comm.util as util
    except Exception as exc:  # pragma: no cover - defensive import guard
        logger.debug("pykrx util import failed; skip logging patch: %s", exc)
        return
    try:
        util.logging.info = _safe_pykrx_info
    except Exception as exc:  # pragma: no cover - defensive assignment guard
        logger.debug("pykrx logging patch failed: %s", exc)


def safe_get_topn(market: str, date_str: str, n: int) -> pd.DataFrame:
    """
    Fetch market cap DataFrame with retries and safe logging.

    Raises:
        KRXTemporaryError: when pykrx repeatedly fails with transient errors.
    """
    patch_pykrx_logging()
    last_exc: Exception | None = None
    delay = 1.0
    for attempt in range(3):
        try:
            df = get_market_cap_by_ticker(date_str, market=market)
            if df is None:
                raise ValueError("pykrx returned None for market cap data")
            df = df.copy()
            if df.index.name is None:
                df.index.name = "code"
            df.index = df.index.astype(str).str.zfill(6)
            if df.empty:
                return df
            first_col = df.columns[0] if len(df.columns) else None
            if first_col:
                df = df.sort_values(first_col, ascending=False)
            return df.head(n)
        except (
            requests.exceptions.JSONDecodeError,
            json.JSONDecodeError,
            IndexError,
            KeyError,
            ValueError,
            requests.RequestException,
        ) as exc:
            last_exc = exc
        except Exception as exc:  # pragma: no cover - unexpected edge cases
            last_exc = exc
        if attempt < 2:
            time.sleep(delay)
            delay *= 2
    raise KRXTemporaryError(f"pykrx market cap fetch failed for {market} {date_str}") from last_exc
