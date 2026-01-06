from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
import requests
from pykrx.stock import get_market_cap_by_ticker

from trader.universe.krx_safe import patch_pykrx_logging

logger = logging.getLogger(__name__)

REQUIRED_CAP_COLUMNS = ("시가총액", "시가 총액", "MKT_CAP")
NAME_COLUMNS = ("종목명", "Name", "name")


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


def _prev_business_day(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _has_any_column(df: pd.DataFrame, candidates: Iterable[str]) -> bool:
    cols = set(df.columns)
    return any(c in cols for c in candidates)


def _validate_krx_df(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    if not _has_any_column(df, REQUIRED_CAP_COLUMNS):
        return False
    return True


def fetch_with_rollback(market: str, as_of_date: str | date, max_rollback_days: int = 7) -> tuple[pd.DataFrame, date, str]:
    patch_pykrx_logging()
    if isinstance(as_of_date, str):
        try:
            base_date = datetime.fromisoformat(as_of_date).date()
        except Exception:
            base_date = datetime.strptime(as_of_date, "%Y%m%d").date()
    else:
        base_date = as_of_date

    attempts = [base_date]
    cursor = base_date
    for _ in range(max(0, int(max_rollback_days))):
        cursor = _prev_business_day(cursor)
        attempts.append(cursor)

    requested_as_of = base_date.isoformat()
    last_reason = "unknown"
    for idx, attempt in enumerate(attempts, start=1):
        try:
            df = get_market_cap_by_ticker(attempt.strftime("%Y%m%d"), market=market)
            if _validate_krx_df(df):
                used_reason = "ok" if attempt == base_date else "rolled_back"
                logger.info(
                    "[KRX][ROLLBACK][OK] market=%s used_as_of=%s requested_as_of=%s rows=%s attempts=%s reason=%s",
                    market,
                    attempt.isoformat(),
                    requested_as_of,
                    len(df) if df is not None else 0,
                    idx,
                    used_reason,
                )
                return df, attempt, used_reason
            last_reason = "empty_or_missing_cols"
            logger.warning(
                "[KRX][ROLLBACK][WARN] market=%s requested_as_of=%s used_as_of=%s attempt=%s/%s reason=%s",
                market,
                requested_as_of,
                attempt.isoformat(),
                idx,
                len(attempts),
                last_reason,
            )
        except (
            ValueError,
            KeyError,
            json.JSONDecodeError,
            requests.exceptions.JSONDecodeError,
            requests.RequestException,
            Exception,
        ) as exc:  # pragma: no cover - network/remote failure
            last_reason = exc.__class__.__name__
            logger.warning(
                "[KRX][ROLLBACK][WARN] market=%s requested_as_of=%s used_as_of=%s attempt=%s/%s reason=%s",
                market,
                requested_as_of,
                attempt.isoformat(),
                idx,
                len(attempts),
                last_reason,
            )
            continue

    raise RuntimeError(f"KRX fetch failed after rollback attempts for {market} requested_as_of={requested_as_of} last_reason={last_reason}")
