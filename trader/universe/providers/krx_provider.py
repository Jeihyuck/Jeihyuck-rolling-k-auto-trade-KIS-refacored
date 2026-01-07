from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Iterable

import pandas as pd
import requests
from pykrx.stock import get_market_cap_by_ticker

from trader.universe.krx_safe import patch_pykrx_logging

logger = logging.getLogger(__name__)

REQUIRED_CAP_COLUMNS = ("시가총액", "시가 총액", "MKT_CAP")
NAME_COLUMNS = ("종목명", "Name", "name")
MAX_REPEAT_FAIL = int(os.getenv("KRX_MAX_REPEAT_FAIL", "5"))
MAX_ROLLBACK_DAYS = int(os.getenv("KRX_MAX_ROLLBACK_DAYS", "3"))
MAX_ATTEMPTS = int(os.getenv("KRX_MAX_ATTEMPTS", "5"))


class EmptyDataFrame(Exception):
    """Raised when pykrx returns an empty dataframe."""


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
    if not _has_any_column(df, NAME_COLUMNS):
        return False
    return True


def _log_failure(market: str, requested_as_of: str, used_as_of: str, attempt_idx: int, total_attempts: int, exc: Exception, df: pd.DataFrame | None = None) -> None:
    preview = ""
    try:
        if df is not None and not df.empty:
            cols = list(df.columns)
            preview = f"cols={cols[:10]}"
    except Exception:
        preview = ""
    logger.warning(
        "[KRX][FAIL] market=%s requested_as_of=%s used_as_of=%s attempt=%s/%s exc=%s msg=%s %s",
        market,
        requested_as_of,
        used_as_of,
        attempt_idx,
        total_attempts,
        exc.__class__.__name__,
        exc,
        preview,
    )


def fetch_with_rollback(market: str, as_of_date: str | date, max_rollback_days: int | None = None) -> tuple[pd.DataFrame, date, str]:
    patch_pykrx_logging()
    if os.getenv("KRX_DISABLE", "0") in {"1", "true", "TRUE"}:
        raise RuntimeError("KRX_DISABLE=1")
    if isinstance(as_of_date, str):
        try:
            base_date = datetime.fromisoformat(as_of_date).date()
        except Exception:
            base_date = datetime.strptime(as_of_date, "%Y%m%d").date()
    else:
        base_date = as_of_date

    max_rollback_days = MAX_ROLLBACK_DAYS if max_rollback_days is None else max_rollback_days
    attempts = [base_date]
    cursor = base_date
    rollback_steps = 0
    while rollback_steps < max(0, int(max_rollback_days)) and len(attempts) < max(1, MAX_ATTEMPTS):
        cursor = _prev_business_day(cursor)
        attempts.append(cursor)
        rollback_steps += 1

    requested_as_of = base_date.isoformat()
    last_reason = "unknown"
    failure_counts: dict[str, int] = {}
    for idx, attempt in enumerate(attempts, start=1):
        used_as_of = attempt.isoformat()
        try:
            df = safe_get_market_cap_by_ticker(attempt.strftime("%Y%m%d"), market=market)
            if not _validate_krx_df(df):
                logger.warning(
                    "[KRX][NO_DATA] market=%s requested_as_of=%s used_as_of=%s cols=%s rows=%s",
                    market,
                    requested_as_of,
                    used_as_of,
                    list(df.columns),
                    len(df) if df is not None else 0,
                )
                raise EmptyDataFrame("empty_or_missing_cols")
            used_reason = "ok" if attempt == base_date else "rolled_back"
            logger.info(
                "[KRX][ROLLBACK][OK] market=%s used_as_of=%s requested_as_of=%s rows=%s attempts=%s reason=%s",
                market,
                used_as_of,
                requested_as_of,
                len(df) if df is not None else 0,
                idx,
                len(attempts),
                used_reason,
            )
            return df, attempt, used_reason
        except (
            ValueError,
            KeyError,
            json.JSONDecodeError,
            requests.exceptions.JSONDecodeError,
            requests.RequestException,
            EmptyDataFrame,
        ) as exc:  # pragma: no cover - network/remote failure
            last_reason = exc.__class__.__name__
            failure_counts[last_reason] = failure_counts.get(last_reason, 0) + 1
            _log_failure(market, requested_as_of, used_as_of, idx, len(attempts), exc)
            if failure_counts[last_reason] >= MAX_REPEAT_FAIL:
                logger.warning(
                    "[KRX][FAIL][ABORT] market=%s requested_as_of=%s exc=%s repeat=%s limit=%s",
                    market,
                    requested_as_of,
                    last_reason,
                    failure_counts[last_reason],
                    MAX_REPEAT_FAIL,
                )
                raise
            continue

    raise RuntimeError(
        f"KRX fetch failed after rollback attempts for {market} requested_as_of={requested_as_of} last_reason={last_reason}"
    )
