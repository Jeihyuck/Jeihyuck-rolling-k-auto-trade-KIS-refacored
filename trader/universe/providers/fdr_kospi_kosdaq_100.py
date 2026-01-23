from __future__ import annotations

import logging
from collections import Counter
from typing import Iterable

import pandas as pd

from trader.universe.providers.fdr_marketcap_top import (
    CODE_COLUMNS,
    MARKET_CAP_COLUMNS,
    MARKET_COLUMNS,
    NAME_COLUMNS,
    _filter_market,
    _find_column,
    _normalize_code,
)

logger = logging.getLogger(__name__)

STATUS_COLUMNS = (
    "거래정지",
    "거래정지여부",
    "관리종목",
    "관리종목여부",
    "상장폐지",
    "상장폐지여부",
    "정리매매",
    "정리매매여부",
    "투자주의",
    "투자경고",
    "투자위험",
    "투자주의종목",
    "투자경고종목",
    "투자위험종목",
)


def _is_flagged(row: pd.Series, columns: Iterable[str]) -> bool:
    for col in columns:
        if col not in row:
            continue
        val = row.get(col)
        if val is None:
            continue
        if isinstance(val, (int, float)) and val != 0:
            return True
        if isinstance(val, str) and val.strip() and val.strip().upper() not in {"N", "NO", "0", "FALSE", "F"}:
            return True
        if isinstance(val, bool) and val:
            return True
    return False


def _normalize_rows(
    df: pd.DataFrame,
    *,
    code_col: str,
    name_col: str | None,
    cap_col: str,
    market_col: str,
    market_name: str,
    target: int,
) -> tuple[list[dict], Counter[str]]:
    rows: list[dict] = []
    dropped: Counter[str] = Counter()
    seen: set[str] = set()
    status_cols = [col for col in STATUS_COLUMNS if col in df.columns]
    for _, row in df.iterrows():
        if len(rows) >= target:
            break
        market_val = str(row.get(market_col) or "").upper()
        if market_name.upper() not in market_val:
            dropped["non_target_market"] += 1
            continue
        if status_cols and _is_flagged(row, status_cols):
            dropped["flagged"] += 1
            continue
        code = _normalize_code(row.get(code_col))
        if not code:
            dropped["invalid_code"] += 1
            continue
        try:
            marcap = float(row.get(cap_col))
        except Exception:
            marcap = 0.0
        if pd.isna(marcap) or marcap <= 0:
            dropped["nan_marcap"] += 1
            continue
        if code in seen:
            dropped["dup_code"] += 1
            continue
        seen.add(code)
        name = None
        if name_col:
            raw_name = row.get(name_col)
            if raw_name is not None:
                name = str(raw_name).strip() or None
        rows.append({"code": code, "name": name})
    return rows, dropped


def fetch_kospi100_kosdaq100(*, target: int = 100) -> dict[str, list[dict]]:
    """Return top 100 market-cap rows for KOSPI/KOSDAQ using FinanceDataReader."""
    try:
        import FinanceDataReader as fdr
    except Exception:
        logger.exception("[UNIVERSE][FDR-KOSPI100] FinanceDataReader import failed")
        raise

    logger.info("[UNIVERSE][FDR-KOSPI100][START] target=%s", target)
    listing = fdr.StockListing("KRX")
    logger.info("[UNIVERSE][FDR-KOSPI100][LISTING] rows=%s cols=%s", len(listing), list(listing.columns))

    market_col = _find_column(listing.columns, MARKET_COLUMNS, "market")
    cap_col = _find_column(listing.columns, MARKET_CAP_COLUMNS, "market cap")
    code_col = _find_column(listing.columns, CODE_COLUMNS, "code")
    name_col = None
    try:
        name_col = _find_column(listing.columns, NAME_COLUMNS, "name")
    except RuntimeError:
        name_col = None

    results: dict[str, list[dict]] = {}
    for market_name in ("KOSPI", "KOSDAQ"):
        market_df = _filter_market(listing, market_col, market_name)
        market_df[cap_col] = pd.to_numeric(market_df[cap_col], errors="coerce")
        market_df = market_df.sort_values(cap_col, ascending=False)
        candidate_limit = max(target * 3, target)
        logger.info(
            "[UNIVERSE][FDR-KOSPI100][SORT] market=%s column=%s order=desc target=%s candidate_limit=%s",
            market_name,
            cap_col,
            target,
            candidate_limit,
        )
        top_df = market_df.head(candidate_limit)
        rows, dropped = _normalize_rows(
            top_df,
            code_col=code_col,
            name_col=name_col,
            cap_col=cap_col,
            market_col=market_col,
            market_name=market_name,
            target=target,
        )
        sample_codes = [row["code"] for row in rows[:5]]
        logger.info("[UNIVERSE][FDR-KOSPI100][SAMPLE] market=%s codes=%s", market_name, sample_codes)
        logger.info("[UNIVERSE][FDR-KOSPI100][SELECTED] market=%s count=%s", market_name, len(rows))
        if len(rows) < target:
            logger.warning(
                "[UNIVERSE][FDR-KOSPI100][FILLDOWN][WARN] market=%s target=%s selected=%s dropped=%s",
                market_name,
                target,
                len(rows),
                dict(dropped),
            )
        results[market_name] = rows

    total_count = sum(len(rows) for rows in results.values())
    logger.info("[UNIVERSE][FDR-KOSPI100][DONE] total=%s", total_count)
    return results
