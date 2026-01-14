from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)

MARKET_CAP_COLUMNS = ("Marcap", "MarketCap", "MARKET_CAP", "시가총액", "시가 총액")
MARKET_COLUMNS = ("Market", "시장", "MKT", "MarketName")
CODE_COLUMNS = ("Code", "Symbol", "종목코드", "종목코드(6자리)")
NAME_COLUMNS = ("Name", "종목명", "종목명(국문)", "StockName")


def _normalize_code(code: object) -> str:
    code_str = str(code).strip()
    if not code_str.isdigit():
        return ""
    return code_str.zfill(6)


def _find_column(columns: Iterable[str], candidates: Iterable[str], label: str) -> str:
    cols = list(columns)
    for candidate in candidates:
        if candidate in cols:
            return candidate
    raise RuntimeError(f"[UNIVERSE][FDR] {label} column not found in columns={cols}")


def _filter_market(df: pd.DataFrame, market_col: str, market_name: str) -> pd.DataFrame:
    market_series = df[market_col].astype(str).str.upper()
    return df[market_series.str.contains(market_name.upper(), na=False)].copy()


def _normalize_rows(df: pd.DataFrame, code_col: str, name_col: str | None) -> list[dict]:
    rows: list[dict] = []
    for _, row in df.iterrows():
        code = _normalize_code(row.get(code_col))
        if not code:
            continue
        name = None
        if name_col:
            raw_name = row.get(name_col)
            if raw_name is not None:
                name = str(raw_name).strip() or None
        rows.append({"code": code, "name": name})
    return rows


def fetch_marketcap_top(targets: dict[str, int]) -> dict[str, list[dict]]:
    """Return marketcap top rows for each market using FinanceDataReader."""
    try:
        import FinanceDataReader as fdr
    except Exception:
        logger.exception("[UNIVERSE][FDR] FinanceDataReader import failed")
        raise

    logger.info("[UNIVERSE][FDR][START] targets=%s", targets)
    listing = fdr.StockListing("KRX")
    logger.info("[UNIVERSE][FDR][LISTING] rows=%s cols=%s", len(listing), list(listing.columns))

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
        market_df[cap_col] = pd.to_numeric(market_df[cap_col], errors="coerce").fillna(0)
        market_df = market_df.sort_values(cap_col, ascending=False)
        limit = max(0, int(targets.get(market_name, 0)))
        logger.info("[UNIVERSE][FDR][SORT] market=%s column=%s order=desc limit=%s", market_name, cap_col, limit)
        top_df = market_df.head(limit)
        rows = _normalize_rows(top_df, code_col, name_col)
        sample_codes = [row["code"] for row in rows[:5]]
        logger.info("[UNIVERSE][FDR][SAMPLE] market=%s codes=%s", market_name, sample_codes)
        logger.info("[UNIVERSE][FDR][SELECTED] market=%s count=%s", market_name, len(rows))
        results[market_name] = rows

    total_count = sum(len(rows) for rows in results.values())
    logger.info("[UNIVERSE][FDR][DONE] total=%s", total_count)
    return results
