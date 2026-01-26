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


def _normalize_code(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.extract(r"(\d+)")[0]
    s = s.fillna("")
    s = s.str.zfill(6)
    s = s.where(s.str.match(r"^\d{6}$"), "")
    return s


def _normalize_marcap(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(",", "", regex=False)
    s = s.replace({"": None, "nan": None, "NaN": None, "-": None})
    return pd.to_numeric(s, errors="coerce")


def _normalize_rows(
    df: pd.DataFrame,
    *,
    name_col: str | None,
) -> tuple[list[dict], Counter[str]]:
    rows: list[dict] = []
    dropped: Counter[str] = Counter()
    seen: set[str] = set()
    for _, row in df.iterrows():
        code = row.get("code_norm")
        if not code:
            dropped["invalid_code"] += 1
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
        raw_dtypes = market_df[[code_col, cap_col]].dtypes.astype(str).to_dict()
        logger.info("[UNIVERSE][FDR-KOSPI100][DTYPE] market=%s raw_dtype=%s", market_name, raw_dtypes)

        market_df = market_df.copy()
        market_df["code_norm"] = _normalize_code(market_df[code_col])
        market_df["marcap_norm"] = _normalize_marcap(market_df[cap_col])

        nan_rate = float(market_df["marcap_norm"].isna().mean()) if len(market_df) else 0.0
        invalid_code_rate = float((market_df["code_norm"] == "").mean()) if len(market_df) else 0.0

        sample_rows = []
        for _, row in market_df.head(5).iterrows():
            sample_rows.append(
                {
                    "code_raw": row.get(code_col),
                    "marcap_raw": row.get(cap_col),
                    "code_norm": row.get("code_norm"),
                    "marcap_norm": row.get("marcap_norm"),
                }
            )
        logger.info(
            "[UNIVERSE][FDR-KOSPI100][SAMPLE][RAW] market=%s samples=%s",
            market_name,
            sample_rows,
        )

        status_cols = [col for col in STATUS_COLUMNS if col in market_df.columns]
        if status_cols:
            market_df["flagged"] = market_df.apply(lambda row: _is_flagged(row, status_cols), axis=1)
        else:
            market_df["flagged"] = False

        invalid_code = int((market_df["code_norm"] == "").sum())
        nan_marcap = int((market_df["marcap_norm"].isna() | (market_df["marcap_norm"] <= 0)).sum())
        flagged = int(market_df["flagged"].sum())
        cleaned = market_df[
            (market_df["code_norm"] != "")
            & market_df["marcap_norm"].notna()
            & (market_df["marcap_norm"] > 0)
            & (~market_df["flagged"])
        ].copy()
        dup_before = len(cleaned)
        cleaned = cleaned.drop_duplicates(subset=["code_norm"], keep="first")
        dup_code = dup_before - len(cleaned)

        logger.info(
            "[UNIVERSE][FDR-KOSPI100][CLEAN] market=%s raw_rows=%s valid_rows=%s nan_marcap=%s invalid_code=%s nan_rate=%.4f invalid_rate=%.4f flagged=%s dup_code=%s",
            market_name,
            len(market_df),
            len(cleaned),
            nan_marcap,
            invalid_code,
            nan_rate,
            invalid_code_rate,
            flagged,
            dup_code,
        )

        cleaned = cleaned.sort_values("marcap_norm", ascending=False)
        candidate_limit = max(target * 3, target)
        candidate_limit = min(candidate_limit, len(cleaned))
        logger.info(
            "[UNIVERSE][FDR-KOSPI100][SORT] market=%s column=%s order=desc target=%s candidate_limit=%s",
            market_name,
            cap_col,
            target,
            candidate_limit,
        )

        selected_df = cleaned.head(candidate_limit)
        while len(selected_df) < target and candidate_limit < len(cleaned):
            candidate_limit = min(candidate_limit * 2, len(cleaned))
            logger.info(
                "[UNIVERSE][FDR-KOSPI100][FILLDOWN] market=%s expanded_candidate_limit=%s",
                market_name,
                candidate_limit,
            )
            selected_df = cleaned.head(candidate_limit)
        selected_df = selected_df.head(target)
        rows, dropped = _normalize_rows(
            selected_df,
            name_col=name_col,
        )
        dropped.update(
            {
                "nan_marcap": nan_marcap,
                "invalid_code": invalid_code,
                "flagged": flagged,
                "dup_code": dup_code,
            }
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
