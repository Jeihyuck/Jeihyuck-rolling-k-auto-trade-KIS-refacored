from fastapi import APIRouter
from datetime import datetime, timedelta
import json, math, pathlib, logging, pandas as pd

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api import kis_api
from rolling_k_auto_trade_api.errors import DomainError


router = APIRouter(prefix="/rebalance", tags=["Rebalance (Watchlist)"])

TOTAL_CAPITAL = 10_000_000
WATCHLIST_DIR = pathlib.Path("watchlists")
WATCHLIST_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)


@router.get("/watchlist/{date_str}")
async def run_rebalance_watchlist(date_str: str):
    """
    전략 기반 종목 필터링 + 목표가 계산 + watchlist JSON 저장
    """
    try:
        rebalance_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise DomainError("날짜 형식은 YYYY-MM-DD 이어야 합니다.") from e

    raw = get_best_k_for_kosdaq_50(date_str)
    logger.info(f"[DEBUG] get_best_k_for_kosdaq_50 raw result len = {len(raw)}")

    filtered = [
        r
        for r in raw
        if r.get("avg_return_pct", 0) > 5
        and r.get("win_rate", 0) > 60
        and r.get("mdd_pct", 99) < 10
    ]

    if not filtered:
        logger.warning("필터 통과 종목이 없습니다. (%s)", date_str)
        raise DomainError("조건을 만족하는 종목이 없습니다.", 422)

    each_cap = TOTAL_CAPITAL // len(filtered)
    watchlist = []

    for rec in filtered:
        code = rec["code"]
        k_val = rec["best_k"]

        prev_ohlc = kis_api.get_price_data(
            code, (rebalance_date - timedelta(days=1)).strftime("%Y-%m-%d")
        )
        today_ohlc = kis_api.get_price_data(code, date_str)

        if not prev_ohlc or not today_ohlc:
            logger.error("OHLCV 누락 → %s", code)
            continue

        open_price = today_ohlc["open"]
        target = open_price + (prev_ohlc["high"] - prev_ohlc["low"]) * k_val
        qty = math.floor(each_cap / open_price)

        watchlist.append(
            {
                "code": code,
                "best_k": k_val,
                "target_price": round(target, 2),
                "open_price": open_price,
                "qty": qty,
                "bought": False,
            }
        )

    fp = WATCHLIST_DIR / f"{date_str}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(watchlist, f, indent=2, ensure_ascii=False)

    logger.info("[watchlist_ready] %s tickers=%d path=%s", date_str, len(watchlist), fp)

    return {
        "status": "watchlist_ready",
        "tickers": len(watchlist),
        "watchlist_file": str(fp),
    }
