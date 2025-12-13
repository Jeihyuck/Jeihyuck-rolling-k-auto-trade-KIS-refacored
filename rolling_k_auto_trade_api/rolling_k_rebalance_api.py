import os
import json
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_krx_topn
from rolling_k_auto_trade_api.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

rolling_rebalance_router = APIRouter()

ROLLING_RATIO = float(os.getenv("ROLLING_RATIO", "0.3"))
TOP_K_LIMIT = int(os.getenv("TOP_K_LIMIT", "20"))
REBALANCE_OUT_DIR = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
REBALANCE_STORE = os.getenv("REBALANCE_STORE", "./data/selected_stocks.json")

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

@rolling_rebalance_router.post("/rebalance/rolling/{date}", tags=["RollingRebalance"])
async def run_rolling_rebalance(
    date: str,
    rolling_ratio: float = Query(ROLLING_RATIO, description="하위 몇 % 교체(0.0~1.0)"),
):
    """
    ROLLING(부분) 리밸런싱: 유니버스 중 하위 rolling_ratio만 교체, 신규 강세주 자동 편입
    """
    logger.info(f"[ROLLING] rolling_rebalance 호출: date={date}, rolling_ratio={rolling_ratio}")

    # 1) 기존 포트폴리오 불러오기
    try:
        with open(REBALANCE_STORE, "r", encoding="utf-8") as f:
            prev_data = json.load(f)
            old_universe = prev_data.get("selected_stocks", [])
    except Exception:
        old_universe = []

    old_codes = [s.get("code") or s.get("stock_code") for s in old_universe if (s.get("code") or s.get("stock_code"))]
    logger.info(f"[ROLLING] 기존 유니버스: {old_codes}")

    # 2) 전체 후보군 + score(수익률, 승률, 모멘텀 등) 재산출
    try:
        new_candidates = get_best_k_for_krx_topn(date)
    except Exception as e:
        logger.exception(f"[ERROR] Rolling BestK 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Rolling Best K 계산 실패")
    candidates_map = {}
    for s in new_candidates:
        code = s.get("stock_code") or s.get("code") or s.get("티커")
        if code:
            s["code"] = code
            candidates_map[code] = s

    # 3) 현재 포트 내 하위 rolling_ratio만 제거
    current_scored = [candidates_map.get(code, {}) for code in old_codes]
    valid_current = [c for c in current_scored if c]
    num_drop = int(len(valid_current) * rolling_ratio)
    sorted_current = sorted(valid_current, key=lambda x: float(x.get("avg_return_pct", 0)))
    codes_to_drop = set([c["code"] for c in sorted_current[:num_drop]])
    logger.info(f"[ROLLING] drop {num_drop}/{len(valid_current)}: {codes_to_drop}")

    # 4) 신규 상위 강세주 자동 편입 (Top K중 OUT 종목 우선)
    sorted_candidates = sorted(candidates_map.values(), key=lambda x: float(x.get("avg_return_pct", 0)), reverse=True)
    add_candidates = []
    for c in sorted_candidates:
        code = c["code"]
        if code not in old_codes:
            add_candidates.append(c)
        if len(add_candidates) >= num_drop:
            break

    # 5) 최종 유니버스 합산(유지+신규), 비중 재계산(균등 or 기존 weight 유지)
    kept = [c for c in valid_current if c["code"] not in codes_to_drop]
    final_universe = kept + add_candidates
    if len(final_universe) > TOP_K_LIMIT:
        final_universe = final_universe[:TOP_K_LIMIT]

    logger.info(f"[ROLLING] 최종 유니버스: {[c['code'] for c in final_universe]}")

    # 6) 저장 (selected_stocks.json 등 표준)
    _ensure_dir(REBALANCE_OUT_DIR)
    out_fp = os.path.join(REBALANCE_OUT_DIR, f"rebalance_rolling_{date}.json")
    with open(out_fp, "w", encoding="utf-8") as f:
        json.dump(final_universe, f, ensure_ascii=False, indent=2)
    # 표준 캐시도 저장
    with open(REBALANCE_STORE, "w", encoding="utf-8") as f:
        json.dump({"date": date, "selected_stocks": final_universe}, f, ensure_ascii=False, indent=2)

    return {
        "status": "rolling_rebalance_saved",
        "selected_count": len(final_universe),
        "selected_stocks": final_universe,
        "store": REBALANCE_STORE,
        "out_file": out_fp,
        "rolling_ratio": rolling_ratio,
    }
# TOTAL_LINES: 85
