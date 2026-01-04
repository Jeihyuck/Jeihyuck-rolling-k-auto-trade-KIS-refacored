import logging
from typing import List

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


class WatchItem(BaseModel):
    code: str
    name: str
    best_k: float


def _run_filter(date: str) -> List[WatchItem]:
    """
    TODO: 실제 리밸런싱 로직으로 대체.
    현재는 빈 리스트를 반환해 '종목 없음' 시나리오만 테스트합니다.
    """
    return []


@router.get(
    "/rebalance/watchlist/{date}",
    response_model=List[WatchItem],
    tags=["Rebalance"],
)
async def get_watchlist(date: str):
    logger.info("📅 watchlist 요청: %s", date)
    watchlist = _run_filter(date)
    if not watchlist:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"필터 통과 종목 없음 ({date})",
        )
    return watchlist
