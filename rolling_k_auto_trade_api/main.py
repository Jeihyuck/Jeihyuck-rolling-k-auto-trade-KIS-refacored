# -*- coding: utf-8 -*-
# main.py — FastAPI 진입점
# - 전역 로깅 적용(logging_config)
# - 예외 미들웨어(요청 본문까지 로깅)
# - /rebalance/run/{date} : 리밸런싱 결과 스키마 보정(selected / selected_stocks)
# - /buy-order, /sell-order : KIS 래퍼 가드형 주문 우선 사용

import logging_config  # 루트에 위치. 이 한 줄로 전역 로깅 설정이 바로 적용됨
import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 선택적: 외부 라우터가 있으면 사용
try:
    from rolling_k_auto_trade_api.rebalance_api import rebalance_router as _rebalance_router  # type: ignore
except Exception:
    _rebalance_router = None  # 없으면 아래에서 동일 경로를 직접 구현

from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)

app = FastAPI(title="Rolling-K Auto Trade API", version="1.0.0")


# ========== 공통 예외 처리 미들웨어 ==========
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        logger.exception("Unhandled error: %s %s", request.method, request.url.path)
        try:
            body = await request.body()
            logger.debug("Request body: %s", body.decode("utf-8", "ignore"))
        except Exception:
            pass
        return JSONResponse(status_code=500, content={"error": "서버 내부 오류 발생"})


# ========== 헬스체크 ==========
@app.get("/health")
def health():
    return {"status": "ok"}


# ========== 리밸런싱 라우터 연결(있으면 사용) ==========
if _rebalance_router is not None:
    try:
        app.include_router(_rebalance_router)
        logger.info("[router] external rebalance_router included")
    except Exception as e:
        logger.warning("[router] include rebalance_router failed: %s", e)


# ========== 리밸런싱 엔드포인트 (Fallback 구현) ==========
def _normalize_selected(items: Any) -> List[Dict[str, Any]]:
    """
    리밸런싱 결과를 trader.py가 기대하는 스키마로 정규화:
    - code | stock_code
    - name | 종목명
    - best_k | K | k
    - qty | 매수수량  (없으면 weight 사용)
    - prev_open, prev_high, prev_low, prev_close, prev_volume
    - target_price | 목표가 (없어도 OK: trader.py에서 계산)
    - close (선택)
    - strategy (선택)
    """
    if not isinstance(items, list):
        return []

    normed: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue

        code = it.get("code") or it.get("stock_code")
        name = it.get("name") or it.get("종목명")

        # 수량/가중치
        qty = it.get("qty")
        if qty is None:
            qty = it.get("매수수량")
        weight = it.get("weight")

        # k 값
        k_val = it.get("best_k")
        if k_val is None:
            k_val = it.get("K", it.get("k"))

        # 캔들 백업값 (없으면 None으로 채움)
        prev_open = it.get("prev_open")
        prev_high = it.get("prev_high")
        prev_low = it.get("prev_low")
        prev_close = it.get("prev_close")
        prev_volume = it.get("prev_volume")

        target_price = it.get("target_price", it.get("목표가"))

        # close(선택) / strategy(선택)
        close_px = it.get("close", it.get("prev_close"))
        strategy = it.get("strategy", "전월 rolling K 최적화")

        normed.append({
            "code": code,
            "name": name,
            "best_k": k_val,
            "qty": qty,
            "weight": weight,
            "prev_open": prev_open,
            "prev_high": prev_high,
            "prev_low": prev_low,
            "prev_close": prev_close,
            "prev_volume": prev_volume,
            "target_price": target_price,
            "close": close_px,
            "strategy": strategy,
        })
    return normed


def _run_strategy(date: str, force_order: bool = False) -> Dict[str, Any]:
    """
    실제 리밸런싱 생성 로직 호출(있으면 사용).
    - strategies.best_k_meta_strategy.run_rebalance(date, force_order) 기대
    - 없으면 빈 결과 반환(스키마는 유지)
    """
    try:
        from strategies.best_k_meta_strategy import run_rebalance  # type: ignore
    except Exception as e:
        logger.warning("[rebalance] strategy not found, return empty. err=%s", e)
        return {"selected": []}

    try:
        raw = run_rebalance(date=date, force_order=force_order)  # 구현에 따라 dict/list 지원
    except TypeError:
        # 오래된 시그니처 호환: run_rebalance(date)
        raw = run_rebalance(date)

    # raw가 리스트면 selected로 감싸기
    if isinstance(raw, list):
        selected = raw
    elif isinstance(raw, dict):
        selected = raw.get("selected") or raw.get("selected_stocks") or []
    else:
        selected = []

    normalized = _normalize_selected(selected)

    # API 계약: selected/selected_stocks 둘 다 제공(호환성)
    return {"selected": normalized, "selected_stocks": normalized}


# 외부 라우터가 동일 경로를 제공하지 않을 때만 Fallback 등록
_NEED_FALLBACK_REBALANCE = True
try:
    # FastAPI 내부 라우트 테이블을 훑어서 중복 경로 있는지 확인
    for r in app.router.routes:
        if getattr(r, "path", "") == "/rebalance/run/{date}":
            _NEED_FALLBACK_REBALANCE = False
            break
except Exception:
    _NEED_FALLBACK_REBALANCE = True


if _NEED_FALLBACK_REBALANCE:
    @app.post("/rebalance/run/{date}")
    def run_rebalance(date: str, force_order: bool = Query(False, description="주문 강제 여부(서버 내부 로직 용)")):
        """
        리밸런싱 실행(API 계약):
        응답은 `selected` 혹은 `selected_stocks`(동일 배열) 포함.
        각 원소는 code/name/best_k/qty(or weight)/prev_* 필드를 갖도록 보정됨.
        """
        try:
            result = _run_strategy(date=date, force_order=force_order)
            logger.info("[rebalance] %s -> %d stocks", date, len(result.get("selected", [])))
            return result
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("[rebalance] failed: %s", e)
            raise HTTPException(status_code=500, detail=f"rebalance failed: {e}")


# ========== 주문 모델/엔드포인트 ==========
class OrderRequest(BaseModel):
    code: str
    qty: int
    price: Optional[float] = None  # None이면 시장가


kis = KisAPI()


@app.post("/buy-order/")
def buy_order(req: OrderRequest):
    """
    매수 주문:
    - price가 주어지면: buy_stock_limit_guarded 우선, 없으면 buy_stock_market_guarded
    - 가드형 API 미지원 환경은 기본 buy_stock 호출로 폴백
    """
    try:
        code = req.code.strip()
        qty = int(req.qty)
        if qty <= 0:
            raise ValueError("qty must be > 0")

        if req.price is not None and float(req.price) > 0:
            # 지정가 + 예산 가드
            if hasattr(kis, "buy_stock_limit_guarded"):
                result = kis.buy_stock_limit_guarded(code, qty, int(float(req.price)))
            else:
                result = kis.buy_stock_limit(code, qty, int(float(req.price)))  # type: ignore
        else:
            # 시장가 + 예산 가드
            if hasattr(kis, "buy_stock_market_guarded"):
                result = kis.buy_stock_market_guarded(code, qty)
            elif hasattr(kis, "buy_stock_market"):
                result = kis.buy_stock_market(code, qty)  # type: ignore
            else:
                result = kis.buy_stock(code, qty, None)  # type: ignore

        return {"result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[BUY_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/sell-order/")
def sell_order(req: OrderRequest):
    """
    매도 주문:
    - price가 주어지면 지정가, 아니면 시장가
    - kis_wrapper의 보유수량 사전점검/중복매도 방지 로직이 적용됨
    """
    try:
        code = req.code.strip()
        qty = int(req.qty)
        if qty <= 0:
            raise ValueError("qty must be > 0")

        if req.price is not None and float(req.price) > 0:
            result = kis.sell_stock_limit(code, qty, int(float(req.price)))  # type: ignore
        else:
            if hasattr(kis, "sell_stock_market"):
                result = kis.sell_stock_market(code, qty)  # type: ignore
            else:
                result = kis.sell_stock(code, qty, None)  # type: ignore

        return {"result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[SELL_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))
