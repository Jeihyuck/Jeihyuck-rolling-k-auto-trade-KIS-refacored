# rolling_k_auto_trade_api/main.py
"""
FastAPI entrypoint
- 환경변수 검증(필수값 누락 시 즉시 실패)
- 예외 핸들러/헬스체크
- 리밸런스 라우터 포함
- 매수/매도 엔드포인트(order_cash 사용)
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

# 로깅 설정 (logging_config가 있으면 사용, 없으면 기본)
try:
    from logging_config import setup_logging  # type: ignore
except Exception:
    setup_logging = None

if setup_logging:
    setup_logging()
else:
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s')

logger = logging.getLogger(__name__)

# 설정 로드 및 필수값 검증
from settings import APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD, KIS_ENV, API_BASE_URL

# 라우터
try:
    from rolling_k_auto_trade_api.rebalance_api import rebalance_router  # 프로젝트 구조에 맞게 경로 유지
except Exception as e:
    rebalance_router = None
    logger.warning("[router] rebalance_router import failed: %s", e)

# KisAPI 구현 (kis_api 모듈 레벨 래퍼도 존재하지만, 여기선 직접 클래스 사용)
from rolling_k_auto_trade_api.kis_wrapper import KisAPI

app = FastAPI(title="Rolling-K Realtime Trade API", version="1.0.0")


# -----------------------------
# Startup: 환경변수 검증 & 부팅 로그
# -----------------------------
@app.on_event("startup")
def _startup_check() -> None:
    missing = [k for k, v in {
        "KIS_APP_KEY": APP_KEY,
        "KIS_APP_SECRET": APP_SECRET,
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
    }.items() if not v]
    if missing:
        # 즉시 실패하여 GitHub Actions에서도 원인 파악이 쉬움
        raise RuntimeError(f"[CONFIG] Missing env vars at startup: {missing}")

    logger.info("[boot] KIS_ENV=%s API_BASE_URL=%s", KIS_ENV, API_BASE_URL)


# -----------------------------
# 전역 예외 미들웨어
# -----------------------------
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except HTTPException:
        raise  # 이미 적절한 상태코드 포함
    except Exception as e:
        logger.exception("[UNHANDLED] %s %s", request.method, request.url.path)
        try:
            body = await request.body()
            logger.debug("[REQ_BODY] %s", body.decode("utf-8", "ignore"))
        except Exception:
            pass
        return JSONResponse(status_code=500, content={"error": "서버 내부 오류 발생"})


# -----------------------------
# 헬스체크/진단
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok", "env": KIS_ENV}


# -----------------------------
# 리밸런스 라우터 등록
# -----------------------------
if rebalance_router is not None:
    app.include_router(rebalance_router)
else:
    logger.warning("[router] rebalance_router not mounted.")


# -----------------------------
# 매수/매도 엔드포인트
# -----------------------------
class OrderRequest(BaseModel):
    code: str = Field(..., description="종목 코드 (예: 005930 또는 A005930)")
    qty: int = Field(..., gt=0, description="주문 수량")
    price: Optional[float] = Field(None, description="가격. 지정가일 때만 사용")
    order_type: Optional[str] = Field(None, description="KIS 주문유형 코드. 없으면 price 유무로 자동 결정")
    tr_id: Optional[str] = Field(None, description="TR_ID override (일반적으로 자동)")

    @validator("code")
    def _strip_code(cls, v: str) -> str:
        return v.strip()

kis = KisAPI()


def _resolve_order_type(price: Optional[float], order_type: Optional[str]) -> str:
    """주문유형 코드 결정: 명시값 우선, 없으면 price 유무로 자동(시장가=01/지정가=00)"""
    if order_type:
        return order_type
    return "00" if (price and float(price) > 0) else "01"


@app.post("/buy-order/")
def buy_order(req: OrderRequest):
    try:
        ord_type = _resolve_order_type(req.price, req.order_type)
        result = kis.order_cash(code=req.code, qty=req.qty, side="BUY", price=req.price or 0.0,
                                order_type=ord_type, tr_id=req.tr_id)
        if result is None:
            raise RuntimeError("주문 실패(응답 없음/비정상)" )
        return {"ok": True, "result": result}
    except Exception as e:
        logger.error("[BUY_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/sell-order/")
def sell_order(req: OrderRequest):
    try:
        ord_type = _resolve_order_type(req.price, req.order_type)
        result = kis.order_cash(code=req.code, qty=req.qty, side="SELL", price=req.price or 0.0,
                                order_type=ord_type, tr_id=req.tr_id)
        if result is None:
            raise RuntimeError("주문 실패(응답 없음/비정상)")
        return {"ok": True, "result": result}
    except Exception as e:
        logger.error("[SELL_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))
