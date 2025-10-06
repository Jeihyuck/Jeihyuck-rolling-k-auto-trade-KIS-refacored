import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# --- 로깅 설정 ---
try:
    # 패키지 내부 모듈 우선
    from rolling_k_auto_trade_api.logging_config import setup_logging  # type: ignore
except Exception:  # fallback (프로젝트 루트에 있을 때)
    from logging_config import setup_logging  # type: ignore

# 리밸런스 라우터 (signals-only)
from rolling_k_auto_trade_api.rebalance_api import rebalance_router

# KIS API 래퍼
from trader.kis_wrapper import KisAPI


# ──────────────────────────────────────────────────────────────
# 환경변수 표준화: 기존 KIS_* 별칭을 APP_KEY/APP_SECRET/CANO/ACNT_PRDT_CD로 매핑
# ──────────────────────────────────────────────────────────────

def _env_norm():
    # 표준 키들
    app_key = os.getenv("APP_KEY")
    app_secret = os.getenv("APP_SECRET")
    cano = os.getenv("CANO")
    prdt = os.getenv("ACNT_PRDT_CD")

    # 별칭(KIS_*)에서 끌어오기
    app_key = app_key or os.getenv("KIS_APP_KEY")
    app_secret = app_secret or os.getenv("KIS_APP_SECRET")
    cano = cano or os.getenv("KIS_CANO") or os.getenv("CANO")
    prdt = prdt or os.getenv("KIS_ACNT_PRDT_CD") or os.getenv("ACNT_PRDT_CD")

    # 표준화하여 내보내기
    if app_key:
        os.environ["APP_KEY"] = app_key
    if app_secret:
        os.environ["APP_SECRET"] = app_secret
    if cano:
        os.environ["CANO"] = cano
    if prdt:
        os.environ["ACNT_PRDT_CD"] = prdt


# 로깅 초기화 및 환경 정리
setup_logging()
_env_norm()
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# FastAPI 앱
# ──────────────────────────────────────────────────────────────
app = FastAPI()


# 공통 예외 핸들러 미들웨어
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        logger.exception(f"Unhandled error: {request.method} {request.url.path}")
        try:
            body = await request.body()
            logger.debug(f"Request body: {body.decode('utf-8', 'ignore')}")
        except Exception:
            pass
        return JSONResponse(status_code=500, content={"error": "서버 내부 오류 발생"})


# 리밸런스 라우터 등록
app.include_router(rebalance_router)


# ──────────────────────────────────────────────────────────────
# 주문 API (선택)
# ──────────────────────────────────────────────────────────────
class OrderRequest(BaseModel):
    code: str
    qty: int
    price: float | None = None


kis = KisAPI()


@app.post("/buy-order/")
def buy_order(req: OrderRequest):
    try:
        result = kis.buy_stock(req.code, req.qty, req.price)
        return {"result": result}
    except Exception as e:
        logger.error(f"[BUY_ORDER_FAIL] {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/sell-order/")
def sell_order(req: OrderRequest):
    try:
        result = kis.sell_stock(req.code, req.qty, req.price)
        return {"result": result}
    except Exception as e:
        logger.error(f"[SELL_ORDER_FAIL] {e}")
        raise HTTPException(status_code=400, detail=str(e))
