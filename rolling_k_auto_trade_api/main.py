import os
import json
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from logging_config import setup_logging
from settings import (
    APP_KEY,
    APP_SECRET,
    API_BASE_URL,
    CANO,
    ACNT_PRDT_CD,
    KIS_ENV,
)
from rolling_k_auto_trade_api.rebalance_api import router as rebalance_router
from rolling_k_auto_trade_api.best_k_meta_strategy import router as strategy_router
from trader.kis_wrapper import KisAPI

# -----------------------------------------------------
# App & Logging
# -----------------------------------------------------
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Rolling-K Auto Trade API")


# -----------------------------------------------------
# Health / Root
# -----------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok", "env": KIS_ENV}


@app.get("/health")
def health():
    ok = bool(APP_KEY and APP_SECRET)
    return {
        "status": "ok" if ok else "degraded",
        "APP_KEY": bool(APP_KEY),
        "APP_SECRET": bool(APP_SECRET),
        "CANO": bool(CANO),
        "ACNT_PRDT_CD": bool(ACNT_PRDT_CD),
        "KIS_ENV": KIS_ENV,
        "API_BASE_URL": API_BASE_URL,
    }


# -----------------------------------------------------
# Routers (Strategy/Rebalance)
# -----------------------------------------------------
app.include_router(strategy_router)
app.include_router(rebalance_router)


# -----------------------------------------------------
# Compatibility for older workflow step
# -----------------------------------------------------
@app.post("/rebalance/run/{date}")
async def rebalance_run(date: str, force_generate: bool = False):
    from rolling_k_auto_trade_api.rebalance_api import generate_signals

    data = await generate_signals(date=date, force_generate=force_generate)
    return JSONResponse(content=data)


# -----------------------------------------------------
# ORDER API (legacy-compatible)
#   - Original endpoints kept: /buy_order, /sell_order
#   - New aliases added:      /order/buy, /order/sell
# -----------------------------------------------------
class OrderRequest(BaseModel):
    code: Optional[str] = None  # ticker like "293490" or "A293490"
    pdno: Optional[str] = None  # alias field used in some callers
    qty: int
    price: Optional[float] = None  # limit price; ignored on market
    order_type: Optional[str] = "market"  # "market" | "limit"


# --- Helpers ---
def _normalize_code(req: OrderRequest) -> str:
    code = (req.pdno or req.code or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="code/pdno is required")
    # KIS accepts both plain and A-prefixed. Keep as-is.
    return code


# --- BUY ---
@app.post("/buy_order")
async def buy_order(req: OrderRequest):
    api = KisAPI()
    code = _normalize_code(req)

    try:
        if (req.order_type or "market").lower() == "limit" and req.price and float(req.price) > 0:
            res = api.buy_stock_limit(code, int(req.qty), int(float(req.price)))
        else:
            res = api.buy_stock_market(code, int(req.qty))
    except Exception as e:
        logger.exception("[BUY_ORDER_EX] %s", e)
        raise HTTPException(status_code=500, detail=f"buy failed: {e}")

    return res or {"status": "fail", "message": "buy order rejected"}


@app.post("/order/buy")
async def order_buy(req: OrderRequest):
    return await buy_order(req)


# --- SELL ---
@app.post("/sell_order")
async def sell_order(req: OrderRequest):
    api = KisAPI()
    code = _normalize_code(req)

    try:
        if (req.order_type or "market").lower() == "limit" and req.price and float(req.price) > 0:
            res = api.sell_stock_limit(code, int(req.qty), int(float(req.price)))
        else:
            res = api.sell_stock_market(code, int(req.qty))
    except Exception as e:
        logger.exception("[SELL_ORDER_EX] %s", e)
        raise HTTPException(status_code=500, detail=f"sell failed: {e}")

    return res or {"status": "fail", "message": "sell order rejected"}


@app.post("/order/sell")
async def order_sell(req: OrderRequest):
    return await sell_order(req)


# -----------------------------------------------------
# Optional convenience endpoints (kept minimal)
# -----------------------------------------------------
@app.get("/balance")
async def balance():
    api = KisAPI()
    try:
        return api.get_balance()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"balance failed: {e}")

