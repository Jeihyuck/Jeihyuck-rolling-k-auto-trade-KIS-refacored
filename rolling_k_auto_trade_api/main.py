# -*- coding: utf-8 -*-
"""
FastAPI application entrypoint
- Robust logging import (supports both project-root and package path)
- Safe router mounting for rebalance API
- Exception middleware with request-body debug on failures
- Optional KisAPI buy/sell endpoints (lazy init + graceful degradation)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ──────────────────────────────────────────────────────────────
# Logging setup (support both `logging_config.setup_logging` and package path)
# ──────────────────────────────────────────────────────────────
try:
    # original path in your file
    from logging_config import setup_logging as _setup_logging  # type: ignore
except Exception:  # pragma: no cover
    # fallback to package path used by other modules
    from rolling_k_auto_trade_api.logging_config import (
        configure_logging as _setup_logging,  # type: ignore
    )

_setup_logging()
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# FastAPI app
# ──────────────────────────────────────────────────────────────
app = FastAPI(title="Rolling-K API", version="1.0.0")

# Mount rebalance router (signals-only)
from rolling_k_auto_trade_api.rebalance_api import rebalance_router  # noqa: E402

app.include_router(rebalance_router)

# ──────────────────────────────────────────────────────────────
# Global error middleware
# ──────────────────────────────────────────────────────────────
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception:
        logger.exception(
            "[UNHANDLED] %s %s", request.method, request.url.path
        )
        # Best-effort request body dump for debugging
        try:
            body = await request.body()
            logger.debug("[REQUEST BODY] %s", body.decode("utf-8", "ignore"))
        except Exception:
            pass
        return JSONResponse(status_code=500, content={"error": "서버 내부 오류 발생"})


# ──────────────────────────────────────────────────────────────
# Health endpoints (useful for CI readiness checks)
# ──────────────────────────────────────────────────────────────
@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    return {"status": "ready"}


# ──────────────────────────────────────────────────────────────
# Optional: Buy/Sell endpoints via KisAPI (lazy init)
#  - Won't break app startup if credentials or module are missing
# ──────────────────────────────────────────────────────────────
class OrderRequest(BaseModel):
    code: str
    qty: int
    price: Optional[float] = None


_KIS_IMPORT_ERROR: Optional[str] = None
_KIS_INSTANCE = None


def _have_kis_creds() -> bool:
    # Minimal check; adapt if your env var names differ
    return bool(os.getenv("CANO") and os.getenv("ACNT_PRDT_CD") and os.getenv("KIS_APP_KEY"))


def _get_kis():
    global _KIS_INSTANCE, _KIS_IMPORT_ERROR
    if _KIS_INSTANCE is not None:
        return _KIS_INSTANCE

    try:
        from trader.kis_wrapper import KisAPI  # type: ignore
    except Exception as e:  # pragma: no cover
        _KIS_IMPORT_ERROR = f"KisAPI import failed: {e}"
        logger.warning("[KIS] %s", _KIS_IMPORT_ERROR)
        return None

    if not _have_kis_creds():
        _KIS_IMPORT_ERROR = "KIS credentials not configured (CANO/ACNT_PRDT_CD/KIS_APP_KEY)"
        logger.warning("[KIS] %s", _KIS_IMPORT_ERROR)
        return None

    try:
        _KIS_INSTANCE = KisAPI()
        logger.info("[KIS] KisAPI initialized")
    except Exception as e:  # pragma: no cover
        _KIS_IMPORT_ERROR = f"KisAPI init failed: {e}"
        logger.error("[KIS] %s", _KIS_IMPORT_ERROR)
        _KIS_INSTANCE = None
    return _KIS_INSTANCE


@app.post("/buy-order/")
def buy_order(req: OrderRequest):
    kis = _get_kis()
    if kis is None:
        raise HTTPException(status_code=503, detail=_KIS_IMPORT_ERROR or "KIS unavailable")
    try:
        result = kis.buy_stock(req.code, req.qty, req.price)
        return {"result": result}
    except Exception as e:
        logger.error("[BUY_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/sell-order/")
def sell_order(req: OrderRequest):
    kis = _get_kis()
    if kis is None:
        raise HTTPException(status_code=503, detail=_KIS_IMPORT_ERROR or "KIS unavailable")
    try:
        result = kis.sell_stock(req.code, req.qty, req.price)
        return {"result": result}
    except Exception as e:
        logger.error("[SELL_ORDER_FAIL] %s", e)
        raise HTTPException(status_code=400, detail=str(e))
