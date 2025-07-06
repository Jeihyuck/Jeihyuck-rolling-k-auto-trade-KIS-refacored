import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from logging_config import setup_logging
from rolling_k_auto_trade_api.rebalance_api import rebalance_router

# 1) 로거 설정
setup_logging()
logger = logging.getLogger(__name__)

# 2) FastAPI 앱 생성
app = FastAPI()

# 3) 예외 처리 미들웨어
@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as e:
        logger.exception(f"Unhandled error: {request.method} {request.url.path}")
        try:
            body = await request.body()
            logger.debug(f"Request body: {body.decode('utf-8', 'ignore')}")
        except:
            pass
        return JSONResponse(
            status_code=500,
            content={"error": "서버 내부 오류 발생"}
        )

# 4) 라우터 등록
app.include_router(rebalance_router)
