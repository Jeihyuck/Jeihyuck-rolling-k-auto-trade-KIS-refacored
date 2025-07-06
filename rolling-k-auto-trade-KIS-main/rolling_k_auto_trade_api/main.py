from fastapi import FastAPI, BackgroundTasks
from fastapi.openapi.utils import get_openapi

from .rebalance_api import rebalance_router, latest_rebalance_result
from .report_api import report_router
from .kis_token import get_kis_access_token, update_env_token
from .realtime_executor import monitor_and_trade_all
from .rebalance_watchlist import router as watchlist_router  # ✅ 추가
from rolling_k_auto_trade_api.errors import DomainError, http_exception_handler


app = FastAPI(
    title="Rolling K Auto Trade API",
    version="1.0.0",
    description="KIS 모의투자 기반 Rolling K 변동성 돌파 전략 자동매매 API"
)

@app.on_event("startup")
def refresh_token_on_startup():
    try:
        token = get_kis_access_token()
        update_env_token(token)
    except Exception as e:
        print("❌ KIS Access Token 갱신 실패:", e)

# 예외 핸들러 등록
app.add_exception_handler(DomainError, http_exception_handler)

# 라우터 등록
app.include_router(rebalance_router)
app.include_router(report_router)
app.include_router(watchlist_router)  # ✅ 추가

@app.get("/monitor/start", tags=["Monitoring"])
def start_realtime_monitoring(background_tasks: BackgroundTasks):
    stocks = latest_rebalance_result.get("selected_stocks", [])
    if not stocks:
        return {"message": "❌ 리밸런싱된 종목이 없습니다. 먼저 /rebalance/run 실행하세요."}
    background_tasks.add_task(monitor_and_trade_all, stocks)
    return {
        "message": "📡 실시간 매매 감시 시작됨",
        "watching": len(stocks),
        "stocks": [s["stock_code"] for s in stocks]
    }

@app.get("/openapi.json", include_in_schema=False)
def custom_openapi():
    return get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
        description=app.description
    )

