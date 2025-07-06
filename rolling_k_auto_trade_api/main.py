from fastapi import FastAPI

from logging_config import setup_logging

# 로깅은 앱 생성 전에 초기화!
setup_logging("rolling_k")

from .rebalance_watchlist import router as rebalance_router  # noqa: E402
from .errors import DomainError, domain_error_handler  # noqa: E402

app = FastAPI(title="Rolling K Auto Trade API")

# 라우터 & 예외 핸들러 등록
app.include_router(rebalance_router)
app.add_exception_handler(DomainError, domain_error_handler)
