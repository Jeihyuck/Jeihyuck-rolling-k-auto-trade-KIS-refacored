from fastapi import Request, status
from fastapi.responses import JSONResponse


class DomainError(Exception):
    """비즈니스 규칙 위반 등 API 전용 예외."""

    def __init__(
        self, detail: str, status_code: int = status.HTTP_400_BAD_REQUEST
    ) -> None:
        self.detail = detail
        self.status_code = status_code


async def domain_error_handler(_: Request, exc: DomainError):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
