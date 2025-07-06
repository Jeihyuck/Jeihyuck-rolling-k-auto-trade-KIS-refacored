# errors.py

from fastapi.responses import JSONResponse
from fastapi import Request
from starlette import status

class DomainError(Exception):
    def __init__(self, message: str, code: int = status.HTTP_400_BAD_REQUEST):
        super().__init__(message)
        self.status_code = code

def http_exception_handler(_: Request, exc: DomainError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": str(exc)},
    )
