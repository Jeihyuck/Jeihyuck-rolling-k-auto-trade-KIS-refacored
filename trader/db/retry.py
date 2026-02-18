from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError as SAOperationalError

logger = logging.getLogger(__name__)

T = TypeVar("T")

_TRANSIENT_HINTS = (
    "ssl error",
    "unexpected eof",
    "consuming input failed",
    "server closed the connection",
    "connection reset by peer",
    "connection is closed",
    "connection not open",
    "terminating connection",
)


try:
    from psycopg import OperationalError as PsycopgOperationalError  # type: ignore
except Exception:  # pragma: no cover
    PsycopgOperationalError = tuple()  # type: ignore


def is_transient_db_error(exc: Exception) -> bool:
    if isinstance(exc, SAOperationalError):
        return True
    if PsycopgOperationalError and isinstance(exc, PsycopgOperationalError):
        return True

    msg = str(exc).lower()
    orig = getattr(exc, "orig", None)
    if orig is not None:
        msg = f"{msg} {str(orig).lower()}"

    return any(token in msg for token in _TRANSIENT_HINTS)


def _backoff_seconds(attempt_idx: int) -> float:
    base = float(2 ** attempt_idx)
    jitter = random.uniform(0.0, 0.4)
    return base + jitter


def run_with_db_retry(
    engine: Engine,
    *,
    fn: Callable[[], T],
    operation: str,
    max_attempts: int = 5,
) -> T:
    last_exc: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            is_transient = is_transient_db_error(exc)
            is_last = attempt >= (max_attempts - 1)
            if (not is_transient) or is_last:
                raise

            sleep_sec = _backoff_seconds(attempt)
            logger.warning(
                "[DB][RETRY] op=%s attempt=%s/%s err=%s sleep=%.2fs",
                operation,
                attempt + 1,
                max_attempts,
                exc,
                sleep_sec,
            )
            try:
                engine.dispose()
            except Exception as dispose_exc:  # pragma: no cover
                logger.debug("[DB][RETRY][DISPOSE_FAIL] op=%s err=%s", operation, dispose_exc)
            time.sleep(sleep_sec)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"[DB][RETRY] unknown failure op={operation}")
