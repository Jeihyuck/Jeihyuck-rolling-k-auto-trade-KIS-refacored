from __future__ import annotations

import logging

from .engine import get_db_url, make_engine, _redact_url

logger = logging.getLogger(__name__)


def assert_db_ready() -> None:
    # url 로깅은 마스킹
    try:
        url = get_db_url()
        logger.info("[DB][HEALTH] url=%s", _redact_url(url))
    except Exception as e:
        logger.error("[DB][HEALTH] get_db_url failed: %s", e)
        raise

    engine = make_engine()
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
