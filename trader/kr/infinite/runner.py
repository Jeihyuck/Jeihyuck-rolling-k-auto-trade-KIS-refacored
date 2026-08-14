"""Standalone entry point. Disabled gates perform no broker or DB I/O."""
import logging,os
from .config import InfiniteConfig
def main()->int:
    logging.basicConfig(level=logging.INFO)
    c=InfiniteConfig.from_env()
    try:c.validate()
    except ValueError as e: logging.error("[KR_INFINITE][BLOCK] reason=%s",e);return 2
    if not c.enabled or not c.live or os.getenv("KIS_ENV","").lower() not in {"real","live"}:
        logging.info("[KR_INFINITE][DECISION] decision=WAIT reason=KR_INF_SAFETY_GATE_CLOSED");return 0
    logging.error("[KR_INFINITE][BLOCK] reason=KR_INF_LIVE_ORCHESTRATION_REQUIRES_EXPLICIT_RUNTIME_ADAPTER")
    return 2
if __name__=="__main__":raise SystemExit(main())
