"""US trading 전용 로깅 유틸리티.

중복 basicConfig 호출을 방지하는 setup_us_logging() 제공.

로그 라인이 2번 찍히는 문제는 root logger에 handler가 이미 있는 상태에서
추가 handler를 붙이거나 basicConfig가 중복 호출될 때 발생한다.

이 함수는:
  1. _LOGGING_CONFIGURED 플래그로 중복 호출 방지
  2. root logger의 기존 handler를 제거 후 새 handler 1개만 설정
  3. 개별 module logger에서 logger.addHandler()를 직접 호출하지 않도록 주의
     → 꼭 필요하면 logger.propagate = False 설정 후 사용
"""
from __future__ import annotations

import logging
import sys

_LOGGING_CONFIGURED = False


def setup_us_logging(
    level: int = logging.INFO,
    fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s",
) -> None:
    """root logger를 한 번만 설정한다. 중복 호출은 무시된다.

    기존 handler를 모두 제거한 뒤 stdout handler 1개만 설정한다.
    이를 통해 같은 로그 라인이 2번 출력되는 문제를 방지한다.
    """
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    root = logging.getLogger()

    # 기존 handler 모두 제거
    for h in list(root.handlers):
        root.removeHandler(h)

    # stdout handler 1개만 설정
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    _LOGGING_CONFIGURED = True
