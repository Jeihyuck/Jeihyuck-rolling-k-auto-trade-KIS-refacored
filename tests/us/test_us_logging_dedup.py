# -*- coding: utf-8 -*-
"""tests/us/test_us_logging_dedup.py

setup_us_logging을 여러 번 호출해도 root logger의 handler가 1개뿐이고
로그 라인이 중복 출력되지 않는지 검증.
"""
from __future__ import annotations

import importlib
import logging
import sys


def _reload_logging_utils():
    """모듈을 재로드해서 _LOGGING_CONFIGURED 플래그를 초기화."""
    import trader.us.utils.logging_utils as mod
    mod._LOGGING_CONFIGURED = False  # noqa: SLF001 (테스트 목적)
    return mod


class TestLoggingDedup:
    def setup_method(self):
        # 테스트마다 root logger 핸들러를 초기화
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        # 플래그도 초기화
        _reload_logging_utils()

    def test_single_call_creates_one_handler(self):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        root = logging.getLogger()
        assert len(root.handlers) == 1

    def test_double_call_still_one_handler(self):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        setup_us_logging()  # 두 번째 호출
        root = logging.getLogger()
        assert len(root.handlers) == 1

    def test_triple_call_still_one_handler(self):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        setup_us_logging()
        setup_us_logging()  # 세 번째 호출
        root = logging.getLogger()
        assert len(root.handlers) == 1

    def test_handler_is_stream_handler(self):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0], logging.StreamHandler)

    def test_handler_writes_to_stdout(self):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        root = logging.getLogger()
        handler = root.handlers[0]
        assert handler.stream is sys.stdout

    def test_no_duplicate_log_lines(self, capsys):
        from trader.us.utils.logging_utils import setup_us_logging
        setup_us_logging()
        setup_us_logging()  # 두 번 호출
        logger = logging.getLogger("test_dedup")
        logger.info("dedup_test_marker_12345")
        captured = capsys.readouterr()
        count = captured.out.count("dedup_test_marker_12345")
        assert count == 1, f"Log line appeared {count} times, expected exactly 1"
