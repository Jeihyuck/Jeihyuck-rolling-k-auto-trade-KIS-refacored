# -*- coding: utf-8 -*-
"""Live Gate Policy 단위 테스트

엣지 케이스 테스트:
- 08:59 (preopen)
- 09:00 (morning 시작)
- 09:00:04 (open buffer 내)
- 15:15 (close 시작)
- 15:30 (after 시작)
- 주말
- 공휴일 (간단 구현에서는 주말만)
- 긴급 제어 (KILL_SWITCH, FORCE_BLOCK_LIVE, FORCE_LIVE)
"""
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest

from trader.live_gate import compute_live_gate, is_trading_day_korea, classify_window

KST = ZoneInfo("Asia/Seoul")


class TestIsTradingDay:
    """거래일 판정 테스트"""
    
    def test_monday_is_trading_day(self):
        # 2026-02-23 (월요일)
        mon = datetime(2026, 2, 23, 10, 0, tzinfo=KST)
        assert is_trading_day_korea(mon) is True
    
    def test_friday_is_trading_day(self):
        # 2026-02-27 (금요일)
        fri = datetime(2026, 2, 27, 10, 0, tzinfo=KST)
        assert is_trading_day_korea(fri) is True
    
    def test_saturday_is_not_trading_day(self):
        # 2026-02-28 (토요일)
        sat = datetime(2026, 2, 28, 10, 0, tzinfo=KST)
        assert is_trading_day_korea(sat) is False
    
    def test_sunday_is_not_trading_day(self):
        # 2026-03-01 (일요일)
        sun = datetime(2026, 3, 1, 10, 0, tzinfo=KST)
        assert is_trading_day_korea(sun) is False


class TestClassifyWindow:
    """시간대 구분 테스트"""
    
    def test_preopen_0859(self):
        dt = datetime(2026, 2, 25, 8, 59, tzinfo=KST)
        assert classify_window(dt) == "preopen"
    
    def test_morning_0900(self):
        dt = datetime(2026, 2, 25, 9, 0, tzinfo=KST)
        assert classify_window(dt) == "morning"
    
    def test_morning_0959(self):
        dt = datetime(2026, 2, 25, 9, 59, tzinfo=KST)
        assert classify_window(dt) == "morning"
    
    def test_intraday_1000(self):
        dt = datetime(2026, 2, 25, 10, 0, tzinfo=KST)
        assert classify_window(dt) == "intraday"
    
    def test_intraday_1514(self):
        dt = datetime(2026, 2, 25, 15, 14, tzinfo=KST)
        assert classify_window(dt) == "intraday"
    
    def test_close_1515(self):
        dt = datetime(2026, 2, 25, 15, 15, tzinfo=KST)
        assert classify_window(dt) == "close"
    
    def test_close_1529(self):
        dt = datetime(2026, 2, 25, 15, 29, tzinfo=KST)
        assert classify_window(dt) == "close"
    
    def test_after_1530(self):
        dt = datetime(2026, 2, 25, 15, 30, tzinfo=KST)
        assert classify_window(dt) == "after"
    
    def test_after_1800(self):
        dt = datetime(2026, 2, 25, 18, 0, tzinfo=KST)
        assert classify_window(dt) == "after"


class TestComputeLiveGate:
    """Live Gate 정책 테스트"""
    
    def setup_method(self):
        """각 테스트 전에 환경변수 초기화"""
        for key in ["KILL_SWITCH", "FORCE_BLOCK_LIVE", "FORCE_LIVE", "FORCE_LIVE_CONFIRM", "OPEN_BUFFER_SEC"]:
            if key in os.environ:
                del os.environ[key]
    
    # ================================================================
    # 정상 시나리오: LIVE 모드, 거래 시간대
    # ================================================================
    
    def test_live_mode_morning_should_allow(self):
        """LIVE 모드, 평일 09:05 -> 주문 허용"""
        dt = datetime(2026, 2, 25, 9, 5, tzinfo=KST)  # 수요일
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "TIME_WINDOW_OK"
        assert status.trading_day is True
        assert status.window == "morning"
    
    def test_live_mode_intraday_should_allow(self):
        """LIVE 모드, 평일 13:00 -> 주문 허용"""
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "TIME_WINDOW_OK"
        assert status.trading_day is True
        assert status.window == "intraday"
    
    def test_live_mode_close_should_allow(self):
        """LIVE 모드, 평일 15:20 -> 주문 허용"""
        dt = datetime(2026, 2, 25, 15, 20, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "TIME_WINDOW_OK"
        assert status.trading_day is True
        assert status.window == "close"
    
    # ================================================================
    # 차단 시나리오: 시간대 밖
    # ================================================================
    
    def test_live_mode_preopen_should_block(self):
        """LIVE 모드, 평일 08:59 (preopen) -> 차단"""
        dt = datetime(2026, 2, 25, 8, 59, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "WINDOW=preopen"
        assert status.trading_day is True
        assert status.window == "preopen"
    
    def test_live_mode_after_should_block(self):
        """LIVE 모드, 평일 15:30 (after) -> 차단"""
        dt = datetime(2026, 2, 25, 15, 30, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "WINDOW=after"
        assert status.trading_day is True
        assert status.window == "after"
    
    def test_live_mode_weekend_should_block(self):
        """LIVE 모드, 주말 -> 차단"""
        dt = datetime(2026, 2, 28, 13, 0, tzinfo=KST)  # 토요일
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "NOT_TRADING_DAY"
        assert status.trading_day is False
    
    # ================================================================
    # 차단 시나리오: 모드/플래그
    # ================================================================
    
    def test_diag_mode_should_block(self):
        """DIAG 모드 -> 거래 시간대라도 차단"""
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="DIAG", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "MODE=DIAG"
    
    def test_dryrun_should_block(self):
        """Dry-run 모드 -> 차단"""
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=True, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "DRYRUN"
    
    def test_analysis_only_should_block(self):
        """분석 전용 모드 -> 차단"""
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=True
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "ANALYSIS_ONLY"
    
    # ================================================================
    # 긴급 제어
    # ================================================================
    
    def test_kill_switch_should_block(self):
        """KILL_SWITCH=1 -> 모든 상황에서 차단"""
        os.environ["KILL_SWITCH"] = "1"
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "KILL_SWITCH"
    
    def test_force_block_live_should_block(self):
        """FORCE_BLOCK_LIVE=1 -> 차단"""
        os.environ["FORCE_BLOCK_LIVE"] = "1"
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "FORCE_BLOCK_LIVE"
    
    def test_force_live_practice_should_allow(self):
        """FORCE_LIVE=1, practice 환경 -> 허용"""
        os.environ["FORCE_LIVE"] = "1"
        dt = datetime(2026, 2, 25, 8, 0, tzinfo=KST)  # preopen이지만 허용
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "FORCE_LIVE"
    
    def test_force_live_real_without_confirm_should_block(self):
        """FORCE_LIVE=1, real 환경, 확인 없음 -> 차단"""
        os.environ["FORCE_LIVE"] = "1"
        dt = datetime(2026, 2, 25, 13, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="real", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "FORCE_LIVE_CONFIRM_REQUIRED"
    
    def test_force_live_real_with_confirm_should_allow(self):
        """FORCE_LIVE=1, real 환경, FORCE_LIVE_CONFIRM=YES -> 허용"""
        os.environ["FORCE_LIVE"] = "1"
        os.environ["FORCE_LIVE_CONFIRM"] = "YES"
        dt = datetime(2026, 2, 25, 8, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="real", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "FORCE_LIVE"
    
    # ================================================================
    # 특수: OPEN_BUFFER_SEC
    # ================================================================
    
    def test_open_buffer_09_00_03_should_block(self):
        """09:00:03, OPEN_BUFFER_SEC=5 -> 버퍼 내이므로 차단"""
        os.environ["OPEN_BUFFER_SEC"] = "5"
        dt = datetime(2026, 2, 25, 9, 0, 3, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is False
        assert status.force_block_live is True
        assert status.reason == "OPEN_BUFFER<5s"
    
    def test_open_buffer_09_00_05_should_allow(self):
        """09:00:05, OPEN_BUFFER_SEC=5 -> 버퍼 지남, 허용"""
        os.environ["OPEN_BUFFER_SEC"] = "5"
        dt = datetime(2026, 2, 25, 9, 0, 5, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "TIME_WINDOW_OK"
    
    def test_open_buffer_09_01_should_allow(self):
        """09:01, OPEN_BUFFER_SEC=5 -> 09:00 지남, 허용"""
        os.environ["OPEN_BUFFER_SEC"] = "5"
        dt = datetime(2026, 2, 25, 9, 1, 0, tzinfo=KST)
        status = compute_live_gate(
            dt, kis_env="practice", strategy_mode="LIVE", dryrun=False, analysis_only=False
        )
        assert status.allow_live_gate is True
        assert status.force_block_live is False
        assert status.reason == "TIME_WINDOW_OK"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
