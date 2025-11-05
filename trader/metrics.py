# -*- coding: utf-8 -*-
"""
metrics.py — ORB/VWAP/거래대금/슬리피지 등 '지표 계산 & 품질 가드' 유틸리티

역할
- 개장 후 N분 동안 ORB(Opening Range) 상단/하단을 추적하고, 고정(Freeze) 시점을 기록
- VWAP/스프레드/거래대금 등 품질 가드 함수 제공
- 종목 특성(전일 거래대금, 현재 가격)에 따라 슬리피지 임계치를 적응식으로 산출

주의
- 이 모듈은 '계산/판단'만 담당합니다. 실제 주문은 trader.py에서 수행합니다.
- VWAP / 스프레드 / 거래대금 1분치 값은 각자 보유한 데이터 공급 로직과 연결하세요.
"""
from __future__ import annotations
import math
import logging

logger = logging.getLogger(__name__)


class OpeningRange:
    """
    개장 이후 ORB_MIN 분 동안의 고가/저가를 추적하여,
    ORB_MIN 경과 시점에 박스를 '고정'하고 브레이크아웃 여부 판단에 사용.
    """
    def __init__(self, orb_min: int):
        if orb_min <= 0:
            raise ValueError("orb_min must be > 0")
        self.orb_min = orb_min
        self._orh = {}      # code -> float
        self._orl = {}      # code -> float
        self._frozen = set()  # code

    def update(self, code: str, last_price: float, minutes_from_open: int) -> None:
        """개장 이후 분(min) 정보와 현재가를 받아 ORH/ORL 업데이트."""
        if minutes_from_open <= self.orb_min:
            # ORB 관측 구간
            self._orh[code] = max(self._orh.get(code, -math.inf), float(last_price))
            self._orl[code] = min(self._orl.get(code,  math.inf), float(last_price))
        else:
            # 최초 고정 로그 1회
            if code not in self._frozen:
                logger.info("[ORB-FIX] %s ORH=%.2f ORL=%.2f",
                            code, self._orh.get(code, float("nan")), self._orl.get(code, float("nan")))
                self._frozen.add(code)

    def ready(self, code: str) -> bool:
        """ORB 박스가 고정되었는지 여부."""
        return code in self._frozen

    def orh_value(self, code: str) -> float | None:
        """ORB 상단(관측된 고가)."""
        return self._orh.get(code)

    def orl_value(self, code: str) -> float | None:
        """ORB 하단(관측된 저가)."""
        return self._orl.get(code)


def vwap_guard(last: float | None, vwap: float | None, tol: float) -> bool:
    """
    VWAP 지지 여부 판단: last가 vwap*(1±tol) 범위 위쪽(=상회 또는 근접)에 있는지 체크.
    tol: 0.003(=0.3%) 같은 비율 입력
    """
    if not last or not vwap or vwap <= 0:
        return False
    # 근접 허용: last가 vwap*(1 - tol) 이상이면 '지지'로 간주
    return (last >= vwap * (1 - tol))


def spread_guard(spread_ticks: int | None, max_ticks: int) -> bool:
    """
    호가 스프레드가 특정 틱 이하인지 체크.
    - 고가주/저유동 종목은 스프레드가 커지므로 max_ticks는 .env에서 조정
    """
    if spread_ticks is None:
        return False
    return spread_ticks <= max_ticks


def adaptive_slip(prev_turnover: float | None, price: float | None, base_pct: float) -> float:
    """
    적응식 슬리피지 가드 산출.
    - prev_turnover: 전일 거래대금(원), 낮을수록 가드폭 확대
    - price: 현재가, 10만원 이상 고가주는 약간 가드폭 확대
    - base_pct: .env의 SLIPPAGE_BASE_PCT(예: 1.0%)
    반환: '비율' (예: 0.012 = 1.2%)
    """
    p = float(price or 0.0)
    t = float(prev_turnover or 0.0)

    # 거래대금 구간별 가중치
    #   > 5e10 (500억) → 1.0
    #   > 1e10 (100억) → 1.3
    #   그 이하는 → 1.6
    if t > 5e10:
        factor_t = 1.0
    elif t > 1e10:
        factor_t = 1.3
    else:
        factor_t = 1.6

    # 고가주(>= 100,000) 약간 확대
    factor_p = 1.1 if p >= 100_000 else 1.0

    slip = (base_pct / 100.0) * factor_t * factor_p
    return slip
