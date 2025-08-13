# strategy_atr.py
# -----------------------------------------------------------------------------
# ATR 기반 포지션 관리/매도 엔진 (clean version)
# - 분봉 OHLC가 있으면 feed_candle(...)로 ATR을 갱신
# - 분봉이 없다면 feed_tick_and_decide(...)만 써도 동작(간이 TR 추정)
# - 매수 체결/매도 체결 훅 제공: on_buy_filled / on_sell_filled
# - 의사결정 제공: SELL_PARTIAL / SELL_ALL / ADJUST_STOP / HOLD
# -----------------------------------------------------------------------------
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Optional


# ===== 공용 구조 =====
@dataclass
class StrategyDecision:
    """
    type:
      - HOLD: 아무 것도 하지 않음
      - SELL_PARTIAL: 일부 청산 (qty > 0)
      - SELL_ALL: 전량 청산 (qty > 0)
      - ADJUST_STOP: 서버 스탑주문이 없으므로 내부 관리용(기록/로그용)
    """
    type: str = "HOLD"
    qty: int = 0
    new_stop: Optional[float] = None
    reason: str = ""
    debug: Optional[dict] = None


def _env_float(key: str, default: float) -> float:
    try:
        v = os.getenv(key)
        return float(v) if v is not None and v.strip() != "" else default
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        v = os.getenv(key)
        return int(float(v)) if v is not None and v.strip() != "" else default
    except Exception:
        return default


# ===== 파라미터 =====
@dataclass
class StrategyATRParams:
    # ATR
    atr_window: int = _env_int("ATR_WINDOW", 14)

    # 손절: 기본/와이드 (초기가격 기준)  ex) -3.5% / -4.5%
    stop_loss_pct: float = _env_float("STOP_LOSS_PCT", -3.5)
    stop_loss_pct_wide: float = _env_float("STOP_LOSS_PCT_WIDE", -4.5)

    # 변동성 큰 날 판단(엔트리 시점 ATR%가 이 값을 넘으면 wide stop 채택)
    # ATR% = ATR / price * 100
    atr_wide_threshold_pct: float = _env_float("ATR_WIDE_THRESHOLD_PCT", 2.0)

    # (선택) ATR 기반 추가 가드: 엔트리-ATR배수 스탑과 퍼센트 스탑 중 더 '타이트'한 쪽 사용
    use_atr_stop_guard: bool = True
    atr_stop_mult: float = _env_float("ATR_STOP_MULT", 2.2)

    # 목표가/부분청산
    tp1_pct: float = _env_float("TP1_PCT", 2.8)
    tp1_ratio: float = _env_float("TP1_RATIO", 0.35)   # 0.30~0.40 추천
    tp2_pct: float = _env_float("TP2_PCT", 5.5)
    tp2_ratio: float = _env_float("TP2_RATIO", 0.35)

    # 트레일링 스탑
    trail_pct_pre_tp2: float = _env_float("TRAIL_PCT_PRE_TP2", 6.0)   # 피크 대비 하락폭
    trail_pct_post_tp2: float = _env_float("TRAIL_PCT_POST_TP2", 4.0) # TP2 도달 후 타이트닝

    # 부분청산 최소 수량
    min_partial_qty: int = _env_int("MIN_PARTIAL_QTY", 1)


# ===== ATR 추적기 =====
class _ATRTracker:
    """
    Wilder 방식에 가까운 ATR 계산.
    - 분봉이 들어오면 feed_candle(high, low, close)
    - 분봉이 없다면 feed_tick(close)로 간이 TR(=|close - prev_close|) 추정
    """
    def __init__(self, window: int = 14):
        self.window = max(2, int(window))
        self._tr_buf: Deque[float] = deque(maxlen=self.window)
        self.prev_close: Optional[float] = None
        self.atr: Optional[float] = None

    @staticmethod
    def _true_range(high: float, low: float, prev_close: Optional[float]) -> float:
        if prev_close is None:
            return float(abs(high - low))
        return max(abs(high - low), abs(high - prev_close), abs(low - prev_close))

    def feed_candle(self, high: float, low: float, close: float) -> Optional[float]:
        tr = self._true_range(high, low, self.prev_close)
        self.prev_close = float(close)

        if self.atr is None:
            # 초기 구간은 단순이평 TR 평균, 이후 Wilder smoothing
            self._tr_buf.append(tr)
            if len(self._tr_buf) < self.window:
                return None
            self.atr = sum(self._tr_buf) / len(self._tr_buf)
        else:
            self.atr = (self.atr * (self.window - 1) + tr) / self.window
        return self.atr

    def feed_tick(self, close: float) -> Optional[float]:
        """
        분봉 미제공 시 close 간 차이를 TR로 간주(보수적, 과소추정 가능).
        """
        if self.prev_close is None:
            self.prev_close = float(close)
            return None
        tr = abs(float(close) - self.prev_close)
        self.prev_close = float(close)

        if self.atr is None:
            self._tr_buf.append(tr)
            if len(self._tr_buf) < self.window:
                return None
            self.atr = sum(self._tr_buf) / len(self._tr_buf)
        else:
            self.atr = (self.atr * (self.window - 1) + tr) / self.window
        return self.atr

    def atr_pct(self, price: Optional[float]) -> float:
        if self.atr is None or not price or price <= 0:
            return 0.0
        return (self.atr / float(price)) * 100.0


# ===== 포지션 상태 =====
@dataclass
class _PosState:
    entry: float             # 매수 체결가
    qty: int                 # 현재 보유 수량
    peak: float              # 진입 이후 최고가
    stop: float              # 현재 내부 스탑(가격)
    tp1_hit: bool = False
    tp2_hit: bool = False
    use_wide_stop: bool = False
    last_price: Optional[float] = None


# ===== 전략 엔진 =====
class ATRStrategyEngine:
    """
    사용법(핵심 훅)
      - on_buy_filled(code, qty, price)   : 매수 체결 직후 호출
      - on_sell_filled(code, sold_qty)    : 매도 체결 직후 호출
      - feed_candle(code, high, low, close, ts) : 분봉 들어올 때 호출(선택)
      - feed_tick_and_decide(code, price, ts)   : 틱/주기마다 호출 → StrategyDecision 반환
    """
    def __init__(self, params: StrategyATRParams = StrategyATRParams()):
        self.p = params
        self._atr_map: Dict[str, _ATRTracker] = {}
        self._pos: Dict[str, _PosState] = {}

    # --- 내부 헬퍼 ---
    def _atr(self, code: str) -> _ATRTracker:
        tr = self._atr_map.get(code)
        if tr is None:
            tr = _ATRTracker(window=self.p.atr_window)
            self._atr_map[code] = tr
        return tr

    def _init_stop(self, price: float, atr_now: Optional[float], atr_pct_now: float, wide_mode: bool) -> float:
        # 퍼센트 스탑
        base_pct = abs(self.p.stop_loss_pct_wide if wide_mode else self.p.stop_loss_pct) / 100.0
        stop_by_pct = price * (1.0 - base_pct)

        if self.p.use_atr_stop_guard and atr_now and atr_now > 0:
            stop_by_atr = price - (self.p.atr_stop_mult * atr_now)
            # 더 '타이트'한(가격이 더 높은) 스탑을 채택
            return max(stop_by_pct, stop_by_atr)
        return stop_by_pct

    def _trail_candidate(self, peak: float, after_tp2: bool) -> float:
        trail_pct = self.p.trail_pct_post_tp2 if after_tp2 else self.p.trail_pct_pre_tp2
        return peak * (1.0 - float(trail_pct) / 100.0)

    # --- 외부 API ---
    def on_buy_filled(self, code: str, qty: int, price: float) -> None:
        tracker = self._atr(code)
        # 엔트리 시점 ATR%로 wide stop 여부 판단
        atr_pct_now = tracker.atr_pct(price)
        wide = atr_pct_now >= self.p.atr_wide_threshold_pct

        init_stop = self._init_stop(price=float(price),
                                    atr_now=tracker.atr,
                                    atr_pct_now=atr_pct_now,
                                    wide_mode=wide)
        self._pos[code] = _PosState(
            entry=float(price),
            qty=int(max(0, qty)),
            peak=float(price),
            stop=float(init_stop),
            tp1_hit=False,
            tp2_hit=False,
            use_wide_stop=wide,
            last_price=float(price),
        )

    def on_sell_filled(self, code: str, sold_qty: int) -> None:
        st = self._pos.get(code)
        if not st:
            return
        st.qty = max(0, int(st.qty) - int(sold_qty))
        if st.qty <= 0:
            self._pos.pop(code, None)

    def feed_candle(self, code: str, high: float, low: float, close: float, ts=None) -> Optional[float]:
        """분봉이 있을 때 ATR 업데이트(권장)."""
        tracker = self._atr(code)
        return tracker.feed_candle(high=float(high), low=float(low), close=float(close))

    def feed_tick_and_decide(self, code: str, price: float, ts=None) -> StrategyDecision:
        """
        매 주기(틱/폴링)마다 호출해 의사결정을 받는다.
        - 분봉을 쓰지 않는 환경에선 여기서 ATR 간이 추정(feed_tick)도 함께 수행.
        - 반환: StrategyDecision (HOLD / SELL_PARTIAL / SELL_ALL / ADJUST_STOP)
        """
        st = self._pos.get(code)
        tracker = self._atr(code)

        # ATR 업데이트(분봉이 이미 들어왔다면 feed_tick이 큰 영향은 없음)
        tracker.feed_tick(float(price))

        # 포지션 없으면 HOLD
        if not st or st.qty <= 0:
            return StrategyDecision("HOLD", reason="NO_POSITION")

        # 상태 업데이트
        st.last_price = float(price)
        if price > st.peak:
            st.peak = float(price)

        # 트레일링 스탑 후보
        trail_cand = self._trail_candidate(st.peak, after_tp2=st.tp2_hit)
        new_stop = max(st.stop, trail_cand)

        # --- TP 이벤트 판정 ---
        entry = st.entry
        qty = st.qty
        tp1_px = entry * (1.0 + self.p.tp1_pct / 100.0)
        tp2_px = entry * (1.0 + self.p.tp2_pct / 100.0)

        debug = {
            "entry": entry, "price": price, "peak": st.peak, "stop": st.stop,
            "tp1_px": tp1_px, "tp2_px": tp2_px,
            "atr": tracker.atr, "atr_pct": tracker.atr_pct(price),
            "tp1_hit": st.tp1_hit, "tp2_hit": st.tp2_hit,
            "trail_cand": trail_cand, "new_stop": new_stop,
        }

        # 1) 손절/스탑(가격이 스탑 밑으로 내려옴)
        if price <= st.stop:
            return StrategyDecision(
                type="SELL_ALL",
                qty=int(qty),
                new_stop=st.stop,
                reason="STOP_HIT",
                debug=debug,
            )

        # 2) TP1: +TP1% 도달 시 부분청산 + 스탑을 BE로 상향
        if (not st.tp1_hit) and price >= tp1_px:
            st.tp1_hit = True
            # 부분청산 수량
            pqty = max(self.p.min_partial_qty, int(math.floor(qty * self.p.tp1_ratio)))
            pqty = min(pqty, max(0, qty - self.p.min_partial_qty)) if qty > 1 else 0
            # 스탑을 BE(손익분기점) 이상으로 끌어올림
            be_stop = max(st.stop, entry)
            # 내부 상태에 즉시 반영
            st.stop = max(new_stop, be_stop)
            return StrategyDecision(
                type="SELL_PARTIAL" if pqty > 0 else "ADJUST_STOP",
                qty=int(pqty),
                new_stop=st.stop,
                reason="TP1_PARTIAL_AND_BE_STOP" if pqty > 0 else "TP1_BE_STOP_ONLY",
                debug=debug,
            )

        # 3) TP2: +TP2% 도달 시 부분청산 + 트레일링을 타이트(4%)로 축소
        if (not st.tp2_hit) and price >= tp2_px:
            st.tp2_hit = True
            pqty = max(self.p.min_partial_qty, int(math.floor(qty * self.p.tp2_ratio)))
            pqty = min(pqty, max(0, qty - self.p.min_partial_qty)) if qty > 1 else 0
            # TP2 이후 더 타이트한 트레일 후보를 반영
            trail_after_tp2 = self._trail_candidate(st.peak, after_tp2=True)
            st.stop = max(st.stop, trail_after_tp2)
            return StrategyDecision(
                type="SELL_PARTIAL" if pqty > 0 else "ADJUST_STOP",
                qty=int(pqty),
                new_stop=st.stop,
                reason="TP2_PARTIAL_AND_TIGHT_TRAIL" if pqty > 0 else "TP2_TIGHT_TRAIL_ONLY",
                debug=debug,
            )

        # 4) 평상시: 트레일 후보가 올라갔다면 스탑 상향만 전달
        if new_stop > st.stop:
            st.stop = new_stop
            return StrategyDecision(
                type="ADJUST_STOP",
                qty=0,
                new_stop=st.stop,
                reason="TRAIL_RATCHET",
                debug=debug,
            )

        # 5) 아무 변화 없음
        return StrategyDecision("HOLD", reason="NO_SIGNAL", debug=debug)

    # --- (선택) 상태 접근 유틸 ---
    def get_position_state(self, code: str) -> Optional[dict]:
        st = self._pos.get(code)
        if not st:
            return None
        return {
            "entry": st.entry,
            "qty": st.qty,
            "peak": st.peak,
            "stop": st.stop,
            "tp1_hit": st.tp1_hit,
            "tp2_hit": st.tp2_hit,
            "wide": st.use_wide_stop,
            "last_price": st.last_price,
        }

    def export_states(self) -> Dict[str, dict]:
        return {c: self.get_position_state(c) for c in list(self._pos.keys()) if self.get_position_state(c)}
