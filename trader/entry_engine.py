"""Entry Signal Engine - Breakout, Pullback, Momentum Strategies.

최종 30 종목 Watchlist에서 실시간 진입 시그널을 스캔하는 엔진.

전략 구성:
- Breakout: 신고점 돌파 + 거래량 급증
- Pullback: 추세 내 눌림목 + 반등
- Momentum: 강한 상대강도 + 추세 지속

권장 비율:
- Breakout: 40%
- Pullback: 40%
- Momentum: 20%
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class EntrySignal:
    """진입 시그널 데이터 클래스."""
    code: str
    name: str
    strategy: str  # "breakout" | "pullback" | "momentum"
    close: float
    signal_strength: float  # 0.0 ~ 1.0
    meta: Dict[str, Any]


def _safe_float(val: Any, default: float = 0.0) -> float:
    """안전한 float 변환."""
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _calculate_ma(df: pd.DataFrame, period: int, col: str = "close") -> pd.Series:
    """이동평균 계산."""
    if df is None or df.empty or col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index if df is not None else [])
    return df[col].rolling(window=period, min_periods=1).mean()


def _calculate_volume_avg(df: pd.DataFrame, period: int) -> pd.Series:
    """평균 거래량 계산."""
    if df is None or df.empty or "volume" not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index if df is not None else [])
    return df["volume"].rolling(window=period, min_periods=1).mean()


def scan_breakout(
    *,
    watchlist: List[Dict[str, Any]],
    ohlcv_provider: Callable[[str, int], Optional[pd.DataFrame]],
) -> List[EntrySignal]:
    """
    Breakout 전략 스캔.
    
    진입 조건:
    - close > high_50 (50일 최고가 돌파)
    - volume > volume_avg20 * 1.5 (거래량 급증)
    
    Args:
        watchlist: 종목 리스트 [{"code": "005930", "name": "삼성전자", ...}, ...]
        ohlcv_provider: OHLCV 데이터 제공 함수 (code, days) -> DataFrame
    
    Returns:
        Breakout 시그널 리스트
    """
    signals = []
    
    logger.info("[ENTRY_SCAN][BREAKOUT] scanning %s symbols", len(watchlist))
    
    for item in watchlist:
        code = str(item.get("code", "")).zfill(6)
        name = str(item.get("name", ""))
        
        if not code:
            continue
        
        try:
            # 최근 60일 데이터 조회
            df = ohlcv_provider(code, 60)
            
            if df is None or len(df) < 50:
                logger.debug("[BREAKOUT] %s: insufficient data (rows=%s)", code, 0 if df is None else len(df))
                continue
            
            # 현재가
            close = _safe_float(df["close"].iloc[-1])
            
            # 50일 최고가
            high_50 = _safe_float(df["high"].tail(50).max())
            
            # 평균 거래량 (20일)
            volume_avg20 = _safe_float(df["volume"].tail(20).mean())
            
            # 현재 거래량
            volume = _safe_float(df["volume"].iloc[-1])
            
            # Breakout 조건 체크
            if close > high_50 and volume > volume_avg20 * 1.5:
                # 시그널 강도 계산 (0.0 ~ 1.0)
                # - 돌파 강도: (close - high_50) / high_50
                # - 거래량 강도: volume / (volume_avg20 * 1.5)
                breakout_strength = min(1.0, (close - high_50) / high_50 * 10.0) if high_50 > 0 else 0.0
                volume_strength = min(1.0, volume / (volume_avg20 * 1.5)) if volume_avg20 > 0 else 0.0
                signal_strength = (breakout_strength * 0.6 + volume_strength * 0.4)
                
                signal = EntrySignal(
                    code=code,
                    name=name,
                    strategy="breakout",
                    close=close,
                    signal_strength=signal_strength,
                    meta={
                        "high_50": high_50,
                        "volume": volume,
                        "volume_avg20": volume_avg20,
                        "volume_ratio": volume / volume_avg20 if volume_avg20 > 0 else 0.0,
                        "breakout_pct": ((close - high_50) / high_50 * 100.0) if high_50 > 0 else 0.0,
                    }
                )
                signals.append(signal)
                logger.info(
                    "[BREAKOUT] %s %s: close=%.0f high_50=%.0f vol_ratio=%.2f strength=%.3f",
                    code, name, close, high_50, volume / volume_avg20 if volume_avg20 > 0 else 0, signal_strength
                )
        
        except Exception as exc:
            logger.debug("[BREAKOUT] %s: error - %s", code, exc)
            continue
    
    logger.info("[ENTRY_SCAN][BREAKOUT] found %s signals", len(signals))
    return signals


def scan_pullback(
    *,
    watchlist: List[Dict[str, Any]],
    ohlcv_provider: Callable[[str, int], Optional[pd.DataFrame]],
) -> List[EntrySignal]:
    """
    Pullback 전략 스캔.
    
    진입 조건:
    - close > ma50 (50일 이평선 위)
    - 0.05 <= pullback_pct <= 0.15 (5~15% 눌림)
    
    Args:
        watchlist: 종목 리스트
        ohlcv_provider: OHLCV 데이터 제공 함수
    
    Returns:
        Pullback 시그널 리스트
    """
    signals = []
    
    logger.info("[ENTRY_SCAN][PULLBACK] scanning %s symbols", len(watchlist))
    
    for item in watchlist:
        code = str(item.get("code", "")).zfill(6)
        name = str(item.get("name", ""))
        
        if not code:
            continue
        
        try:
            # 최근 260일 데이터 조회 (52주)
            df = ohlcv_provider(code, 260)
            
            if df is None or len(df) < 50:
                logger.debug("[PULLBACK] %s: insufficient data (rows=%s)", code, 0 if df is None else len(df))
                continue
            
            # 현재가
            close = _safe_float(df["close"].iloc[-1])
            
            # 50일 이평선
            ma50 = _safe_float(_calculate_ma(df, 50).iloc[-1])
            
            # 52주 최고가 (또는 사용 가능한 최대 기간)
            high_52w = _safe_float(df["high"].tail(min(252, len(df))).max())
            
            # Pullback 비율 계산
            pullback_pct = (high_52w - close) / high_52w if high_52w > 0 else 0.0
            
            # Pullback 조건 체크
            if close > ma50 and 0.05 <= pullback_pct <= 0.15:
                # 시그널 강도 계산
                # - 이평선 위 강도: (close - ma50) / ma50
                # - 눌림 적정성: 1 - |pullback_pct - 0.10| / 0.05 (10%가 최적)
                ma_strength = min(1.0, (close - ma50) / ma50 * 10.0) if ma50 > 0 else 0.0
                pullback_optimality = max(0.0, 1.0 - abs(pullback_pct - 0.10) / 0.05)
                signal_strength = (ma_strength * 0.4 + pullback_optimality * 0.6)
                
                signal = EntrySignal(
                    code=code,
                    name=name,
                    strategy="pullback",
                    close=close,
                    signal_strength=signal_strength,
                    meta={
                        "ma50": ma50,
                        "high_52w": high_52w,
                        "pullback_pct": pullback_pct * 100.0,
                        "ma50_distance_pct": ((close - ma50) / ma50 * 100.0) if ma50 > 0 else 0.0,
                    }
                )
                signals.append(signal)
                logger.info(
                    "[PULLBACK] %s %s: close=%.0f ma50=%.0f pullback=%.2f%% strength=%.3f",
                    code, name, close, ma50, pullback_pct * 100.0, signal_strength
                )
        
        except Exception as exc:
            logger.debug("[PULLBACK] %s: error - %s", code, exc)
            continue
    
    logger.info("[ENTRY_SCAN][PULLBACK] found %s signals", len(signals))
    return signals


def scan_momentum(
    *,
    watchlist: List[Dict[str, Any]],
    ohlcv_provider: Callable[[str, int], Optional[pd.DataFrame]],
) -> List[EntrySignal]:
    """
    Momentum 전략 스캔.
    
    진입 조건:
    - rs_percentile >= 80 (상대강도 상위 20%)
    - close > ma20 (20일 이평선 위)
    - volume > volume_avg20 (거래량 확인)
    
    Args:
        watchlist: 종목 리스트
        ohlcv_provider: OHLCV 데이터 제공 함수
    
    Returns:
        Momentum 시그널 리스트
    """
    signals = []
    
    logger.info("[ENTRY_SCAN][MOMENTUM] scanning %s symbols", len(watchlist))
    
    for item in watchlist:
        code = str(item.get("code", "")).zfill(6)
        name = str(item.get("name", ""))
        
        if not code:
            continue
        
        # RS percentile은 watchlist에서 가져옴 (이미 계산되어 있음)
        rs_percentile = _safe_float(item.get("rs_pctile", item.get("rs_percentile", 0.0)))
        
        try:
            # 최근 60일 데이터 조회
            df = ohlcv_provider(code, 60)
            
            if df is None or len(df) < 20:
                logger.debug("[MOMENTUM] %s: insufficient data (rows=%s)", code, 0 if df is None else len(df))
                continue
            
            # 현재가
            close = _safe_float(df["close"].iloc[-1])
            
            # 20일 이평선
            ma20 = _safe_float(_calculate_ma(df, 20).iloc[-1])
            
            # 평균 거래량
            volume_avg20 = _safe_float(df["volume"].tail(20).mean())
            
            # 현재 거래량
            volume = _safe_float(df["volume"].iloc[-1])
            
            # Momentum 조건 체크
            if rs_percentile >= 80 and close > ma20 and volume > volume_avg20:
                # 시그널 강도 계산
                # - RS 강도: (rs_percentile - 80) / 20
                # - 이평선 위 강도: (close - ma20) / ma20
                # - 거래량 강도: volume / volume_avg20
                rs_strength = min(1.0, (rs_percentile - 80) / 20)
                ma_strength = min(1.0, (close - ma20) / ma20 * 20.0) if ma20 > 0 else 0.0
                volume_strength = min(1.0, volume / volume_avg20 - 1.0) if volume_avg20 > 0 else 0.0
                signal_strength = (rs_strength * 0.5 + ma_strength * 0.3 + volume_strength * 0.2)
                
                signal = EntrySignal(
                    code=code,
                    name=name,
                    strategy="momentum",
                    close=close,
                    signal_strength=signal_strength,
                    meta={
                        "rs_percentile": rs_percentile,
                        "ma20": ma20,
                        "volume": volume,
                        "volume_avg20": volume_avg20,
                        "volume_ratio": volume / volume_avg20 if volume_avg20 > 0 else 0.0,
                        "ma20_distance_pct": ((close - ma20) / ma20 * 100.0) if ma20 > 0 else 0.0,
                    }
                )
                signals.append(signal)
                logger.info(
                    "[MOMENTUM] %s %s: close=%.0f ma20=%.0f rs=%.1f vol_ratio=%.2f strength=%.3f",
                    code, name, close, ma20, rs_percentile, volume / volume_avg20 if volume_avg20 > 0 else 0, signal_strength
                )
        
        except Exception as exc:
            logger.debug("[MOMENTUM] %s: error - %s", code, exc)
            continue
    
    logger.info("[ENTRY_SCAN][MOMENTUM] found %s signals", len(signals))
    return signals


def scan_all_strategies(
    *,
    watchlist: List[Dict[str, Any]],
    ohlcv_provider: Callable[[str, int], Optional[pd.DataFrame]],
) -> Dict[str, List[EntrySignal]]:
    """
    모든 전략을 실행하여 진입 시그널 스캔.
    
    Args:
        watchlist: 종목 리스트
        ohlcv_provider: OHLCV 데이터 제공 함수
    
    Returns:
        {
            "breakout": [...],
            "pullback": [...],
            "momentum": [...],
            "all": [...]  # 중복 제거된 전체 시그널
        }
    """
    logger.info("[ENTRY_SCAN][ALL] starting all strategies scan on %s symbols", len(watchlist))
    
    # 각 전략별 스캔
    breakout_signals = scan_breakout(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    pullback_signals = scan_pullback(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    momentum_signals = scan_momentum(watchlist=watchlist, ohlcv_provider=ohlcv_provider)
    
    # 중복 제거 (동일 종목이 여러 전략에서 시그널 발생 시)
    all_signals_map: Dict[str, EntrySignal] = {}
    
    for signal in breakout_signals + pullback_signals + momentum_signals:
        if signal.code in all_signals_map:
            # 시그널 강도가 더 높은 것을 선택
            if signal.signal_strength > all_signals_map[signal.code].signal_strength:
                all_signals_map[signal.code] = signal
        else:
            all_signals_map[signal.code] = signal
    
    all_signals = list(all_signals_map.values())
    
    logger.info(
        "[ENTRY_SCAN][ALL] completed - breakout=%s pullback=%s momentum=%s unique=%s",
        len(breakout_signals),
        len(pullback_signals),
        len(momentum_signals),
        len(all_signals),
    )
    
    return {
        "breakout": breakout_signals,
        "pullback": pullback_signals,
        "momentum": momentum_signals,
        "all": all_signals,
    }


def calculate_position_size(
    *,
    capital: float,
    entry_price: float,
    atr: float,
    risk_per_trade: float = 0.01,
) -> Dict[str, float]:
    """
    포지션 사이즈 계산 (리스크 기반).
    
    Args:
        capital: 총 자본
        entry_price: 진입가
        atr: ATR (Average True Range)
        risk_per_trade: 거래당 리스크 비율 (기본 1%)
    
    Returns:
        {
            "position_size": 포지션 크기 (금액),
            "shares": 주식 수,
            "stop_price": 손절가,
            "risk_amount": 리스크 금액,
        }
    """
    # 거래당 리스크 금액
    risk_amount = capital * risk_per_trade
    
    # ATR 기반 손절가 (entry_price - atr * 2)
    stop_price = entry_price - (atr * 2)
    
    # 1주당 리스크
    risk_per_share = entry_price - stop_price
    
    # 주식 수 계산
    if risk_per_share > 0:
        shares = int(risk_amount / risk_per_share)
    else:
        shares = 0
    
    # 포지션 크기 (금액)
    position_size = shares * entry_price
    
    return {
        "position_size": position_size,
        "shares": shares,
        "stop_price": stop_price,
        "risk_amount": risk_amount,
        "atr": atr,
        "entry_price": entry_price,
    }
