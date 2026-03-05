"""
PB-CORE v6: Multi-Strategy Integration Test
전체 Multi-Strategy 시스템 테스트
"""
from __future__ import annotations

import pandas as pd
from datetime import datetime, timedelta

from trader.multi_strategy_engine import (
    MultiStrategyEngine,
    generate_multi_strategy_signals,
)
from trader.signal_aggregator import resolve_signals, group_by_symbol
from trader.market_regime import MarketRegimeEngine, detect_market_regime


def create_sample_ohlcv(days: int = 300) -> pd.DataFrame:
    """샘플 OHLCV 데이터 생성"""
    dates = [datetime.now().date() - timedelta(days=i) for i in range(days, 0, -1)]
    
    # 상승 추세 데이터
    base_price = 10000
    df = pd.DataFrame({
        "date": dates,
        "open": [base_price + i * 10 for i in range(days)],
        "high": [base_price + i * 10 + 50 for i in range(days)],
        "low": [base_price + i * 10 - 50 for i in range(days)],
        "close": [base_price + i * 10 for i in range(days)],
        "volume": [1000000 + i * 1000 for i in range(days)],
    })
    
    return df


def test_multi_strategy_engine():
    """Multi-Strategy Engine 테스트"""
    print("\n=== Multi-Strategy Engine Test ===")
    
    # 샘플 데이터
    symbol = "005930"  # 삼성전자
    ohlcv_df = create_sample_ohlcv()
    rs_percentile = 85.0
    
    # Engine 생성
    engine = MultiStrategyEngine(
        enable_pullback=True,
        enable_breakout=True,
        enable_momentum=True,
    )
    
    # 신호 생성
    signals = engine.generate_signals(
        symbol=symbol,
        ohlcv_df=ohlcv_df,
        rs_percentile=rs_percentile,
    )
    
    print(f"Generated {len(signals)} signals for {symbol}")
    for signal in signals:
        print(f"  - Strategy: {signal.strategy}, Score: {signal.score:.1f}, OK: {signal.signal_ok}, Reason: {signal.reason}")
    
    return signals


def test_signal_aggregator():
    """Signal Aggregator 테스트"""
    print("\n=== Signal Aggregator Test ===")
    
    # 여러 종목의 신호 생성
    symbols = ["005930", "000660", "035720"]
    all_signals = []
    
    for symbol in symbols:
        ohlcv_df = create_sample_ohlcv()
        rs_percentile = 80.0 + len(symbol) % 20  # 다양한 RS
        
        signals = generate_multi_strategy_signals(
            symbol=symbol,
            ohlcv_df=ohlcv_df,
            rs_percentile=rs_percentile,
            regime="bull",
        )
        all_signals.extend(signals)
    
    print(f"Total signals before aggregation: {len(all_signals)}")
    
    # 종목별 그룹화
    grouped = group_by_symbol(all_signals)
    for symbol, sigs in grouped.items():
        print(f"  {symbol}: {len(sigs)} signals ({', '.join(s.strategy for s in sigs)})")
    
    # 신호 통합 (best_score)
    aggregated = resolve_signals(all_signals, mode="best_score")
    print(f"\nAggregated signals: {len(aggregated)}")
    for signal in aggregated:
        print(f"  {signal.symbol}: {signal.strategy} (score: {signal.score:.1f})")
    
    return aggregated


def test_market_regime():
    """Market Regime Engine 테스트"""
    print("\n=== Market Regime Engine Test ===")
    
    # 지수 데이터 (상승 추세)
    index_df = create_sample_ohlcv(days=300)
    
    # Regime 감지
    regime_engine = MarketRegimeEngine(index_symbol="229200")
    regime_state = regime_engine.detect_regime(index_df)
    
    print(f"Detected regime: {regime_state.regime}")
    print(f"Confidence: {regime_state.confidence:.2f}")
    print(f"Reason: {regime_state.reason}")
    print(f"Index price: {regime_state.index_price:.2f}")
    print(f"MA50: {regime_state.ma50:.2f}")
    print(f"MA200: {regime_state.ma200:.2f}")
    print(f"Trend strength: {regime_state.trend_strength:.3f}")
    
    # 전략 활성화
    enabled = regime_engine.get_enabled_strategies(regime_state.regime)
    print(f"\nEnabled strategies for {regime_state.regime}:")
    for strategy, is_enabled in enabled.items():
        print(f"  {strategy}: {'✅' if is_enabled else '❌'}")
    
    return regime_state


def test_full_pipeline():
    """전체 파이프라인 테스트"""
    print("\n=== Full Multi-Strategy Pipeline Test ===")
    
    # 1. Market Regime 감지
    print("\n[1] Market Regime Detection")
    index_df = create_sample_ohlcv(days=300)
    regime = detect_market_regime(index_df)
    print(f"Market regime: {regime}")
    
    # 2. 종목 리스트 (Final30 시뮬레이션)
    print("\n[2] Candidate Pool")
    symbols = [f"{i:06d}" for i in range(5930, 5940)]  # 10개 종목
    print(f"Candidates: {len(symbols)}")
    
    # 3. Multi-Strategy 신호 생성
    print("\n[3] Multi-Strategy Signal Generation")
    all_signals = []
    
    for i, symbol in enumerate(symbols):
        ohlcv_df = create_sample_ohlcv()
        rs_percentile = 70.0 + (i % 30)  # 다양한 RS
        
        signals = generate_multi_strategy_signals(
            symbol=symbol,
            ohlcv_df=ohlcv_df,
            rs_percentile=rs_percentile,
            regime=regime,
        )
        all_signals.extend(signals)
    
    print(f"Total signals: {len(all_signals)}")
    
    # 4. Signal Aggregation
    print("\n[4] Signal Aggregation")
    aggregated = resolve_signals(all_signals, mode="best_score")
    print(f"Aggregated signals: {len(aggregated)}")
    
    # 5. Top Signals
    print("\n[5] Top 5 Signals")
    top_signals = sorted(aggregated, key=lambda s: s.score, reverse=True)[:5]
    
    for i, signal in enumerate(top_signals, 1):
        print(f"{i}. {signal.symbol}: {signal.strategy} (score: {signal.score:.1f})")
    
    print("\n✅ Full pipeline test completed!")
    return top_signals


if __name__ == "__main__":
    print("=" * 60)
    print("PB-CORE v6: Multi-Strategy System Test")
    print("=" * 60)
    
    # 개별 테스트
    test_multi_strategy_engine()
    test_signal_aggregator()
    test_market_regime()
    
    # 전체 파이프라인
    print("\n" + "=" * 60)
    test_full_pipeline()
    print("=" * 60)
