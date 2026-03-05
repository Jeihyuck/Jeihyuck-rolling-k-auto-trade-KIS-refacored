"""
Entry Engine Configuration
멀티 전략 설정 및 포트폴리오 할당
"""

# 전략별 포지션 가중치 (헤지펀드 스타일)
ENTRY_WEIGHTS = {
    "pullback": 0.4,    # 40% - 안정적, 낮은 드로우다운
    "breakout": 0.4,    # 40% - 고수익, 추세 포착
    "momentum": 0.2,    # 20% - 단기 폭등, 고위험
}

# 전략별 손절 규칙 (수익률 %)
STOP_RULES = {
    "pullback": 0.07,    # 7% 손절
    "breakout": 0.05,    # 5% 손절 (더 타이트)
    "momentum": 0.08,    # 8% 손절 (더 느슨함)
}

# 시장 레짐별 전략 활성화
# "bull": 강세장, "sideways": 박스권, "bear": 약세장
STRATEGY_REGIME = {
    "pullback": ["bull", "sideways"],  # 모든 시장에서 작동
    "breakout": ["bull"],               # 강세장에서만
    "momentum": ["bull"],               # 강세장에서만
}

# 전략별 최대 포지션 수
MAX_POSITIONS_PER_STRATEGY = {
    "pullback": 4,      # 보수적
    "breakout": 3,      # 중간
    "momentum": 2,      # 공격적 (낮은 수)
}

# 전체 최대 포지션 수
MAX_TOTAL_POSITIONS = 8

# 포트폴리오 최적화 설정
PORTFOLIO_CONFIG = {
    "max_sector_exposure": 0.30,      # 섹터당 최대 30% 노출
    "max_single_position": 0.15,      # 단일 종목 최대 15%
    "correlation_threshold": 0.70,    # 상관계수 70% 이상이면 하나만 진입
}

# 신호 필터링 설정
SIGNAL_CONFIG = {
    "min_confidence": 0.5,            # 최소 신뢰도 50%
    "min_signal_strength": 0.5,       # 최소 신호 강도 50%
}
