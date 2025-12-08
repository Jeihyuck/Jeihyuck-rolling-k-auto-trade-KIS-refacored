import os

# =========================
# [CONFIG] .env 없이도 동작
# - 아래 값을 기본으로 사용
# - (선택) 동일 키를 환경변수로 넘기면 override
# =========================
CONFIG = {
    "SELL_FORCE_TIME": "15:25",
    "SELL_ALL_BALANCES_AT_CUTOFF": "false",  # "true"면 커트오프에 전체 잔고 포함 강제매도 루틴 사용
    "API_RATE_SLEEP_SEC": "0.5",
    "FORCE_SELL_PASSES_CUTOFF": "2",
    "FORCE_SELL_PASSES_CLOSE": "4",
    "PARTIAL1": "0.5",
    "PARTIAL2": "0.3",
    "TRAIL_PCT": "0.02",
    "FAST_STOP": "0.01",
    "ATR_STOP": "1.5",
    "TIME_STOP_HHMM": "13:00",
    "DEFAULT_PROFIT_PCT": "3.0",
    "DEFAULT_LOSS_PCT": "5.0",
    "DAILY_CAPITAL": "250000000",
    "CAP_CAP": "0.8",
    "SLIPPAGE_LIMIT_PCT": "0.25",
    "SLIPPAGE_ENTER_GUARD_PCT": "2.5",
    "VWAP_TOL": "0.003",  # 🔸 VWAP 허용 오차(기본 0.3%)
    "W_MAX_ONE": "0.25",
    "W_MIN_ONE": "0.03",
    "REBALANCE_ANCHOR": "weekly",  # weekly | today | monthly
    "WEEKLY_ANCHOR_REF": "last",  # NEW: 'last'(직전 일요일) | 'next'(다음 일요일)
    "MOMENTUM_OVERRIDES_FORCE_SELL": "true",
    # 레짐(코스닥) 파라미터
    "KOSDAQ_INDEX_CODE": "KOSDAQ",
    "KOSDAQ_ETF_FALLBACK": "229200",
    "REG_BULL_MIN_UP_PCT": "0.5",
    "REG_BULL_MIN_MINUTES": "10",
    "REG_BEAR_VWAP_MINUTES": "10",
    "REG_BEAR_DROP_FROM_HIGH": "0.7",
    "REG_BEAR_STAGE1_MINUTES": "20",
    "REG_BEAR_STAGE2_ADD_DROP": "0.5",
    "REG_PARTIAL_S1": "0.30",
    "REG_PARTIAL_S2": "0.30",
    "TRAIL_PCT_BULL": "0.025",
    "TRAIL_PCT_BEAR": "0.012",
    "TP_PROFIT_PCT_BULL": "3.5",
    # 신고가 돌파 후 3일 눌림 + 반등 매수용 파라미터
    "USE_PULLBACK_ENTRY": "true",  # true면 '신고가 → 3일 연속 하락 → 반등' 패턴 충족 시에만 눌림목 진입 허용
    "PULLBACK_LOOKBACK": "60",  # 신고가 탐색 범위(거래일 기준)
    "PULLBACK_DAYS": "3",  # 연속 하락 일수
    "PULLBACK_REVERSAL_BUFFER_PCT": "0.2",  # 되돌림 확인 여유(%): 직전 하락일 고가 대비 여유율
    "PULLBACK_TOPN": "50",  # 눌림목 스캔용 각 시장별 시총 상위 종목 수
    "PULLBACK_UNIT_WEIGHT": "0.03",  # 눌림목 매수 1건당 자본 배분(활성 자본 비율)
    # 챔피언 후보 필터
    "CHAMPION_MIN_TRADES": "5",  # 최소 거래수
    "CHAMPION_MIN_WINRATE": "45.0",  # 최소 승률(%)
    "CHAMPION_MAX_MDD": "30.0",  # 최대 허용 MDD(%)
    "CHAMPION_MIN_SHARPE": "0.0",  # 최소 샤프 비율
    # 기타
    "MARKET_DATA_WHEN_CLOSED": "false",
    "FORCE_WEEKLY_REBALANCE": "0",
    # NEW: 1분봉 VWAP 모멘텀 파라미터
    "MOM_FAST": "5",  # 1분봉 fast MA 길이
    "MOM_SLOW": "20",  # 1분봉 slow MA 길이
    "MOM_TH_PCT": "0.5",  # fast/slow 괴리 임계값(%) – 0.5% 이상이면 강세로 본다
    # 시간 구간
    "ACTIVE_START_HHMM": "09:30",
    "FULL_ACTIVE_END_HHMM": "14:30",
    "CLOSE_BET_PREP_START_HHMM": "14:30",
    "CLOSE_BET_ENTRY_START_HHMM": "15:10",
    "MARKET_CLOSE_HHMM": "15:30",
    # 종가 베팅
    "CLOSE_BET_TOPN": "5",
    "CLOSE_BET_CAP_FRACTION": "0.2",
    "CLOSE_BET_MIN_RET_PCT": "3.0",
    "CLOSE_BET_MAX_PULLBACK_PCT": "3.0",
    "CLOSE_BET_MIN_VOL_SPIKE": "2.0",
    # 코어 포지션
    "ENABLE_CORE_POSITIONS": "true",
    "CORE_MAX_FRACTION": "0.6",
    "CORE_W_MAX_ONE": "0.10",
    "CORE_SCAN_TOPN": "250",
    "CORE_BOX_RANGE_PCT": "5.0",
    "CORE_BREAKOUT_PCT": "2.0",
    # 유니버스 구성 (코스닥/코스피 비율 및 사용 여부)
    "UNIVERSE_INCLUDE_MARKETS": "KOSDAQ,KOSPI",  # "KOSDAQ", "KOSPI", "KOSDAQ,KOSPI"
    "UNIVERSE_KOSDAQ_TOPN": "50",
    "UNIVERSE_KOSPI_TOPN": "50",
}


def cfg(key: str) -> str:
    """환경변수 > CONFIG 기본값."""
    return os.getenv(key, CONFIG.get(key, ""))


__all__ = ["CONFIG", "cfg"]
