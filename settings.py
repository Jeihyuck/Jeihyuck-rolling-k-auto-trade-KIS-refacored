# FILE: settings.py
import os
import logging
from pathlib import Path

# -------------------------------
# 공통 유틸
# -------------------------------

def safe_strip(val):
    """모든 입력값에서 개행/캐리지리턴/양쪽 공백 제거 후 문자열 반환."""
    if val is None:
        return ""
    if isinstance(val, str):
        return val.replace("\n", "").replace("\r", "").strip()
    return str(val).strip()


def _getenv(name: str, default: str = "") -> str:
    """환경변수를 읽고 safe_strip 적용."""
    return safe_strip(os.getenv(name, default))


def _getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "y", "yes", "on"}


def _seed_env(name: str, value: str) -> None:
    """환경변수에 기본값을 주입(미설정시에만). trader.py가 os.getenv로 읽는 항목 보호."""
    if name not in os.environ or str(os.environ[name]).strip() == "":
        os.environ[name] = str(value)


# -------------------------------
# KIS 인증/환경
# -------------------------------
APP_KEY       = _getenv("KIS_APP_KEY")
APP_SECRET    = _getenv("KIS_APP_SECRET")
CANO          = _getenv("CANO")                 # 8자리
ACNT_PRDT_CD  = _getenv("ACNT_PRDT_CD", "01")   # 2자리
KIS_ENV       = _getenv("KIS_ENV", "practice")   # practice / real

if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443"
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443"

# 선택 옵션(미사용일 수 있음)
KIS_ACCOUNT = _getenv("KIS_ACCOUNT")
KIS_REST_URL = _getenv("KIS_REST_URL")
KIS_WS_URL = _getenv("KIS_WS_URL")


# -------------------------------
# 디렉토리
# -------------------------------
BASE_DIR = Path(__file__).parent
LOG_DIR = (BASE_DIR / "trader" / "logs").resolve()
REPORT_DIR = (BASE_DIR / "trader" / "reports").resolve()
LOG_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------
# 스케줄/리스크 파라미터 (환경변수 ← 기본값)
#   * trader.py는 os.getenv로 직접 읽음 → 아래에서 환경변수 seed
# -------------------------------
# 리밸런싱 기준일 앵커: first(월초)/today(당일)
REBALANCE_ANCHOR = _getenv("REBALANCE_ANCHOR", "first").lower()

# 스냅샷 & 커트오프/종료 시간(KST, HH:MM)
REBALANCE_SNAPSHOT_TIME_KST = _getenv("REBALANCE_SNAPSHOT_TIME_KST", "08:50")
MIDDAY_SNAPSHOT_TIME_KST    = _getenv("MIDDAY_SNAPSHOT_TIME_KST", "12:00")
SELL_FORCE_TIME_KST         = _getenv("SELL_FORCE_TIME_KST", "14:30")
ACTION_KILL_TIME_KST        = _getenv("ACTION_KILL_TIME_KST", "14:35")

# trader.py 호환(환경변수 이름 그대로도 제공)
SELL_FORCE_TIME  = _getenv("SELL_FORCE_TIME", SELL_FORCE_TIME_KST)
ACTION_KILL_TIME = _getenv("ACTION_KILL_TIME", ACTION_KILL_TIME_KST)
_seed_env("SELL_FORCE_TIME", SELL_FORCE_TIME)
_seed_env("ACTION_KILL_TIME", ACTION_KILL_TIME)

# 장중/마감 강제청산 패스 수 & API rate sleep
FORCE_SELL_PASSES_CUTOFF = int(_getenv("FORCE_SELL_PASSES_CUTOFF", "2"))
FORCE_SELL_PASSES_CLOSE  = int(_getenv("FORCE_SELL_PASSES_CLOSE", "4"))
API_RATE_SLEEP_SEC       = float(_getenv("API_RATE_SLEEP_SEC", "0.5"))
SELL_ALL_BALANCES_AT_CUTOFF = _getenv_bool("SELL_ALL_BALANCES_AT_CUTOFF", True)
_seed_env("FORCE_SELL_PASSES_CUTOFF", str(FORCE_SELL_PASSES_CUTOFF))
_seed_env("FORCE_SELL_PASSES_CLOSE", str(FORCE_SELL_PASSES_CLOSE))
_seed_env("API_RATE_SLEEP_SEC", str(API_RATE_SLEEP_SEC))
_seed_env("SELL_ALL_BALANCES_AT_CUTOFF", "true" if SELL_ALL_BALANCES_AT_CUTOFF else "false")

# 진입/청산 파라미터
PARTIAL1 = float(_getenv("PARTIAL1", "0.5"))    # TP1 도달 시 매도 비중
PARTIAL2 = float(_getenv("PARTIAL2", "0.3"))    # TP2 도달 시 매도 비중
TRAIL_PCT = float(_getenv("TRAIL_PCT", "0.02"))  # 고점대비 하락폭
FAST_STOP = float(_getenv("FAST_STOP", "0.01"))  # 진입 5분내 급락 손절
ATR_STOP  = float(_getenv("ATR_STOP", "1.5"))    # ATR x 배수 손절(절대)
TIME_STOP_HHMM = _getenv("TIME_STOP_HHMM", "13:00")
DEFAULT_PROFIT_PCT = float(_getenv("DEFAULT_PROFIT_PCT", "3.0"))
DEFAULT_LOSS_PCT   = float(_getenv("DEFAULT_LOSS_PCT", "-2.0"))
for k, v in (
    ("PARTIAL1", PARTIAL1), ("PARTIAL2", PARTIAL2), ("TRAIL_PCT", TRAIL_PCT),
    ("FAST_STOP", FAST_STOP), ("ATR_STOP", ATR_STOP), ("TIME_STOP_HHMM", TIME_STOP_HHMM),
    ("DEFAULT_PROFIT_PCT", DEFAULT_PROFIT_PCT), ("DEFAULT_LOSS_PCT", DEFAULT_LOSS_PCT),
):
    _seed_env(k, str(v))

# RK-Max 강화 파라미터
DAILY_CAPITAL = int(_getenv("DAILY_CAPITAL", "3000000"))            # 일일 총 집행 금액(원)
SLIPPAGE_LIMIT_PCT = float(_getenv("SLIPPAGE_LIMIT_PCT", "0.15"))   # 정보성 로깅 임계
SLIPPAGE_ENTER_GUARD_PCT = float(_getenv("SLIPPAGE_ENTER_GUARD_PCT", "1.5"))
W_MAX_ONE = float(_getenv("W_MAX_ONE", "0.25"))
W_MIN_ONE = float(_getenv("W_MIN_ONE", "0.03"))
for k, v in (
    ("DAILY_CAPITAL", DAILY_CAPITAL), ("SLIPPAGE_LIMIT_PCT", SLIPPAGE_LIMIT_PCT),
    ("SLIPPAGE_ENTER_GUARD_PCT", SLIPPAGE_ENTER_GUARD_PCT), ("W_MAX_ONE", W_MAX_ONE), ("W_MIN_ONE", W_MIN_ONE),
):
    _seed_env(k, str(v))

# 선택 전략 파라미터(유니버스/윈도우)
CARRY_MAX_DAYS = int(_getenv("CARRY_MAX_DAYS", "3"))
TOP_K          = int(_getenv("TOP_K", "10"))
BENCH          = int(_getenv("BENCH", "4"))
LOOKBACK_DAYS  = int(_getenv("LOOKBACK_DAYS", "10"))
SPREAD_REPRICE_PCT = float(_getenv("SPREAD_REPRICE_PCT", "1"))
TIMEOUT_SEC    = float(_getenv("TIMEOUT_SEC", "2"))

# trader 호환을 위해 seed (선택)
for k, v in (
    ("REBALANCE_ANCHOR", REBALANCE_ANCHOR),
    ("REBALANCE_SNAPSHOT_TIME_KST", REBALANCE_SNAPSHOT_TIME_KST),
    ("MIDDAY_SNAPSHOT_TIME_KST", MIDDAY_SNAPSHOT_TIME_KST),
):
    _seed_env(k, str(v))


# -------------------------------
# 로깅
# -------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")
logger.info(f"[스케줄] SELL_FORCE_TIME={SELL_FORCE_TIME} / ACTION_KILL_TIME={ACTION_KILL_TIME} / TIME_STOP_HHMM={TIME_STOP_HHMM}")
logger.info(f"[리스크] PARTIAL1={PARTIAL1}, PARTIAL2={PARTIAL2}, TRAIL_PCT={TRAIL_PCT}, FAST_STOP={FAST_STOP}, ATR_STOP={ATR_STOP}")
logger.info(f"[집행] DAILY_CAPITAL={DAILY_CAPITAL:,}, API_RATE_SLEEP_SEC={API_RATE_SLEEP_SEC}")
