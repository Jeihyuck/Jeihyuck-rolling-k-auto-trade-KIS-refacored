# settings.py (전체 통합형 예시: 실전 전략 + API + 환경옵션 한눈에 관리)
import os
import logging

def safe_strip(val):
    """모든 입력값에서 개행, 캐리지리턴, 양쪽 공백 제거."""
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()


SLACK_WEBHOOK = safe_strip(os.getenv("SLACK_WEBHOOK", ""))

# === API/계정/실전 환경 ===
APP_KEY        = safe_strip(os.getenv("KIS_APP_KEY"))
APP_SECRET     = safe_strip(os.getenv("KIS_APP_SECRET"))
CANO           = safe_strip(os.getenv("CANO"))
ACNT_PRDT_CD   = safe_strip(os.getenv("ACNT_PRDT_CD"))
KIS_ENV        = safe_strip(os.getenv("KIS_ENV", "practice"))

if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443"
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443"

KIS_ACCOUNT    = safe_strip(os.getenv("KIS_ACCOUNT", ""))
KIS_REST_URL   = safe_strip(os.getenv("KIS_REST_URL", ""))
KIS_WS_URL     = safe_strip(os.getenv("KIS_WS_URL", ""))

# === 실전 전략/실험 환경 변수 ===
#REBALANCE_ANCHOR      = os.getenv("REBALANCE_ANCHOR", "first")
REBALANCE_ANCHOR="weekly"

DAILY_CAPITAL         = int(os.getenv("DAILY_CAPITAL", "250000000"))
SLIPPAGE_LIMIT_PCT    = float(os.getenv("SLIPPAGE_LIMIT_PCT", "0.25"))
PARTIAL1              = float(os.getenv("PARTIAL1", "0.5"))
PARTIAL2              = float(os.getenv("PARTIAL2", "0.3"))
TRAIL_PCT             = float(os.getenv("TRAIL_PCT", "0.02"))
FAST_STOP             = float(os.getenv("FAST_STOP", "0.01"))
ATR_STOP              = float(os.getenv("ATR_STOP", "1.5"))
TIME_STOP_HHMM        = os.getenv("TIME_STOP_HHMM", "13:00")
DEFAULT_PROFIT_PCT    = float(os.getenv("DEFAULT_PROFIT_PCT", "3.0"))
DEFAULT_LOSS_PCT      = float(os.getenv("DEFAULT_LOSS_PCT", "5.0"))
SELL_FORCE_TIME       = os.getenv("SELL_FORCE_TIME", "14:40")
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"
API_RATE_SLEEP_SEC    = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "2"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "4"))
SLIPPAGE_ENTER_GUARD_PCT = float(os.getenv("SLIPPAGE_ENTER_GUARD_PCT", "2.5"))
W_MAX_ONE             = float(os.getenv("W_MAX_ONE", "0.25"))
W_MIN_ONE             = float(os.getenv("W_MIN_ONE", "0.03"))

# === best_k_meta_strategy, 전략/백테스트용 필터 ===
K_MIN                 = float(os.getenv("K_MIN", "0.1"))
K_MAX                 = float(os.getenv("K_MAX", "1.0"))
K_STEP                = float(os.getenv("K_STEP", "0.1"))
K_GRID_MODE           = os.getenv("K_GRID_MODE", "fixed").lower()
K_STEP_FINE           = float(os.getenv("K_STEP_FINE", "0.05"))
K_DYNAMIC_STEP_MIN    = float(os.getenv("K_DYNAMIC_STEP_MIN", "0.03"))
K_DYNAMIC_STEP_MAX    = float(os.getenv("K_DYNAMIC_STEP_MAX", "0.10"))
K_DYNAMIC_STEP_MULT   = float(os.getenv("K_DYNAMIC_STEP_MULT", "1.5"))
MIN_TRADES            = int(os.getenv("MIN_TRADES", "5"))
MAX_MDD_PCT           = float(os.getenv("MAX_MDD_PCT", "30"))
REQUIRE_POS_RET       = os.getenv("REQUIRE_POS_RET", "true").lower() == "true"
TOP_N                 = int(os.getenv("TOP_N", "50"))
ALWAYS_INCLUDE_CODES  = {c.strip() for c in os.getenv("ALWAYS_INCLUDE_CODES", "").replace(" ", "").split(",") if c.strip()}
KEEP_HELD_BYPASS_FILTERS = os.getenv("KEEP_HELD_BYPASS_FILTERS", "true").lower() == "true"
HELD_MIN_WEIGHT       = float(os.getenv("HELD_MIN_WEIGHT", "0.01"))

# === 로깅 설정 ===
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")
logger.info(f"[환경변수 체크] 기타 옵션들 정상 적용됨")
