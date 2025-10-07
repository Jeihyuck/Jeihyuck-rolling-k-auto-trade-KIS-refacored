# settings.py

import os
import logging

def safe_strip(val):
    """모든 입력값에서 개행, 캐리지리턴, 양쪽 공백 제거."""
    if val is None:
        return ''
    if isinstance(val, str):
        return val.replace('\n', '').replace('\r', '').strip()
    return str(val).strip()

def _env_first(*keys, default=""):
    """
    여러 키 중 앞에서부터 존재하는 첫 값을 반환.
    예) _env_first("KIS_APP_KEY", "APP_KEY")
    """
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return safe_strip(v)
    return safe_strip(default)

# -----------------------------------------------------------------------------
# 환경변수 로딩 (원본과 동일한 키 우선) + 호환 별칭 지원
# -----------------------------------------------------------------------------
APP_KEY      = _env_first("KIS_APP_KEY", "APP_KEY")
APP_SECRET   = _env_first("KIS_APP_SECRET", "APP_SECRET")

# 원본 키 유지 + 별칭(KIS_CANO/KIS_ACNT_PRDT_CD) 보조
CANO         = _env_first("CANO", "KIS_CANO")
ACNT_PRDT_CD = _env_first("ACNT_PRDT_CD", "KIS_ACNT_PRDT_CD")

# practice(모의) / real(실전) — 원본 기본값 유지
KIS_ENV      = safe_strip(os.getenv("KIS_ENV", "practice")).lower()

# 추가 옵션(원본 유지) — 비어 있어도 OK
KIS_ACCOUNT  = safe_strip(os.getenv("KIS_ACCOUNT", ""))
KIS_REST_URL = safe_strip(os.getenv("KIS_REST_URL", ""))
KIS_WS_URL   = safe_strip(os.getenv("KIS_WS_URL", ""))

# -----------------------------------------------------------------------------
# API_BASE_URL 결정 로직
# 1) 명시 오버라이드: API_BASE_URL(최우선) > KIS_REST_URL
# 2) 없으면 원본 방식(KIS_ENV에 따라 vts/real URL)
# -----------------------------------------------------------------------------
_api_override = safe_strip(os.getenv("API_BASE_URL", "")) or KIS_REST_URL
if _api_override:
    API_BASE_URL = _api_override
else:
    if KIS_ENV == "real":
        API_BASE_URL = "https://openapi.koreainvestment.com:9443"
    else:
        API_BASE_URL = "https://openapivts.koreainvestment.com:29443"

# -----------------------------------------------------------------------------
# 로깅 설정
# -----------------------------------------------------------------------------
_LOG_LEVEL = safe_strip(os.getenv("LOG_LEVEL", "INFO")).upper()
_level = getattr(logging, _LOG_LEVEL, logging.INFO)

# 이미 다른 쪽에서 기본 설정을 했을 수도 있으니, 핸들러가 없을 때만 설정
if not logging.getLogger().handlers:
    logging.basicConfig(level=_level)
else:
    logging.getLogger().setLevel(_level)

logger = logging.getLogger(__name__)

# 환경변수 체크 로그 (원본 포맷 유지)
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

# 필요시 추가 세팅/유틸 함수도 이 파일에 정의
