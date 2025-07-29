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

# 환경변수 읽어서 무조건 safe_strip 적용
APP_KEY        = safe_strip(os.getenv("KIS_APP_KEY"))
APP_SECRET     = safe_strip(os.getenv("KIS_APP_SECRET"))
CANO           = safe_strip(os.getenv("CANO"))
ACNT_PRDT_CD   = safe_strip(os.getenv("ACNT_PRDT_CD"))
KIS_ENV        = safe_strip(os.getenv("KIS_ENV", "practice"))

# 실전/모의 투자 구분
if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443"
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443"

# 추가 옵션 (필요시)
KIS_ACCOUNT    = safe_strip(os.getenv("KIS_ACCOUNT", ""))
KIS_REST_URL   = safe_strip(os.getenv("KIS_REST_URL", ""))
KIS_WS_URL     = safe_strip(os.getenv("KIS_WS_URL", ""))

# 로깅 설정 (디버깅/실행 환경별 필요시)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"[환경변수 체크] APP_KEY={repr(APP_KEY)}")
logger.info(f"[환경변수 체크] CANO={repr(CANO)}")
logger.info(f"[환경변수 체크] ACNT_PRDT_CD={repr(ACNT_PRDT_CD)}")
logger.info(f"[환경변수 체크] API_BASE_URL={repr(API_BASE_URL)}")
logger.info(f"[환경변수 체크] KIS_ENV={repr(KIS_ENV)}")

# 필요시 추가 세팅/유틸 함수도 이 파일에 정의
