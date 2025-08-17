# settings.py
from __future__ import annotations
import os
import logging

# (선택) 로컬 개발 편의를 위해 .env 자동 로드 (없어도 에러 안 남)
try:
    from dotenv import load_dotenv, find_dotenv  # pip install python-dotenv
    _env_file = find_dotenv()
    if _env_file:
        load_dotenv(_env_file)
except Exception:
    pass

logger = logging.getLogger("settings")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")

def _safe(s: str | None) -> str:
    return (s or "").strip()

APP_KEY       = _safe(os.getenv("KIS_APP_KEY"))
APP_SECRET    = _safe(os.getenv("KIS_APP_SECRET"))
CANO          = _safe(os.getenv("CANO"))
ACNT_PRDT_CD  = _safe(os.getenv("ACNT_PRDT_CD"))
KIS_ENV       = _safe(os.getenv("KIS_ENV") or "practice").lower()

# API_BASE_URL: 주어지면 그대로, 없으면 KIS_ENV 기준 기본값
API_BASE_URL  = _safe(os.getenv("API_BASE_URL")) or (
    "https://openapivts.koreainvestment.com:29443" if KIS_ENV == "practice"
    else "https://openapi.koreainvestment.com:9443"
)

# --------- 필수값 즉시 검증 (GHA/서버에서 누락시 빠르게 실패) ----------
_missing = [k for k,v in {
    "KIS_APP_KEY": APP_KEY,
    "KIS_APP_SECRET": APP_SECRET,
    "CANO": CANO,
    "ACNT_PRDT_CD": ACNT_PRDT_CD,
}.items() if not v]

# GitHub Actions나 프로덕션에서 누락되면 바로 에러
if _missing:
    raise RuntimeError(
        f"[CONFIG] Missing required env vars: {_missing}. "
        f"Provide via GitHub Actions secrets (recommended) or a local .env."
    )

# 마스킹된 로그로 확인
logger.info("[환경변수 체크] APP_KEY=%s", (APP_KEY[:3] + "***") if APP_KEY else "")
logger.info("[환경변수 체크] CANO='%s'", CANO)
logger.info("[환경변수 체크] ACNT_PRDT_CD='%s'", ACNT_PRDT_CD)
logger.info("[환경변수 체크] API_BASE_URL='%s'", API_BASE_URL)
logger.info("[환경변수 체크] KIS_ENV='%s'", KIS_ENV)
