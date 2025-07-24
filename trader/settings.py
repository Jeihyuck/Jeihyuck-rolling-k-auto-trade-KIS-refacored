import os

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD")
KIS_ENV = os.getenv("KIS_ENV", "practice")

# ✅ API_BASE_URL 정의 추가
if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443"
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443"

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")


# 예시: 종목코드 → 목표가
TARGETS = {
    "005930": 75000,
    "000660": 120000
}

# 폴링 주기 (5분)
POLL_INTERVAL = 300
