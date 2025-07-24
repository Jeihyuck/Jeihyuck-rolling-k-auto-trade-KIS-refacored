import os

APP_KEY = os.getenv("KIS_APP_KEY").strip()
APP_SECRET = os.getenv("KIS_APP_SECRET").strip()
CANO = os.getenv("CANO").strip()
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD").strip()
KIS_ENV = os.getenv("KIS_ENV", "practice").strip()

if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443".strip()
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443".strip()
