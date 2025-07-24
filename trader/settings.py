import os

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD")
KIS_ENV = os.getenv("KIS_ENV", "practice")

if KIS_ENV == "real":
    API_BASE_URL = "https://openapi.koreainvestment.com:9443"
else:
    API_BASE_URL = "https://openapivts.koreainvestment.com:29443"
