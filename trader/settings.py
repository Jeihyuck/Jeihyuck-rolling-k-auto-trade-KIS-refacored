import os

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")

# 예시: 종목코드 → 목표가
TARGETS = {
    "005930": 75000,
    "000660": 120000
}

# 폴링 주기 (5분)
POLL_INTERVAL = 300
