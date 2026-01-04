# rolling_k_auto_trade_api/notify.py
import os
import requests

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_slack_message(message: str):
    if not SLACK_WEBHOOK:
        return {"error": "Slack webhook not configured"}
    payload = {"text": message}
    response = requests.post(SLACK_WEBHOOK, json=payload)
    return {"status": response.status_code, "text": response.text}


def send_telegram_message(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return {"error": "Telegram credentials not set"}
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    response = requests.post(url, data=payload)
    return {"status": response.status_code, "text": response.text}


def send_notification(message: str):
    slack_result = send_slack_message(message)
    telegram_result = send_telegram_message(message)
    return {"slack": slack_result, "telegram": telegram_result}
