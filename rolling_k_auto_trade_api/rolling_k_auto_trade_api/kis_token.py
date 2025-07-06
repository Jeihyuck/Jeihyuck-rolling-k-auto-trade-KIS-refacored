import requests
import os
from dotenv import load_dotenv

ENV_PATH = ".env"
load_dotenv(ENV_PATH)

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")


def get_kis_access_token():
    url = "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"
    headers = {"Content-Type": "application/json"}
    payload = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    response = requests.post(url, headers=headers, json=payload)
    res_json = response.json()

    if "access_token" not in res_json:
        raise Exception(f"❌ Token 발급 실패: {res_json}")

    return res_json["access_token"]


def update_env_token(token: str):
    lines = []
    updated = False
    with open(ENV_PATH, "r") as f:
        for line in f:
            if line.startswith("KIS_ACCESS_TOKEN="):
                lines.append(f"KIS_ACCESS_TOKEN={token}\n")
                updated = True
            else:
                lines.append(line)
    if not updated:
        lines.append(f"KIS_ACCESS_TOKEN={token}\n")
    with open(ENV_PATH, "w") as f:
        f.writelines(lines)
    print("✅ KIS_ACCESS_TOKEN 자동 갱신 완료")
