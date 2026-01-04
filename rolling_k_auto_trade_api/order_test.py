import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
ACCOUNT = os.getenv("KIS_ACCOUNT")
CANO = ACCOUNT[:8]
ACNT_PRDT_CD = ACCOUNT[8:]
BASE_URL = os.getenv("KIS_REST_URL", "https://openapivts.koreainvestment.com:29443")

def get_access_token():
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
    }
    res = requests.post(url, json=data, headers=headers)
    if res.status_code == 200:
        token = res.json().get("access_token")
        print("✅ ACCESS_TOKEN 발급 성공")
        return token
    print("❌ ACCESS_TOKEN 발급 실패", res.text)
    return None

def get_hashkey(body: dict, token: str):
    url = f"{BASE_URL}/uapi/hashkey"
    headers = {
        "content-type": "application/json",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "authorization": f"Bearer {token}",
    }
    resp = requests.post(url, headers=headers, data=json.dumps(body))
    if resp.status_code == 200:
        hashkey = resp.json().get("HASH")
        print("✅ hashkey 발급 성공")
        return hashkey
    else:
        print(f"❌ hashkey 발급 실패: {resp.text}")
        return None

def order_headers(hashkey, token):
    return {
        "content-type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC0807U",   # 모의투자 지정가
        "hashkey": hashkey,
    }

def test_send_order(token, code="005930", qty=1, price=85000, ord_dvsn="00"):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    body = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": ord_dvsn,      # "00": 지정가, "03": 시장가
        "ORD_QTY": str(qty),
        "ORD_UNPR": str(price),
    }
    hashkey = get_hashkey(body, token)
    if not hashkey:
        print("❌ hashkey 발급 실패로 주문 중단")
        return None

    headers = order_headers(hashkey, token)
    print(f"📦 주문 바디: {body}")
    resp = requests.post(url, headers=headers, json=body)
    print(f"🔎 주문 Status Code: {resp.status_code}")
    try:
        result = resp.json()
        if result.get("rt_cd") == "0":
            print("✅ 주문 성공!")
        else:
            print(f"❌ 주문 오류: {result.get('msg1')}")
        print("📋 주문 전체 응답:", result)
        return result
    except Exception as e:
        print("❌ 주문 응답 파싱 오류:", e)
        print(resp.text)
        return None

if __name__ == "__main__":
    token = get_access_token()
    if token:
        # ⭐️ 여기서 코드/수량/가격/주문유형 등 자유롭게 바꿔서 테스트 가능
        test_send_order(token, code="005930", qty=1, price=85000, ord_dvsn="00")  # 지정가 매수
