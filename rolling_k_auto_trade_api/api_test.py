import os
import requests
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
CANO = os.getenv("KIS_ACCOUNT")[:8]
ACNT_PRDT_CD = os.getenv("KIS_ACCOUNT")[8:]
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
        access_token = res.json().get("access_token")
        print("✅ ACCESS_TOKEN 발급 성공")
        return access_token
    else:
        print("❌ ACCESS_TOKEN 발급 실패", res.text)
        return None

def test_inquire_balance(access_token):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC8434R",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "OFL_YN": "N",
        "INQR_DVSN": "01",
        "PRCS_DVSN": "00",  # ✅ 여기 추가!
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    res = requests.get(url, headers=headers, params=params)

    print(f"🔎 Status Code: {res.status_code}")
    try:
        data = res.json()
        data = res.json()
        if data.get("rt_cd") == "0":
            print("✅ 예수금 조회 성공")
            output2_list = data.get("output2", [])
            if output2_list:
                output = output2_list[0]
                print(f"💰 총예수금: {output.get('dnca_tot_amt')} 원")
                print(f"📌 주문가능금액: {output.get('nxdy_excc_amt')} 원")
                print(f"📉 매수가능금액: {output.get('pchs_amt_smtl_amt')} 원")
            else:
                print("⚠️ output2 결과가 비어있습니다.")
        else:
            print(f"❌ 응답 오류: {data.get('msg1')}")
        print("📋 전체 응답:", data)
    except Exception as e:
        print("❌ JSON 파싱 오류:", e)
        print(res.text)

if __name__ == "__main__":
    token = get_access_token()
    if token:
        test_inquire_balance(token)
