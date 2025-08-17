# rolling_k_auto_trade_api/kis_wrapper.py

"""
얇은 래퍼: 기존 모듈들이 KisAPI 클래스를 기대할 때를 위한 호환 구현.
실제 로직은 rolling_k_auto_trade_api.kis_api의 함수들을 사용합니다.
"""

from typing import Optional, Dict, Any

class KisAPI:
    def __init__(self):
        # 초기화가 필요하면 여기에 추가 (예: 세션 생성)
        self._session = None

    def get_price_data(self, code: str) -> Dict[str, Any]:
        # 내부에서 실제 함수 임포트 (순환 import 안전)
        try:
            from .kis_api import get_price_data
            return get_price_data(code)
        except Exception as e:
            # 실패해도 import-time 에러를 내지 않도록 안전하게 처리
            return {"code": code, "price": None, "error": str(e)}

    def send_order(self, code: str, qty: int, price: int, side: str) -> Optional[dict]:
        try:
            from .kis_api import send_order
            return send_order(code=code, qty=qty, price=price, side=side)
        except Exception as e:
            return {"error": str(e)}

    def get_cash_balance(self) -> int:
        try:
            from .kis_api import get_cash_balance
            return get_cash_balance()
        except Exception:
            return 0

    def inquire_balance(self, code: str = None) -> dict:
        try:
            from .kis_api import inquire_balance
            return inquire_balance(code)
        except Exception as e:
            return {"qty": 0, "eval_amt": 0, "error": str(e)}

    def inquire_filled_order(self, ord_no: str) -> dict:
        try:
            from .kis_api import inquire_filled_order
            return inquire_filled_order(ord_no)
        except Exception as e:
            return {"error": str(e)}
