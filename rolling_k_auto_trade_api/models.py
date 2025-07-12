from pydantic import BaseModel


class OrderBase(BaseModel):
    account_no: str         # 종합계좌번호 (8자리)
    product_code: str       # 계좌상품코드 (2자리, 일반적으로 '01')
    code: str               # 종목코드
    order_type: str         # 주문구분 (00: 지정가, 01: 시장가 등)
    quantity: int           # 수량
    price: str              # 단가 (시장가인 경우 '0')

class BuyOrderRequest(OrderBase):
    pass

class SellOrderRequest(OrderBase):
    pass

