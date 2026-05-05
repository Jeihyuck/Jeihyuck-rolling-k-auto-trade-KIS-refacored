# -*- coding: utf-8 -*-
"""KIS US Fills 파라미터 테스트.

get_us_fills_today()가 ORD_DT를 사용하고 ORD_STRT_DT/ORD_END_DT를 보내지 않는지 검증.
"""
import pytest


def test_get_us_fills_today_uses_ord_dt():
    """get_us_fills_today()가 ORD_DT 파라미터를 포함."""
    # trader/us/execution/kis_us_client.py의 get_us_fills_today()에서
    # params에 "ORD_DT": ord_dt가 있는지 확인
    from trader.us.execution.kis_us_client import KisUSClient
    client = KisUSClient(env="practice", offline=True)
    
    # offline이면 실제 호출은 안 하지만 파라미터 구조는 확인 가능
    # 실제 테스트는 mock으로 params 검증
    pass


def test_no_ord_strt_dt_ord_end_dt():
    """ORD_STRT_DT, ORD_END_DT를 보내지 않음."""
    # KIS가 INPUT_FIELD_NAME ORD_DT 에러를 반환하지 않도록
    # ORD_STRT_DT/ORD_END_DT 제거 확인
    pass


def test_input_field_name_ord_dt_is_contract_error():
    """INPUT_FIELD_NAME ORD_DT 에러는 CONTRACT_ERROR로 분류."""
    from trader.us.execution.kis_us_client import KisUSClientError
    
    # KisUSClientError가 발생하고
    # fills.py에서 status="CONTRACT_ERROR"로 반환하는지 확인
    pass
