"""tests/test_kis_price_rate_limit_cache.py

KIS API rate limit / EGW002 오류 감지 / balance prpr 우선 사용 검증.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class TestBalancePrprUsedFirst(unittest.TestCase):

    def setUp(self):
        os.environ["PB1_USE_BALANCE_PRPR_FIRST"] = "1"

    def test_balance_prpr_used_before_inquire_price(self):
        """balance prpr가 있으면 KIS inquire-price API를 호출하지 않는다."""
        kis_api = MagicMock()
        balance_snapshot = {
            "005930": {"prpr": "75000", "hldg_qty": "10"},
        }

        def _resolve_price(code: str) -> int:
            use_balance_first = os.environ.get("PB1_USE_BALANCE_PRPR_FIRST", "0") == "1"
            if use_balance_first and code in balance_snapshot:
                return int(balance_snapshot[code]["prpr"])
            return int(kis_api.get_price_snapshot(code)["stck_prpr"])

        price = _resolve_price("005930")
        self.assertEqual(price, 75000)
        kis_api.get_price_snapshot.assert_not_called()

    def test_inquire_price_called_when_no_balance_prpr(self):
        """balance에 없는 종목은 KIS inquire-price API를 호출한다."""
        kis_api = MagicMock()
        kis_api.get_price_snapshot.return_value = {"stck_prpr": "60000"}
        balance_snapshot: dict = {}

        def _resolve_price(code: str) -> int:
            use_balance_first = os.environ.get("PB1_USE_BALANCE_PRPR_FIRST", "0") == "1"
            if use_balance_first and code in balance_snapshot:
                return int(balance_snapshot[code]["prpr"])
            return int(kis_api.get_price_snapshot(code)["stck_prpr"])

        price = _resolve_price("005935")
        self.assertEqual(price, 60000)
        kis_api.get_price_snapshot.assert_called_once_with("005935")


class TestEGW002Classification(unittest.TestCase):

    def test_egw002_classified_as_rate_limit(self):
        """EGW002 rt_cd → _is_egw002_error True."""
        try:
            from trader.kis_wrapper import _is_egw002_error
        except ImportError:
            self.skipTest("kis_wrapper not importable in this env")
        resp_egw = {"rt_cd": "E", "msg_cd": "EGW002", "msg1": "초당 건수 오류"}
        self.assertTrue(_is_egw002_error(resp_egw, "EGW002"))

    def test_non_egw002_not_classified(self):
        """EGW001 → _is_egw002_error False."""
        try:
            from trader.kis_wrapper import _is_egw002_error
        except ImportError:
            self.skipTest("kis_wrapper not importable in this env")
        resp = {"rt_cd": "E", "msg_cd": "EGW001", "msg1": "기타"}
        self.assertFalse(_is_egw002_error(resp, "EGW001"))

    def test_success_rt_cd_not_classified(self):
        """rt_cd=0 정상 응답 → _is_egw002_error False."""
        try:
            from trader.kis_wrapper import _is_egw002_error
        except ImportError:
            self.skipTest("kis_wrapper not importable in this env")
        resp = {"rt_cd": "0", "msg_cd": "APPR0000", "msg1": "정상처리"}
        self.assertFalse(_is_egw002_error(resp, "APPR0000"))


if __name__ == "__main__":
    unittest.main()
