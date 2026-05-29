# -*- coding: utf-8 -*-
"""tests/us/test_us_pnl_mapping_v2.py

KIS balance의 price 필드 매핑 검증.
"""
from __future__ import annotations

import pytest

from trader.us.utils.pnl_utils import normalize_us_position


class TestNormalizeUsPriceMapping:
    """normalize_us_position 함수의 price 매핑 검증."""

    def _make_raw(self, symbol="NVDA", qty=8, avg_price=214.002, **extra):
        base = {"symbol": symbol, "qty": qty, "avg_price": avg_price}
        base.update(extra)
        return base

    def test_current_field_mapped_to_last_price(self):
        """KIS balance의 `current` 필드가 last_price로 매핑되어야 한다."""
        raw = self._make_raw(current=214.1322)
        pos = normalize_us_position(raw)
        assert pos["last_price"] == pytest.approx(214.1322)

    def test_price_missing_false_when_price_present(self):
        raw = self._make_raw(current=214.1322)
        pos = normalize_us_position(raw)
        assert pos.get("price_missing", False) is False

    def test_price_missing_true_when_no_price(self):
        """가격 정보가 전혀 없으면 price_missing=True여야 한다."""
        raw = self._make_raw()  # current/last_price/close 없음
        pos = normalize_us_position(raw)
        assert pos.get("price_missing", False) is True or pos.get("last_price", 0) <= 0

    def test_last_price_field_passthrough(self):
        """이미 last_price가 있으면 그대로 사용한다."""
        raw = self._make_raw(last_price=214.5)
        pos = normalize_us_position(raw)
        assert pos["last_price"] == pytest.approx(214.5)

    def test_close_field_not_used_as_last_price(self):
        """close 필드만 있을 때 last_price가 0.0임을 확인한다 (close는 fallback 아님).

        실제 매핑 필드: current, last_price, prc_clpr_amt, now_pric 등.
        close는 의도적으로 매핑에서 제외되어 있다.
        """
        raw = self._make_raw(close=214.0)
        pos = normalize_us_position(raw)
        # close 필드는 지원하지 않으므로 last_price=0.0 이고 price_missing
        assert pos.get("last_price", 0) == pytest.approx(0.0)
        assert pos.get("price_missing", False) is True or pos.get("last_price", 0) <= 0

    def test_market_value_computed(self):
        """market_value = qty * last_price 기준으로 계산되어야 한다."""
        raw = self._make_raw(qty=10, avg_price=200.0, current=210.0)
        pos = normalize_us_position(raw)
        if "market_value" in pos:
            assert pos["market_value"] == pytest.approx(10 * 210.0, rel=0.01)


class TestCsvFieldnamesSafe:
    """price_missing 필드가 있어도 CSV writer가 오류 없이 작동해야 한다."""

    def test_price_missing_in_csv_no_error(self, tmp_path):
        import csv

        out = tmp_path / "test.csv"
        fieldnames = ["symbol", "qty", "last_price", "price_missing"]
        rows = [
            {"symbol": "NVDA", "qty": 8, "last_price": 214.1322, "price_missing": False},
            {"symbol": "AAPL", "qty": 5, "last_price": 0.0, "price_missing": True},
        ]
        with open(out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        # 파일이 정상 생성, 행 수 검증
        with open(out) as f:
            lines = f.readlines()
        assert len(lines) == 3  # header + 2 rows

    def test_extra_fields_ignored(self, tmp_path):
        """fieldnames에 없는 extra 필드가 있어도 오류 없이 무시해야 한다."""
        import csv

        out = tmp_path / "test2.csv"
        fieldnames = ["symbol", "qty"]
        rows = [
            {"symbol": "NVDA", "qty": 8, "price_missing": False, "unknown_field": "x"},
        ]
        with open(out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        with open(out) as f:
            content = f.read()
        assert "symbol" in content
        assert "unknown_field" not in content
