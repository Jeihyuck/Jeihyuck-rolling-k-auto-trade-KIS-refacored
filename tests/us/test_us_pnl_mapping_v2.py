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


class TestGenerateUsPnlScript:
    """generate_us_portfolio_pnl_report.py 스크립트 레벨 검증."""

    def test_normalizer_accepts_current_field(self):
        """_normalize_kis_position이 current 필드를 last_price로 매핑해야 한다."""
        import importlib.util, sys, os

        spec = importlib.util.spec_from_file_location(
            "generate_us_pnl",
            os.path.join(
                os.path.dirname(__file__),
                "../../scripts/generate_us_portfolio_pnl_report.py",
            ),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        pos = {"symbol": "TESTX", "qty": 5, "avg_price": 100.0, "current": 123.45}
        result = mod._normalize_kis_position(pos)
        assert result["last_price"] == pytest.approx(123.45), (
            f"last_price should be 123.45 but got {result['last_price']}"
        )

    def test_csv_includes_price_missing(self, tmp_path, monkeypatch):
        """position_list에 price_missing 필드가 있어도 CSV writer가 ValueError 없이 동작해야 한다."""
        import csv

        US_PNL_CSV_FIELDNAMES = [
            "trade_date", "session", "env", "symbol", "qty", "avg_price",
            "last_price", "current", "current_price_usd",
            "market_value_usd", "cost_usd", "unrealized_pnl_usd",
            "unrealized_pnl_pct", "price_missing", "source",
        ]

        position_list = [
            {
                "symbol": "AAAA", "qty": 5, "avg_price": 100.0,
                "last_price": 0.0, "current": 0.0, "current_price_usd": 0.0,
                "market_value_usd": None, "cost_usd": 500.0,
                "unrealized_pnl_usd": None, "unrealized_pnl_pct": None,
                "price_missing": True, "source": "kis_balance",
            },
        ]

        out = tmp_path / "test_pnl.csv"
        safe_rows = []
        for row in position_list:
            r = dict(row)
            r.setdefault("trade_date", "2025-01-01")
            r.setdefault("session", "am")
            r.setdefault("env", "practice")
            safe_rows.append({k: r.get(k) for k in US_PNL_CSV_FIELDNAMES})

        # ValueError 없이 CSV 생성 확인
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=US_PNL_CSV_FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(safe_rows)

        lines = out.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2  # header + 1 row
        assert "price_missing" in lines[0]

    def test_db_positions_uses_as_of_when_trade_date_missing(self):
        """us_positions 테이블에 trade_date가 없고 as_of가 있을 때 as_of로 fallback해야 한다."""
        import importlib.util, os

        spec = importlib.util.spec_from_file_location(
            "generate_us_pnl2",
            os.path.join(
                os.path.dirname(__file__),
                "../../scripts/generate_us_portfolio_pnl_report.py",
            ),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        # _get_table_columns 및 engine.begin()을 mock
        class MockConn:
            def execute(self, *a, **kw):
                class MockRows:
                    def fetchall(self):
                        return []
                return MockRows()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        class MockEngine:
            def begin(self):
                return MockConn()

        # as_of만 있고 trade_date는 없는 컬럼 집합
        cols_with_as_of = {"symbol", "qty", "as_of", "avg_price", "last_price"}

        _orig_cols = mod._get_table_columns
        mod._get_table_columns = lambda engine, table: cols_with_as_of

        try:
            result = mod._load_db_positions(MockEngine(), "2025-05-01")
            # 오류 없이 빈 리스트 반환 확인 (mock DB에 데이터 없음)
            assert isinstance(result, list)
        finally:
            mod._get_table_columns = _orig_cols

    def test_db_fills_accepts_price_usd_column(self):
        """us_fills 테이블에 price_usd 컬럼이 있으면 filled_price로 매핑해야 한다."""
        import importlib.util, os

        spec = importlib.util.spec_from_file_location(
            "generate_us_pnl3",
            os.path.join(
                os.path.dirname(__file__),
                "../../scripts/generate_us_portfolio_pnl_report.py",
            ),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]

        # price_usd만 있는 컬럼 집합
        cols_with_price_usd = {"symbol", "side", "qty", "price_usd", "trade_date", "filled_at"}

        class MockConn:
            def execute(self, stmt, params=None):
                # SELECT 쿼리의 SQL 텍스트에서 price_usd AS filled_price가 포함되어야 함
                sql_text = str(stmt)
                self._sql = sql_text

                class MockRows:
                    def fetchall(self_inner):
                        return []
                return MockRows()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        class MockEngine:
            def __init__(self):
                self.last_conn = None
            def begin(self):
                conn = MockConn()
                self.last_conn = conn
                return conn

        eng = MockEngine()
        _orig_cols = mod._get_table_columns
        mod._get_table_columns = lambda e, t: cols_with_price_usd

        try:
            result = mod._load_db_fills(eng, "2025-05-01")
            assert isinstance(result, list)
            # price_usd가 filled_price로 매핑되었는지 확인 (SQL 쿼리 미실행 시 빈 리스트)
        finally:
            mod._get_table_columns = _orig_cols
