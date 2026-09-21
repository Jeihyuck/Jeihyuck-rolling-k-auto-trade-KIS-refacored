"""Tests: KIS US balance pagination cursor + duplicate row deduplication.

- Empty cursor stops pagination (no infinite loop)
- Whitespace cursor is treated as empty (strip fix)
- Same-page duplicate rows don't double quantities
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(symbol: str, exchange: str, qty: int, orderable: int = 0,
              avg_price: str = "100.0", mv: str = "1000.0", ba: str = "100.0") -> dict:
    """KIS API output1 row 형태 모킹 (kis_us_client._merge_duplicate_symbols이 읽는 필드 사용)."""
    return {
        "pdno": symbol,
        "ovrs_excg_cd": exchange,       # exchange field
        "ovrs_cblc_qty": str(qty),      # qty field
        "ord_psbl_qty": str(orderable or qty),
        "pchs_avg_pric": avg_price,     # avg_price field
        "ovrs_stck_evlu_amt": mv,       # market_value field
        "frcr_pchs_amt1": ba,           # buy_amount field
        "ovrs_now_pric1": "100.0",
    }


def _page(rows: list[dict], fk200: str = "", nk200: str = "") -> dict:
    """Simulate KIS API page response."""
    rt_cd = "0" if rows else "0"
    return {
        "rt_cd": rt_cd,
        "msg_cd": "KIOK0000",
        "msg1": "OK",
        "ctx_area_fk200": fk200,
        "ctx_area_nk200": nk200,
        "output1": rows,
        "output2": {"frcr_dncl_amt_2": "10000.0"},
    }


# ---------------------------------------------------------------------------
# Pagination stop tests
# ---------------------------------------------------------------------------

class TestPaginationStop:
    def test_empty_cursor_stops_on_second_page(self):
        """When second page returns empty cursors, pagination must stop."""
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)
        client.env = "practice"
        client._cano = "12345"
        client._acnt_prdt_cd = "01"

        page1_rows = [_make_row("CRDO", "NASD", 12)]
        page2_rows = [_make_row("LITE", "NASD", 2)]

        call_count = {"n": 0}

        def mock_get(path, headers=None, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _page(page1_rows, fk200="CURSOR1", nk200="CURSOR1")
            elif call_count["n"] == 2:
                return _page(page2_rows, fk200="", nk200="")
            else:
                raise AssertionError("Pagination should have stopped!")

        with patch.object(client, "_get", side_effect=mock_get), \
             patch.object(client, "_build_headers", return_value={}):
            result = client._get_us_balance_single_exchange("NASD")

        assert call_count["n"] == 2, f"Expected 2 API calls, got {call_count['n']}"
        symbols = [r.get("pdno") for r in result.get("output1", [])]
        assert "CRDO" in symbols
        assert "LITE" in symbols

    def test_whitespace_cursor_treated_as_empty(self):
        """Whitespace-only cursor (e.g., '   ') must be stripped and treated as empty."""
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)
        client.env = "practice"
        client._cano = "12345"
        client._acnt_prdt_cd = "01"

        page1_rows = [_make_row("VRT", "NASD", 6)]
        call_count = {"n": 0}

        def mock_get(path, headers=None, params=None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _page(page1_rows, fk200="   ", nk200="   ")
            else:
                raise AssertionError("Should not paginate when cursor is whitespace!")

        with patch.object(client, "_get", side_effect=mock_get), \
             patch.object(client, "_build_headers", return_value={}):
            result = client._get_us_balance_single_exchange("NASD")

        assert call_count["n"] == 1
        symbols = [r.get("pdno") for r in result.get("output1", [])]
        assert "VRT" in symbols


# ---------------------------------------------------------------------------
# Duplicate row deduplication tests
# ---------------------------------------------------------------------------

class TestDuplicateRowDedup:
    def test_us_balance_identical_duplicate_is_deduped(self):
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)
        row = _make_row("HELD", "NASD", 12)
        assert client._merge_duplicate_symbols([row, dict(row)]) == [row]

    def test_us_balance_conflicting_duplicate_is_not_silently_dropped(self, monkeypatch):
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient(offline=False)
        monkeypatch.setenv("US_BALANCE_EXCHANGES", "NASD")
        client._get_us_balance_single_exchange = lambda _exchange: {
            "output1": [_make_row("HELD", "NASD", 1), _make_row("HELD", "NASD", 2)],
            "output2": {},
        }
        result = client.get_us_balance(force_refresh=True)
        assert result["balance_authoritative"] is False
        assert result["failed_exchanges"]["BALANCE_CONFLICT"] == "HELD"
        assert int(result["output1"][0]["ovrs_cblc_qty"]) == 1

    def test_exact_duplicate_rows_skipped(self):
        """Two identical rows for the same symbol must result in qty=12 (not 24)."""
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)

        row = _make_row("CRDO", "NASD", 12, orderable=12, avg_price="50.0",
                        mv="600.0", ba="50.0")
        # Two identical rows (pagination duplication scenario)
        raw_rows = [dict(row), dict(row)]

        deduped = client._merge_duplicate_symbols(raw_rows)

        crdo_rows = [r for r in deduped if r["pdno"] == "CRDO"]
        assert len(crdo_rows) == 1, f"Expected 1 CRDO row, got {len(crdo_rows)}"
        # qty field is ovrs_cblc_qty in the mock row
        assert int(crdo_rows[0]["ovrs_cblc_qty"]) == 12

    def test_same_exchange_different_qty_keeps_first(self):
        """Same symbol+exchange with different qty: first row wins (no merge)."""
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)

        row1 = _make_row("SOXX", "NASD", 4, avg_price="200.0")
        row2 = _make_row("SOXX", "NASD", 4, avg_price="205.0")  # different avg_price
        raw_rows = [row1, row2]

        deduped = client._merge_duplicate_symbols(raw_rows)

        soxx_rows = [r for r in deduped if r["pdno"] == "SOXX"]
        assert len(soxx_rows) == 1, f"Expected 1 SOXX row, got {len(soxx_rows)}"
        # Qty should not be doubled
        assert int(soxx_rows[0]["ovrs_cblc_qty"]) == 4

    def test_cross_exchange_preserved(self):
        """Same symbol on different exchanges → cross-exchange merge (qty 합산, 1 row).

        NASD qty=2 + NYSE qty=3 → 1 row with total qty=5.
        (설계: cross-exchange는 동일 종목으로 간주하여 합산)
        """
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)

        row_nasd = _make_row("LITE", "NASD", 2)
        row_nyse = _make_row("LITE", "NYSE", 3)
        raw_rows = [row_nasd, row_nyse]

        deduped = client._merge_duplicate_symbols(raw_rows)

        lite_rows = [r for r in deduped if r["pdno"] == "LITE"]
        assert len(lite_rows) == 1, "Cross-exchange should be merged into 1 row"
        # qty가 합산되어야 함 (2 + 3 = 5)
        merged_qty = int(lite_rows[0].get("ovrs_cblc_qty", 0))
        assert merged_qty == 5, f"Expected merged qty=5, got {merged_qty}"

    def test_no_false_duplication_with_distinct_symbols(self):
        """Distinct symbols must all be preserved."""
        from trader.us.execution.kis_us_client import KisUSClient

        client = KisUSClient.__new__(KisUSClient)

        rows = [
            _make_row("CRDO", "NASD", 12),
            _make_row("LITE", "NASD", 2),
            _make_row("SOXX", "AMEX", 4),
            _make_row("VRT", "NASD", 6),
        ]
        deduped = client._merge_duplicate_symbols(rows)

        symbols = {r["pdno"] for r in deduped}
        assert symbols == {"CRDO", "LITE", "SOXX", "VRT"}
        assert len(deduped) == 4
