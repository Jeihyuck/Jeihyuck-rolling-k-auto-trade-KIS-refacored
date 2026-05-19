"""Tests: KIS balance is treated as authoritative source in reconcile.

- KIS balance OK → save_position_snapshot called with correct data
- Result includes balance_source='kis_balance_authoritative'
- Position qty matches KIS balance (no doubling)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest


def _make_provider_balance(holdings: list[dict], raw_count: int = 0) -> dict:
    """USDataProvider.get_balance() 반환값 모킹."""
    symbols = [h["symbol"] for h in holdings if h.get("symbol")]
    return {
        "status": "OK",
        "positions": holdings,
        "position_symbols": symbols,
        "total_pvs": "100000.0",
        "total_pvs_source": "kis_balance",
        "raw_output1_count": raw_count or len(holdings),
        "normalized_position_count": len(holdings),
        "balance_parse_status": "OK",
        "balance_parse_error": None,
    }


def _make_position(symbol: str, qty: int, orderable: int | None = None) -> dict:
    return {
        "symbol": symbol,
        "exchange": "NASD",
        "qty": qty,
        "holding_qty": qty,
        "orderable_qty": orderable if orderable is not None else qty,
        "sellable_qty": orderable if orderable is not None else qty,
        "avg_price_usd": 100.0,
        "current_price_usd": 105.0,
        "balance_source": "kis_balance_authoritative",
    }


class TestReconcileKisAuthoritative:
    def test_kis_balance_triggers_upsert(self):
        """KIS balance 성공 시 save_position_snapshot이 호출되어야 한다."""
        from trader.us.execution.reconcile import reconcile_positions

        positions = [
            _make_position("CRDO", 12),
            _make_position("LITE", 2),
        ]
        balance_resp = _make_provider_balance(positions)

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        with patch("trader.us.db.repos.save_position_snapshot") as mock_save:
            mock_save.return_value = len(positions)
            result = reconcile_positions(provider=mock_provider)

            assert mock_save.called, "save_position_snapshot should have been called"
            saved_positions = mock_save.call_args[0][0]
            assert isinstance(saved_positions, list)
            symbols = {p["symbol"] for p in saved_positions}
            assert "CRDO" in symbols
            assert "LITE" in symbols

    def test_balance_source_authoritative(self):
        """결과에 balance_source='kis_balance_authoritative'가 포함되어야 한다."""
        from trader.us.execution.reconcile import reconcile_positions

        positions = [_make_position("VRT", 6)]
        balance_resp = _make_provider_balance(positions)

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        with patch("trader.us.db.repos.save_position_snapshot", return_value=1):
            result = reconcile_positions(provider=mock_provider)

        assert result.get("balance_source") == "kis_balance_authoritative"

    def test_qty_not_doubled(self):
        """KIS balance에서 qty=12인 경우 저장된 qty도 12여야 한다 (2배 방지)."""
        from trader.us.execution.reconcile import reconcile_positions

        positions = [_make_position("CRDO", 12, orderable=12)]
        balance_resp = _make_provider_balance(positions)

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        saved_snapshots: list = []

        def capture_save(snaps):
            saved_snapshots.extend(snaps)
            return len(snaps)

        with patch("trader.us.db.repos.save_position_snapshot", side_effect=capture_save):
            reconcile_positions(provider=mock_provider)

        crdo_saved = next((s for s in saved_snapshots if s.get("symbol") == "CRDO"), None)
        assert crdo_saved is not None, "CRDO position should have been saved"
        saved_qty = crdo_saved.get("qty") or crdo_saved.get("holding_qty")
        assert int(saved_qty) == 12, f"Expected qty=12, got {saved_qty}"

    def test_empty_positions_skips_upsert(self):
        """positions=[] → save_position_snapshot 호출 없음."""
        from trader.us.execution.reconcile import reconcile_positions

        balance_resp = _make_provider_balance([])

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        with patch("trader.us.db.repos.save_position_snapshot") as mock_save:
            result = reconcile_positions(provider=mock_provider)

            mock_save.assert_not_called()

    def test_error_balance_returns_error_status(self):
        """balance 조회 실패 시 status=ERROR 반환."""
        from trader.us.execution.reconcile import reconcile_positions

        mock_provider = MagicMock()
        mock_provider.get_balance.side_effect = RuntimeError("KIS API timeout")

        result = reconcile_positions(provider=mock_provider)

        assert result["status"] == "ERROR"
        assert result.get("block_new_entry") is True

