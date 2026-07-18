"""Tests: KIS balance is treated as authoritative source in reconcile.

- KIS balance OK → save_position_snapshot called with correct data
- Result includes balance_source='kis_balance_authoritative'
- Position qty matches KIS balance (no doubling)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


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
            result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

            assert result["status"] == "OK"
            assert mock_save.called, "save_position_snapshot should have been called"
            saved_positions = mock_save.call_args[0][0]
            assert isinstance(saved_positions, list)
            symbols = {p["symbol"] for p in saved_positions}
            assert "CRDO" in symbols
            assert "LITE" in symbols
            kwargs = mock_save.call_args.kwargs
            assert kwargs["trade_date"] == "2026-07-17"
            assert kwargs["authoritative_positions"] is True
            assert kwargs["preserve_previous_positions"] is False

    def test_balance_source_authoritative(self):
        from trader.us.execution.reconcile import reconcile_positions

        positions = [_make_position("VRT", 6)]
        balance_resp = _make_provider_balance(positions)

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        with patch("trader.us.db.repos.save_position_snapshot", return_value=1):
            result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

        assert result.get("balance_source") == "kis_balance_authoritative"

    def test_qty_not_doubled(self):
        from trader.us.execution.reconcile import reconcile_positions

        positions = [_make_position("CRDO", 12, orderable=12)]
        balance_resp = _make_provider_balance(positions)

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        saved_snapshots: list = []

        def capture_save(snaps, **kwargs):
            saved_snapshots.extend(snaps)
            assert kwargs["trade_date"] == "2026-07-17"
            return len(snaps)

        with patch("trader.us.db.repos.save_position_snapshot", side_effect=capture_save):
            result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

        assert result["status"] == "OK"
        crdo_saved = next((s for s in saved_snapshots if s.get("symbol") == "CRDO"), None)
        assert crdo_saved is not None, "CRDO position should have been saved"
        saved_qty = crdo_saved.get("qty") or crdo_saved.get("holding_qty")
        assert int(saved_qty) == 12, f"Expected qty=12, got {saved_qty}"

    def test_empty_positions_skips_upsert(self):
        from trader.us.execution.reconcile import reconcile_positions

        balance_resp = _make_provider_balance([])

        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = balance_resp

        with patch("trader.us.db.repos.save_position_snapshot") as mock_save:
            result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

            assert result["status"] == "OK"
            mock_save.assert_not_called()

    def test_error_balance_returns_error_status(self):
        from trader.us.execution.reconcile import reconcile_positions

        mock_provider = MagicMock()
        mock_provider.get_balance.side_effect = RuntimeError("KIS API timeout")

        result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

        assert result["status"] == "TEMP_ERROR"
        assert result.get("balance_fetch_status") == "FAILED"
        assert result.get("authoritative_positions") is False
        assert result.get("preserve_previous_positions") is True
        assert result.get("block_new_entry") is True

    def test_authoritative_position_persist_failure_fails_closed(self):
        from trader.us.execution.reconcile import reconcile_positions

        positions = [_make_position("AMD", 3)]
        mock_provider = MagicMock()
        mock_provider.get_balance.return_value = _make_provider_balance(positions)

        with patch("trader.us.db.repos.save_position_snapshot", side_effect=RuntimeError("db down")):
            result = reconcile_positions(provider=mock_provider, trade_date="2026-07-17")

        assert result["status"] == "POSITION_PERSIST_ERROR"
        assert result["block_new_entry"] is True
        assert result["preserve_previous_positions"] is True
