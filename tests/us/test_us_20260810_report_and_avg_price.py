from datetime import datetime, timezone

from trader.us.profit_capture import authoritative_broker_avg
from trader.us.runner import daily_report_runner as report_runner


def test_authoritative_fill_rows_are_deduplicated_and_notional_is_daily(monkeypatch):
    rows = []
    for side, count in (("BUY", 2), ("SELL", 10)):
        for index in range(count):
            row = {"side": side, "symbol": f"{side}{index}", "order_no": f"{side}{index}",
                   "client_order_key": f"K-{side}-{index}", "fill_idempotency_key": f"F-{side}-{index}",
                   "qty": 1, "price_usd": 10, "evidence_type": "KIS_EXECUTION_ACTUAL",
                   "is_synthetic": False, "accounting_active": True, "fill_source": "kis"}
            rows.extend((row, dict(row)))  # repeated reconciliation/API poll
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none", lambda: object())
    monkeypatch.setattr(report_runner, "_read_autocommit", lambda *args, **kwargs: rows)
    result = report_runner.load_us_fills_breakdown("2026-08-10")
    assert result["real_broker_buys"] == 2
    assert result["real_broker_sells"] == 10
    assert result["fills_count"] == 12
    assert result["real_broker_buy_notional"] == 20
    assert result["real_broker_sell_notional"] == 100


def test_kis_balance_raw_average_is_resolved_at_profit_capture_boundary():
    now = datetime.now(timezone.utc)
    value, meta = authoritative_broker_avg({
        "symbol": "MRK", "qty": 2, "orderable_qty": 2,
        "avg_price_usd": "81.25", "broker_avg_price_source": "",
        "broker_avg_price_currency": "USD", "broker_avg_price_asof": now.isoformat(),
        "balance_source": "kis_balance_authoritative", "authoritative_positions": True,
        "position_lifecycle_id": "MRK-life",
    }, now=now)
    assert str(value) == "81.25"
    assert meta["broker_avg_price_source"] == "kis_pchs_avg_pric"


def test_broker_average_without_asof_uses_position_fallback():
    position = {
        "symbol": "HELD", "qty": 1, "orderable_qty": 1,
        "position_lifecycle_id": "position-1",
        "broker_avg_price": "100", "broker_avg_price_currency": "USD",
        "avg_price_usd": "101",
    }
    value, meta = authoritative_broker_avg(position)
    assert str(value) == "101"
    assert meta["broker_avg_price_source"] == "fallback_avg_price_usd"
