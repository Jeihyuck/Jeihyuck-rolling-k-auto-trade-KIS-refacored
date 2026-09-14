from trader.us.db import repos


def _offline(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_PROFIT_CAPTURE_STATE.clear()
    repos._MEM_POSITIONS.clear()
    repos._MEM_INTENTS.clear()


def test_tp_completion_carries_across_trade_dates_for_same_lifecycle(monkeypatch):
    _offline(monkeypatch)
    repos.mark_us_profit_capture_stage(
        "2026-09-14", "JPM", "tp1", position_lifecycle_id="life-1",
        order_key="tp1-order", qty=2, status="FILLED",
    )
    next_day = repos.load_us_profit_capture_state(
        "2026-09-15", ["JPM"], {"JPM": "life-1"},
    )["JPM"]
    assert next_day["tp1_done"] is True
    assert next_day["tp1_pending"] is False
    new_lifecycle = repos.load_us_profit_capture_state(
        "2026-09-15", ["JPM"], {"JPM": "life-2"},
    )["JPM"]
    assert new_lifecycle["tp1_done"] is False


def test_position_snapshot_preserves_entry_policy_across_reconcile(monkeypatch):
    _offline(monkeypatch)
    first = {"symbol": "AMD", "exchange": "NASDAQ", "qty": 5, "avg_price_usd": 100,
             "meta": {"book": "CORE_BOOK", "horizon": "CORE_CARRY", "exit_policy": "US_CORE_TREND"}}
    repos.save_position_snapshot([first], trade_date="2026-09-14")
    raw_next_day = {"symbol": "AMD", "exchange": "NASDAQ", "qty": 5, "avg_price_usd": 100,
                    "current_price_usd": 110, "meta": {"balance_source": "kis_balance_authoritative"}}
    repos.save_position_snapshot([raw_next_day], trade_date="2026-09-15")
    assert raw_next_day["book"] == "CORE_BOOK"
    assert raw_next_day["horizon"] == "CORE_CARRY"
    assert raw_next_day["exit_policy"] == "US_CORE_TREND"
    assert raw_next_day["meta"]["entry_policy_contract_sha256"]


def test_us_buy_intent_persists_policy_contract(monkeypatch):
    _offline(monkeypatch)
    intent = {
        "trade_date": "2026-09-14", "client_order_key": "US-20260914-AMD-BUY-1",
        "symbol": "AMD", "exchange": "NASDAQ", "side": "BUY", "qty": 1,
        "limit_price": 100.0, "notional_usd": 100.0, "strategy": "us_pb1", "meta": {},
    }
    assert repos.save_order_intent(intent, trade_date="2026-09-14") is True
    meta = repos._MEM_INTENTS[0]["meta"]
    assert meta["book"] == "SWING_BOOK"
    assert meta["horizon"] == "SWING_CARRY"
    assert meta["exit_policy"] == "US_SWING_DEFAULT"
    assert meta["entry_policy_contract_version"] == "us_entry_policy_contract_v1"
