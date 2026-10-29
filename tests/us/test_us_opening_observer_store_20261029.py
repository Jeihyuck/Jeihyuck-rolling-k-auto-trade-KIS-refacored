"""Durable verified KIS 1m evidence cannot be forged by sparse WS samples."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from trader.us.entry_timing.opening_observer import (
    load_bars, load_latest_ws_quote, open_store, persist_bars,
    persist_ws_sample, store_path,
)

NY = ZoneInfo("America/New_York")


def sample_rows():
    start = datetime(2026, 10, 28, 9, 30, tzinfo=NY)
    rows = []
    for i in range(31):
        at = start + timedelta(minutes=i)
        rows.append({
            "xymd": "20261028", "xhms": at.strftime("%H%M%S"),
            "open": "100", "high": "101", "low": "99", "last": "100.5",
            "evol": "500", "eamt": "50250",
        })
    return rows


def test_wal_store_is_account_epoch_day_scoped(tmp_path):
    first = store_path(env="practice", account_scope="A:epoch-1",
                       trade_date="2026-10-28", base=tmp_path)
    second = store_path(env="practice", account_scope="A:epoch-2",
                        trade_date="2026-10-28", base=tmp_path)
    third = store_path(env="practice", account_scope="A:epoch-1",
                       trade_date="2026-10-29", base=tmp_path)
    assert len({first, second, third}) == 3
    with pytest.raises(ValueError):
        store_path(env="real", account_scope="A:epoch-1",
                   trade_date="2026-10-28", base=tmp_path)


def test_exact_kis_minute_rows_persist_dedupe_and_current_minute_excluded(tmp_path):
    path = store_path(env="practice", account_scope="scope:epoch1",
                      trade_date="2026-10-28", base=tmp_path)
    db = open_store(path)
    now = datetime(2026, 10, 28, 10, 0, 30, tzinfo=NY)
    count = persist_bars(db, trade_date="2026-10-28", symbol="ELV",
                         owner="US_STANDARD", native_rows=sample_rows(),
                         now_et=now)
    assert count == 30
    assert persist_bars(db, trade_date="2026-10-28", symbol="ELV",
                        owner="US_STANDARD", native_rows=sample_rows(),
                        now_et=now) == 0
    bars = load_bars(db, trade_date="2026-10-28", symbol="ELV")
    assert len(bars) == 30
    assert bars[0]["time_et"][11:16] == "09:30"
    assert bars[-1]["time_et"][11:16] == "09:59"
    assert load_bars(db, trade_date="2026-10-28", symbol="TQQQ") == []
    assert load_bars(db, trade_date="2026-10-29", symbol="ELV") == []
    assert (path.stat().st_mode & 0o077) == 0
    db.close()


def test_ws_price_never_adds_a_fake_minute_bar(tmp_path, monkeypatch):
    db = open_store(store_path(env="practice", account_scope="A:epoch",
                               trade_date="2026-10-28", base=tmp_path))
    import time
    epoch = time.time()
    assert persist_ws_sample(
        db, trade_date="2026-10-28", symbol="ELV",
        quote={"received_at": epoch, "last": 99, "collector_generation": "g1"},
    )
    latest = load_latest_ws_quote(db, trade_date="2026-10-28",
                                  symbol="ELV", now_epoch=epoch + 1)
    assert latest["source"] == "KIS_WEBSOCKET_COLLECTOR"
    assert latest["last"] == 99
    assert load_bars(db, trade_date="2026-10-28", symbol="ELV") == []
    assert load_latest_ws_quote(db, trade_date="2026-10-28",
                                symbol="ELV", now_epoch=epoch + 20) == {}
    db.close()


def test_replay_preserves_only_real_broker_bid_ask_and_handles_legacy_schema(tmp_path):
    import sqlite3
    import time
    from trader.us.entry_timing.opening_observer import load_quote_samples
    path = store_path(env="practice", account_scope="acct:epoch", 
                      trade_date="2026-10-28", base=tmp_path)
    # Older pre-release observer schema must upgrade in-place without
    # rewriting existing quote history.
    old = sqlite3.connect(path)
    old.execute(
        "CREATE TABLE quote_samples (trade_date TEXT NOT NULL, symbol TEXT NOT NULL,"
        " owner TEXT NOT NULL, received_epoch REAL NOT NULL, last REAL NOT NULL,"
        " source TEXT NOT NULL, generation TEXT NOT NULL,"
        " PRIMARY KEY (trade_date,symbol,owner,received_epoch,source,generation))"
    )
    old.commit()
    old.close()
    db = open_store(path)
    epoch = time.time()
    assert persist_ws_sample(
        db, trade_date="2026-10-28", symbol="ELV",
        quote={"received_at": epoch, "last": 99.0, "bid": 98.99,
               "ask": 99.01, "collector_generation": "gen-1"},
    )
    historical = load_quote_samples(
        db, trade_date="2026-10-28", symbol="ELV"
    )
    assert len(historical) == 1
    assert historical[0]["bid"] == 98.99
    assert historical[0]["ask"] == 99.01
    current = load_latest_ws_quote(
        db, trade_date="2026-10-28", symbol="ELV", now_epoch=epoch + 1
    )
    assert current["bid"] == 98.99 and current["ask"] == 99.01
    # Invalid crossed/absent bid-ask is UNKNOWN, not a fabricated
    # executable entry price.
    assert persist_ws_sample(
        db, trade_date="2026-10-28", symbol="PG",
        quote={"received_at": epoch, "last": 95, "bid": 97,
               "ask": 96, "collector_generation": "gen-1"},
    )
    assert load_quote_samples(db, trade_date="2026-10-28", symbol="PG")[0]["bid"] is None
    assert load_quote_samples(db, trade_date="2026-10-28", symbol="TQQQ") == []
    db.close()


def test_broker_minute_endpoint_is_read_only_and_bounded(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient
    from trader.us.execution import kis_us_client as mod

    calls = {}
    client = object.__new__(KisUSClient)
    client._assert_not_offline = lambda name: None
    client._build_headers = lambda tr_id: {"tr_id": tr_id}
    client._resolve_quote_excd = lambda exchange: "NAS"
    def fake_get(path, *, headers, params):
        calls.update(path=path, headers=headers, params=params)
        return {"rt_cd": "0", "output2": sample_rows()}
    client._get = fake_get
    data = client.get_us_intraday_1m_bars("ELV", "NASDAQ", count=999)
    assert len(data) == 31
    assert calls["headers"]["tr_id"] == "HHDFS76950200"
    assert calls["params"]["NMIN"] == "1"
    assert calls["params"]["NREC"] == "120"
    assert calls["path"].endswith("inquire-time-itemchartprice")
    assert calls["params"]["NEXT"] == ""

def test_observer_preserves_all_standard_prep_exchange_aliases_without_cross_owner():
    from trader.us.entry_timing.opening_observer import standard_entry_pairs
    pairs = standard_entry_pairs([
        {"symbol": "ELV", "exchange": "NYS", "strategy_owner": "US_STANDARD"},
        {"symbol": "PG", "exchange": "NYSE", "strategy_owner": "US_STANDARD"},
        {"symbol": "MCK", "exchange": "NASD", "strategy_owner": "US_STANDARD"},
        {"symbol": "MDLZ", "exchange": "NAS", "strategy_owner": "US_STANDARD"},
        {"symbol": "TQQQ", "exchange": "NASDAQ", "strategy_owner": "TQQQ_INFINITE"},
        {"symbol": "XYZ", "exchange": "NAS", "strategy_owner": "OTHER_SLEEVE"},
        {"symbol": "ELV", "exchange": "NYSE"},
    ])
    assert pairs == [
        ("ELV", "NYSE"), ("PG", "NYSE"), ("MCK", "NASDAQ"),
        ("MDLZ", "NASDAQ"),
    ]
