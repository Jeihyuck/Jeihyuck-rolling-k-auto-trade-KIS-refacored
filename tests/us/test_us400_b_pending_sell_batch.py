"""US400-B: actual pending-order query parity and one batch read."""
import os
import pytest
from tests.us.test_us_postgres_integration_real import pg_engine


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="requires disposable PostgreSQL")
def test_pending_sell_batch_matches_single_symbol_queries(pg_engine, monkeypatch):
    from sqlalchemy import text
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *a, **kw: "epoch")
    rows = [
        ("AAA", "SELL", "ACK", "epoch", False, "2026-11-27"),
        ("BBB", "SELL", "FILLED", "epoch", False, "2026-11-27"),
        ("CCC", "BUY", "ACK", "epoch", False, "2026-11-27"),
        ("DDD", "SELL", "ACK", "other-epoch", False, "2026-11-27"),
        ("EEE", "SELL", "ACK", "epoch", True, "2026-11-27"),
        ("FFF", "SELL", "ACK", "epoch", False, "2026-11-26"),
        ("GGG", "SELL", "PARTIALLY_FILLED", "epoch", False, "2026-11-27"),
    ]
    with pg_engine.begin() as conn:
        for idx, (symbol, side, status, epoch, dry, td) in enumerate(rows):
            conn.execute(text("""INSERT INTO us_orders
                (trade_date,client_order_key,symbol,exchange,side,qty_requested,qty_filled,status,dry_run,trading_epoch_id,meta)
                VALUES (:td,:key,:symbol,'NASDAQ',:side,2,0,:status,:dry,:epoch,'{}'::jsonb)
            """), dict(td=td,key=f"batch-{idx}",symbol=symbol,side=side,status=status,dry=dry,epoch=epoch))
    symbols = [r[0] for r in rows]
    statuses = {"ACK", "PARTIALLY_FILLED"}
    expected = {sym for sym in symbols if repos.has_pending_order_for_symbol_side(
        sym,"SELL","2026-11-27",include_statuses=statuses)}
    assert expected == {"AAA","GGG"}
    assert repos.load_pending_order_symbols_for_side(symbols=symbols,side="SELL",
        trade_date="2026-11-27",include_statuses=statuses) == expected


@pytest.mark.parametrize("strict,fail", [(False,False),(True,False),(False,True),(True,True)])
def test_native_sell_batch_uses_one_read_and_retains_fences(monkeypatch, strict, fail):
    from trader.us.db import repos
    from trader.us.runner.trade_tick_runner import _suppress_pending_sell_exit_intents
    for key,value in {"KIS_ENV":"practice","WSL_RUN_MARKET":"US","US_ORDER_ARMED":"1",
                      "US_PENDING_SELL_BATCH_ENABLED":"1"}.items():
        monkeypatch.setenv(key,value)
    calls=[]
    def read(**kw):
        calls.append(kw)
        if fail: raise OSError("unavailable")
        return {"AAA"}
    monkeypatch.setattr(repos,"load_pending_order_symbols_for_side",read)
    monkeypatch.setattr(repos,"has_pending_order_for_symbol_side",
                        lambda **kw: pytest.fail("per-symbol DB read"))
    intents=[{"symbol":sym,"side":"SELL","reason":reason} for sym,reason in
             [("AAA","TP1"),("AAA","stop_loss"),("BBB","stop_loss")]]
    result=_suppress_pending_sell_exit_intents(intents,"2026-11-27",strict=strict)
    assert len(calls)==1 and calls[0]["symbols"]==["AAA","BBB"]
    assert result == ([] if fail else [intents[2]])


def test_native_batch_requires_durable_database(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:None)
    with pytest.raises(RuntimeError,match="db_unavailable"):
        repos.load_pending_order_symbols_for_side(symbols=["AAA"],side="SELL",
            trade_date="2026-11-27",include_statuses={"ACK"})
