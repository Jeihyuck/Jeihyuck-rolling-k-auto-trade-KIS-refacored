from __future__ import annotations


def test_save_fills_skips_duplicate_rows_in_memory(monkeypatch) -> None:
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_FILLS.clear()
    fills = [{
        "symbol": "CIEN",
        "exchange": "NYSE",
        "side": "SELL",
        "qty": 4,
        "price_usd": 500.6966,
        "order_no": "ORD1",
        "client_order_key": "CK1",
    }]

    inserted_first = repos.save_fills(fills, trade_date="2026-06-05")
    inserted_second = repos.save_fills(fills, trade_date="2026-06-05")

    assert inserted_first == 1
    assert inserted_second == 0
    assert len(repos._MEM_FILLS) == 1


def test_migration_recovers_unique_violation_for_0043(monkeypatch) -> None:
    import trader.db.migrate as mod

    calls: list[str] = []

    class DummyNested:
        def __enter__(self):
            calls.append("begin_nested")
            return self

        def __exit__(self, exc_type, exc, tb):
            calls.append("end_nested")
            return False

    class DummyConn:
        def begin_nested(self):
            return DummyNested()

    monkeypatch.setattr(mod, "_is_unique_violation", lambda exc: True)
    monkeypatch.setattr(mod, "_is_us_fills_index_create_failure", lambda message: True)
    monkeypatch.setattr(mod, "_statement_mentions_index", lambda statement, index: index in statement)
    monkeypatch.setattr(mod, "_dedup_us_fills_for_idempotent_index", lambda conn, log_keys=False: calls.append(f"dedup:{int(log_keys)}"))
    monkeypatch.setattr(mod, "_apply_pg_statement", lambda conn, statement: calls.append(f"apply:{statement}"))

    recovered = mod._try_recover_us_fills_unique_violation(
        DummyConn(),
        version="0043_us_fills_idempotency_and_order_reconcile_fix.sql",
        statement="CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_idempotent ON us_fills (trade_date)",
        exc=RuntimeError("duplicate key value violates unique constraint uq_us_fills_idempotent"),
    )

    assert recovered is True
    assert "dedup:1" in calls
    assert any(call.startswith("apply:CREATE UNIQUE INDEX") for call in calls)
