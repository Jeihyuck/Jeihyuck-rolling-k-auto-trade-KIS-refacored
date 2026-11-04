"""PR309: exact KIS-filled US_STANDARD stop (without TP) can settle debt.

Read-only broker stubs. No live KIS, Supabase writes, order submissions or cancels.
"""
from __future__ import annotations

import hashlib
import pytest


ACCOUNT = "practice:123:01"
EPOCH = "epoch-syy"
LIFE = "life-syy"
KEY = "syy-stop-claim"
ORDER_NO = "0000038487"
DEBT = {
    "account_id": ACCOUNT, "trading_epoch_id": EPOCH,
    "strategy_owner": "US_STANDARD", "symbol": "SYY",
    "lifecycle_id": LIFE, "severity": 3, "target_qty": 41,
}


class Row:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    def mappings(self):
        return self

    def all(self):
        return self.value


class ReadOnlyConn:
    def __init__(self, *, with_original=True, race=False):
        self.with_original = with_original
        self.race = race
        self.active_checks = 0
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        return False

    def execute(self, stmt, params):
        sql = str(stmt)
        self.statements.append(sql)
        assert params["epoch"] == EPOCH
        assert params["life"] == LIFE
        assert params["account"] == hashlib.sha256(ACCOUNT.encode()).hexdigest()
        if "COUNT(*)" in sql and "exact_original_tp" not in sql:
            self.active_checks += 1
            return Row(1 if self.race and self.active_checks == 2 else 0)
        if "COUNT(*)" in sql:
            return Row(1 if self.with_original else 0)
        assert "f.meta->>'fill_evidence_type'" in sql
        return Row([{
            "trade_date": "2026-11-03", "symbol": "SYY",
            "order_no": ORDER_NO, "qty_requested": 41,
            "qty_filled": 41, "env": "practice",
            "trading_epoch_id": EPOCH,
        }])


class Engine:
    def __init__(self, conn):
        self.conn = conn

    def connect(self):
        return self.conn


class Broker:
    def __init__(self, *, status="FILLED", filled=41, remaining=0):
        self.status = status
        self.filled = filled
        self.remaining = remaining
        self.requests = []

    def get_fills_by_order_no(self, **kw):
        self.requests.append(kw)
        return {
            "symbol": "SYY", "side": "SELL",
            "order_no": ORDER_NO, "requested_qty": 41,
            "filled_qty": self.filled, "remaining_qty": self.remaining,
            "status": self.status, "filled_qty_present": True,
            "normalization_result": "normalized",
        }


def run_settlement(monkeypatch, broker, *, with_original=True, race=False,
                   balance_authoritative=True):
    from trader.us.execution import protective_stop_settlement_worker as settler
    from trader.us import protective_stop_obligation as stop
    from trader.us.db import repos
    import trader.us.execution.reconcile as recon
    import trader.account_state as account
    monkeypatch.setattr(settler, "_DEFERRED_SETTLEMENT_CURSOR", 0)
    monkeypatch.setattr(account, "get_account_key", lambda **kw: ACCOUNT)
    monkeypatch.setattr(account, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(stop, "load_pending_stop_obligations", lambda **kw: [DEBT])
    conn = ReadOnlyConn(with_original=with_original, race=race)
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine(conn))
    monkeypatch.setattr(recon, "reconcile_positions", lambda **kw: {
        "status": "OK" if balance_authoritative else "ERROR",
        "balance_fetch_status": "OK" if balance_authoritative else "ERROR",
        "authoritative_positions": balance_authoritative,
        "preserve_previous_positions": not balance_authoritative,
        "positions": [],
    })
    receipts = []
    def satisfy(*, debt, proof):
        from trader.us.protective_stop_obligation import validate_terminal_broker_proof
        assert validate_terminal_broker_proof(debt, proof) is True
        assert proof["terminal_orders"][0]["order_no"] == ORDER_NO
        assert proof["terminal_orders"][0]["filled_qty"] == 41
        receipts.append(proof)
        return True
    monkeypatch.setattr(stop, "satisfy_stop_obligation_from_broker_proof", satisfy)
    return settler.settle_verified_stop_debts_once(
        provider=broker, trade_date="2026-11-03", limit=1,
    ), receipts, conn


def test_syy_complete_hard_stop_settles_only_after_exact_kis_confirmation(monkeypatch):
    broker = Broker()
    result, receipts, conn = run_settlement(monkeypatch, broker)
    assert result == {"checked": 1, "settled": 1, "blocked": 0, "total_pending": 1}
    assert len(receipts) == 1
    assert broker.requests == [{
        "order_no": ORDER_NO, "symbol": "SYY", "trade_date": "2026-11-03",
    }]
    assert any("c.action=upper(o.meta->>'reason')" in s for s in conn.statements)
    assert any("c.action_state='SATISFIED'" in s for s in conn.statements)


@pytest.mark.parametrize(
    ("status", "filled", "remaining"),
    [("OPEN", 0, 41), ("PARTIALLY_FILLED", 40, 1),
     ("FILLED", 40, 1), ("CANCELLED", 0, 0)],
)
def test_syy_no_terminal_or_incomplete_kis_stays_pending(
    monkeypatch, status, filled, remaining,
):
    result, receipts, _ = run_settlement(
        monkeypatch, Broker(status=status, filled=filled, remaining=remaining),
    )
    assert result["settled"] == 0
    assert result["blocked"] == 1
    assert receipts == []


def test_missing_exact_original_fill_never_probes_balance(monkeypatch):
    from trader.us.execution import protective_stop_settlement_worker as settler
    import trader.us.execution.reconcile as recon
    monkeypatch.setattr(
        recon, "reconcile_positions",
        lambda **kw: pytest.fail("missing original stop proof must skip broker balance"),
    )
    result, receipts, _ = run_settlement(
        monkeypatch, Broker(), with_original=False,
    )
    assert result["settled"] == 0
    assert receipts == []


def test_syy_claim_acquired_during_balance_get_never_settles(monkeypatch):
    result, receipts, conn = run_settlement(
        monkeypatch, Broker(), race=True,
    )
    assert result["settled"] == 0
    assert result["blocked"] == 1
    assert conn.active_checks == 2
    assert receipts == []


def test_non_authoritative_balance_blocks_even_after_kis_order_proof(monkeypatch):
    with pytest.raises(RuntimeError, match="STAGE4_STOP_SETTLEMENT_BALANCE_UNVERIFIED"):
        run_settlement(
            monkeypatch, Broker(), balance_authoritative=False,
        )


def test_original_date_discovery_uses_exact_filled_stop_and_original_claim():
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    paths = [
        root / "trader/us/execution/protective_stop_settlement_worker.py",
        root / "trader/us/execution/stage4_historical_reader.py",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        for safety_contract in (
            "c.action_key=o.meta->>'execution_action_key'",
            "c.action=upper(o.meta->>'reason')",
            "c.action_state='SATISFIED'",
            "o.status='FILLED'",
            "f.meta->>'fill_evidence_type'",
            "f.meta->>'position_lifecycle_id'=:life",
            "f.meta->>'execution_action_key'=c.action_key",
            "f.trading_epoch_id=o.trading_epoch_id",
            "c.account_id=:account",
        ):
            assert safety_contract in source, (str(path), safety_contract)
