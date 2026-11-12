"""Real code-path contract evidence fixtures; ZERO KIS HTTP or DB writes."""
from datetime import date
import hashlib

from trader.us.entry_exit_contract import build_us_entry_exit_contract
from scripts.us_all_symbols_lifecycle_readonly_audit_20261112 import audit_all_positions

ACCOUNT = "fixture:practice-readonly"
HASH = hashlib.sha256(ACCOUNT.encode()).hexdigest()
EPOCH = "fixture-us-active"
PORTFOLIO = "fixture-portfolio"
CYCLE = "fixture-cycle"


class FakeSQLResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class FakeReadOnlyConn:
    def __init__(self, *, missing_contract=False, unresolved_sell=False):
        self.missing_contract = missing_contract
        self.unresolved_sell = unresolved_sell
        self.calls = []
        self.contract = build_us_entry_exit_contract({
            "strategy_owner": "US_STANDARD", "symbol": "AMGN",
            "position_action": "NEW_POSITION_BUY",
            "entry_style_selected": "ENTRY_PULLBACK",
            "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        })

    def execute(self, sql, params=None):
        statement = str(sql)
        self.calls.append((statement, dict(params or {})))
        if "FROM trading_epochs" in statement:
            return FakeSQLResult([{"trading_epoch_id": EPOCH, "account_id": ACCOUNT}])
        if "WITH latest AS" in statement:
            meta = {
                "strategy_owner": "US_STANDARD",
                "position_cycle_id": CYCLE,
                "portfolio_epoch_id": PORTFOLIO,
                "broker_account_hash": HASH,
                "entry_style_selected": "ENTRY_PULLBACK",
                "entry_exit_contract": self.contract,
                "entry_exit_contract_sha256": self.contract["sha256"],
            }
            if self.missing_contract:
                meta["entry_exit_contract_sha256"] = "wrong-hash"
            return FakeSQLResult([{
                "symbol": "AMGN", "qty": 6, "as_of": date(2026, 11, 11),
                "account_id": None, "portfolio_epoch_id": PORTFOLIO,
                "position_cycle_id": CYCLE, "meta": meta,
            }])
        if "FROM us_position_risk_state" in statement:
            return FakeSQLResult([{"lifecycle": {
                "is_open": True, "lifecycle_id": CYCLE,
                "position_cycle_id": CYCLE, "portfolio_epoch_id": PORTFOLIO,
                "trading_epoch_id": EPOCH, "account_id": ACCOUNT,
                "entry_policy": {"entry_exit_contract_sha256": self.contract["sha256"]},
            }}])
        if "FROM us_orders o" in statement:
            meta = {
                "strategy_owner": "US_STANDARD",
                "entry_exit_contract_sha256": self.contract["sha256"],
            }
            return FakeSQLResult([{
                "trade_date": date(2026, 11, 10),
                "account_id": HASH, "portfolio_epoch_id": PORTFOLIO,
                "position_cycle_id": CYCLE,
                "intent_account_id": HASH, "intent_portfolio_epoch_id": PORTFOLIO,
                "intent_position_cycle_id": CYCLE,
                "fill_account_id": HASH, "fill_portfolio_epoch_id": PORTFOLIO,
                "fill_position_cycle_id": CYCLE,
                "order_meta": meta,
                "intent_meta": {
                    **meta, "entry_exit_contract": self.contract,
                },
                "fill_meta": {
                    **meta, "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                    "cumulative_filled_qty": 6,
                    "is_synthetic": False,
                },
                "filled_shares": 6, "fill_order": "fixture-ord-1",
                "broker_order": "fixture-ord-1", "status": "FILLED",
                "qty_requested": 6, "qty_filled": 6, "intended_shares": 6,
            }])
        if "FROM us_orders" in statement:
            return FakeSQLResult([{
                "trading_epoch_id": EPOCH, "side": "SELL", "status": "OPEN",
            }] if self.unresolved_sell else [])
        raise AssertionError("Unexpected SQL / write: " + statement[:120])


def test_audit_original_buy_db_candidate_is_not_automatic_sell_authority():
    conn = FakeReadOnlyConn()
    report = audit_all_positions(conn, as_of="2026-11-11")
    assert report["positions_audited"] == 1
    assert report["db_rows_modified"] == 0
    assert report["orders_submitted"] == 0
    row = report["positions"][0]
    assert row["symbol"] == "AMGN"
    assert row["original_db_buy_candidates"] == 1
    assert row["frozen_contract_valid"] is True
    assert row["classification"] == "UNVERIFIED"
    assert row["reason"] == "ORIGINAL_DB_SOURCE_CANDIDATE_NEEDS_FRESH_KIS_PROOF"
    assert row["automated_selling_authorized_by_this_audit"] is False
    assert all("INSERT" not in q and "UPDATE" not in q and "DELETE" not in q
               for q, _ in conn.calls)


def test_audit_rejects_changed_contract_and_open_sell():
    changed = audit_all_positions(FakeReadOnlyConn(missing_contract=True),
                                  as_of="2026-11-11")
    assert changed["positions"][0]["classification"] == "UNVERIFIED"
    assert changed["positions"][0]["frozen_contract_valid"] is False
    pending = audit_all_positions(FakeReadOnlyConn(unresolved_sell=True),
                                  as_of="2026-11-11")
    assert pending["positions"][0]["classification"] == "MANUAL_ACTION"
    assert pending["positions"][0]["open_sell_current_epoch"] == 1


def test_readonly_cli_refuses_unarmed_order_capable_environment(monkeypatch, capsys):
    import sys
    from scripts.us_all_symbols_lifecycle_readonly_audit_20261112 import main
    monkeypatch.setattr(sys, "argv", ["audit", "--as-of", "2026-11-11",
                                      "--confirm-practice-readonly"])
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("NO_TRADE", "0")
    assert main() == 42
    assert "PRACTICE_US_READONLY_REQUIRED" in capsys.readouterr().out
