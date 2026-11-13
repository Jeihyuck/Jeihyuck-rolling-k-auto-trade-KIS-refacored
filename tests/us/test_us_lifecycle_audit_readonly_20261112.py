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
                "pre_order_holding_qty": 0,
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


def test_audit_does_not_misclassify_same_contract_add_as_second_original():
    class WithSameContractAdd(FakeReadOnlyConn):
        def execute(self, sql, params=None):
            result = super().execute(sql, params)
            if "FROM us_orders o" not in str(sql):
                return result
            original = result.rows[0]
            add = {
                **original,
                "order_meta": {**original["order_meta"], "pre_order_holding_qty": 6},
                "fill_order": "fixture-add-ord-2",
                "broker_order": "fixture-add-ord-2",
                "filled_shares": 2, "qty_filled": 2,
                "qty_requested": 2, "intended_shares": 2,
                "fill_meta": {**original["fill_meta"], "cumulative_filled_qty": 2},
            }
            return FakeSQLResult([original, add])
    audit = audit_all_positions(WithSameContractAdd(), as_of="2026-11-11")
    assert audit["positions"][0]["original_db_buy_candidates"] == 1


def test_am_close_hooks_do_not_run_in_trade_ticks_or_gate_sells():
    from pathlib import Path
    helper = Path("scripts/wsl/us-lifecycle-readonly-audit.sh").read_text()
    am = Path("scripts/wsl/run-us-am.sh").read_text()
    close = Path("scripts/wsl/run-us-close.sh").read_text()
    tick = Path("trader/us/runner/trade_tick_runner.py").read_text()
    assert 'nullim_us_lifecycle_readonly_audit "AM_START"' in am
    assert 'nullim_us_lifecycle_readonly_audit "CLOSE"' in close
    assert "nullim_us_lifecycle_readonly_audit" not in tick
    assert "US_ORDER_ARMED=0" in helper
    assert "NO_TRADE=1 DRY_RUN=1" in helper
    assert "return 0" in helper


def test_am_close_readonly_audit_has_bounded_runtime_and_no_trade_permission():
    from pathlib import Path
    helper = Path("scripts/wsl/us-lifecycle-readonly-audit.sh").read_text()
    assert "US_LIFECYCLE_AUDIT_TIMEOUT_SECONDS:-15" in helper
    assert "timeout --kill-after=2s" in helper
    assert "NO_TRADE=1 DRY_RUN=1" in helper
    assert "audit_rc=$?" in helper
    assert "no_order_block=1" in helper
    assert "return 0" in helper
