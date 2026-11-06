"""Original US lineage audit is SELECT-only, account-scoped and never repairs."""
from __future__ import annotations

from contextlib import contextmanager

import pytest

from scripts.us_original_position_lineage_readonly_audit import (
    SYMBOLS, READONLY_SQL, audit_readonly, classify_readonly_candidate,
)

BASE = {
    "symbol": "TMO", "qty": 5, "as_of": "2026-11-05",
    "account_matches": True, "account_absent": False, "epoch_matches": True,
    "portfolio_present": True, "cycle_present": True,
    "owner_present": True, "scoped_buy_orders": 2,
    "scoped_buy_fills": 2, "scoped_legacy_lifecycle_rows": 12,
    "legacy_epoch_net_fills": 5,
}


@pytest.mark.parametrize("changes,expected", [
    ({"account_matches": False}, "ORIGINAL_ACCOUNT_SCOPE_MISMATCH_OR_UNKNOWN"),
    ({"account_matches": None, "account_absent": True}, "LEGACY_ORIGINAL_ACCOUNT_SCOPE_ABSENT"),
    ({"epoch_matches": False}, "TRADING_EPOCH_MISMATCH"),
    ({"portfolio_present": False}, "ORIGINAL_PORTFOLIO_CYCLE_UNVERIFIED"),
    ({"cycle_present": False}, "ORIGINAL_PORTFOLIO_CYCLE_UNVERIFIED"),
    ({"owner_present": False}, "STRATEGY_OWNER_UNVERIFIED"),
    ({"scoped_buy_orders": 0}, "ORIGINAL_BUY_ATTEMPT_OR_FILL_UNVERIFIED"),
    ({"scoped_buy_fills": 0}, "ORIGINAL_BUY_ATTEMPT_OR_FILL_UNVERIFIED"),
    ({}, "CANDIDATE_NEEDS_KIS_AND_FROZEN_CONTRACT_PROOF"),
])
def test_readonly_lineage_never_infers_unverified_identity(changes, expected):
    assessed = classify_readonly_candidate({**BASE, **changes})
    assert assessed["classification"] == expected
    assert assessed["auto_identity_rebind_allowed"] is False
    assert assessed["claims_released"] == assessed["orders_submitted"] == 0
    assert assessed["db_rows_modified"] == 0
    assert assessed["legacy_unscoped_buy_orders"] >= 0
    assert assessed["legacy_unscoped_buy_fills"] >= 0


def test_disallow_unlisted_symbol():
    with pytest.raises(ValueError, match="ALLOWLIST"):
        classify_readonly_candidate({**BASE, "symbol": "TQQQ"})


def test_readonly_query_is_account_and_epoch_scoped():
    assert "o.account_id=:account" in READONLY_SQL
    assert "o.trading_epoch_id=:epoch" in READONLY_SQL
    assert "f.account_id=:account" in READONLY_SQL
    assert "r.trading_epoch_id=:epoch" in READONLY_SQL
    assert "o.account_id IS NULL" in READONLY_SQL
    assert "f.account_id IS NULL" in READONLY_SQL
    assert "UPDATE " not in READONLY_SQL.upper()
    assert "DELETE " not in READONLY_SQL.upper()


def test_db_first_statement_is_transaction_read_only():
    statements = []

    class Mapped:
        def all(self):
            return [dict(BASE)]

    class Rows:
        def mappings(self):
            return Mapped()

    class Conn:
        def execute(self, query, params=None):
            statements.append((query.text, params))
            return Rows()

    class Engine:
        @contextmanager
        def connect(self):
            yield Conn()

    out = audit_readonly(
        Engine(), account_fingerprint="a" * 64,
        trading_epoch_id="epoch-test", trade_date="2026-11-05",
    )
    assert statements[0][0] == "SET TRANSACTION READ ONLY"
    assert statements[1][0] == READONLY_SQL
    assert statements[1][1]["account"] == "a" * 64
    assert statements[1][1]["epoch"] == "epoch-test"
    assert out["status"] == "ORIGINAL_LINEAGE_UNVERIFIED"
    assert out["positions_audited"] == len(SYMBOLS)
    assert out["orders_submitted"] == out["claims_released"] == 0
    assert out["db_rows_modified"] == 0
    assert out["kis_original_order_verified"] is False


def test_current_epoch_db_net_matches_but_never_proves_original_account():
    row = classify_readonly_candidate({
        **BASE, "account_absent": True, "account_matches": None,
        "legacy_epoch_net_fills": 5,
    })
    assert row["classification"] == "LEGACY_ORIGINAL_ACCOUNT_SCOPE_ABSENT"
    assert row["legacy_epoch_db_net_matches"] is True
    assert row["auto_identity_rebind_allowed"] is False
    assert row["evidence_status"] == "READONLY_DB_CANDIDATE_NOT_BROKER_PROOF"
    altered = classify_readonly_candidate({
        **BASE, "account_absent": True, "account_matches": None,
        "legacy_epoch_net_fills": 4,
    })
    assert altered["legacy_epoch_db_net_matches"] is False
    assert altered["auto_identity_rebind_allowed"] is False
