"""Legacy nine native KIS manual SELL preflight: no broker POST, fail closed."""
from contextlib import contextmanager

import pytest

from scripts.us_legacy_nine_sell_readiness import (
    SYMBOLS, _qty, _balance_symbols, evaluate_readiness, load_db_readonly,
)


def _positions():
    return [{"symbol": sym, "qty": 3, "trading_epoch_id": "epoch",
             "as_of": "2026-11-10"}
            for sym in SYMBOLS]


def _balance():
    return {
        "rt_cd": "0", "balance_complete": True,
        "balance_authoritative": True, "balance_conflicts": [],
        "failed_exchanges": {},
        "output1": [{"ovrs_pdno": sym, "ovrs_cblc_qty": "3",
                     "ord_psbl_qty": "3"}
                    for sym in SYMBOLS],
    }


def _check(**changes):
    args = {
        "latest_positions": _positions(),
        "unresolved_orders": [],
        "balance": _balance(),
        "original_day_orders": {"2026-11-11": []},
        "current_trade_date": "2026-11-11",
    }
    args.update(changes)
    return evaluate_readiness(**args)


def test_all_nine_manual_review_candidates_without_order_authority():
    out = _check()
    assert out["review_candidates"] == 9
    assert out["broker_orders_submitted"] == 0
    assert out["db_rows_modified"] == 0
    assert out["automated_sell_authorized"] is False
    assert all(x["broker_manual_screen_review"] == "CHECK_PRICE_AND_SUBMIT_IN_BROKER_UI"
               and x["automated_sell_authorized"] is False for x in out["positions"])


@pytest.mark.parametrize("qty", [True, False, -1, 1.0, 1.5, "1.0", None, "", " 3", "3 "])
def test_exact_broker_share_count_cannot_be_coerced(qty):
    assert _qty(qty) is None


@pytest.mark.parametrize("mutate,expected", [
    (lambda b: b.update(balance_complete=False), "FRESH_BROKER_BALANCE_INCOMPLETE"),
    (lambda b: b.update(balance_authoritative=False), "FRESH_BROKER_BALANCE_INCOMPLETE"),
    (lambda b: b.update(balance_conflicts=["KO"]), "FRESH_BROKER_BALANCE_INCOMPLETE"),
    (lambda b: b["output1"].append(dict(b["output1"][0])), "BROKER_DUPLICATE_SYMBOL"),
    (lambda b: b["output1"][0].update(ord_psbl_qty="4"), "BROKER_HOLDING_OR_ORDERABLE_UNVERIFIED"),
    (lambda b: b["output1"][0].pop("ord_psbl_qty"), "BROKER_HOLDING_OR_ORDERABLE_UNVERIFIED"),
])
def test_broker_balance_incomplete_duplicate_or_invalid_qty_fences_all(mutate, expected):
    balance = _balance()
    mutate(balance)
    out = _check(balance=balance)
    assert out["review_candidates"] == 0
    assert all(expected in x["blockers"] for x in out["positions"])


def test_mismatch_and_zero_orderable_never_recommend_sale():
    balance = _balance()
    balance["output1"][0]["ovrs_cblc_qty"] = "5"
    balance["output1"][1]["ord_psbl_qty"] = "0"
    out = _check(balance=balance)
    by = {x["symbol"]: x for x in out["positions"]}
    assert "KIS_DB_HOLDING_MISMATCH" in by["ABBV"]["blockers"]
    assert "KIS_ORDERABLE_ZERO" in by["DHR"]["blockers"]
    assert out["review_candidates"] == 7


def test_kis_current_sell_ack_blocks_that_symbol_even_if_db_has_no_order():
    rows = [{
        "odno": "0000099999", "pdno": "KO", "sll_buy_dvsn_cd": "01",
        "ord_qty": "2", "ft_ccld_qty": "0", "nccs_qty": "2",
    }]
    out = _check(original_day_orders={"2026-11-11": rows})
    by = {x["symbol"]: x for x in out["positions"]}
    assert "CURRENT_KIS_SELL_UNRESOLVED" in by["KO"]["blockers"]
    assert out["review_candidates"] == 8
    assert by["KO"]["broker_manual_screen_review"] == "STOP_AND_RECONCILE_FIRST"


def test_historical_unresolved_tmo_cannot_be_silently_treated_as_expired():
    unresolved = [{
        "symbol": "TMO", "trade_date": "2026-10-30",
        "order_no": "0000036019", "status": "OPEN",
        "qty_requested": 1, "qty_filled": 0,
    }]
    # Even missing KIS original-day data fails closed.
    result = _check(unresolved_orders=unresolved)
    tmo = next(x for x in result["positions"] if x["symbol"] == "TMO")
    assert "KIS_ORIGINAL_DAY_ORDERS_NOT_QUERIED" in tmo["blockers"]
    assert result["review_candidates"] == 8
    # KIS source-day GET alone does not release the stale DB SELL claim.
    result2 = _check(unresolved_orders=unresolved, original_day_orders={
        "2026-11-11": [], "2026-10-30": [],
    })
    tmo2 = next(x for x in result2["positions"] if x["symbol"] == "TMO")
    assert "HISTORICAL_SELL_RECONCILIATION_REQUIRED" in tmo2["blockers"]
    assert "HISTORICAL_KIS_SELL_TERMINAL_UNVERIFIED" in tmo2["blockers"]


def test_practice_db_reads_only_and_history_query_not_epoch_excluded():
    seen = []
    class Result:
        def __init__(self, rows): self.rows = rows
        def mappings(self): return self
        def all(self): return self.rows
    class Conn:
        def execute(self, statement, params=None):
            seen.append((str(statement), params))
            return Result([] if len(seen) == 1 else [{}])
    class Engine:
        @contextmanager
        def connect(self):
            yield Conn()
    positions, pending = load_db_readonly(
        Engine(), epoch="epoch", trade_date="2026-11-11",
    )
    assert len(seen) == 3
    assert seen[0][0] == "SET TRANSACTION READ ONLY"
    assert seen[1][1]["epoch"] == "epoch"
    assert "AND env='practice'" in seen[2][0]
    assert "AND status NOT IN" in seen[2][0]
    assert positions == pending == [{}]


def test_unsafe_runtime_flags_prevent_any_broker_call(monkeypatch, capsys):
    from scripts import us_legacy_nine_sell_readiness as module
    monkeypatch.setattr("sys.argv", [
        "sell-readiness", "--trade-date", "2026-11-11",
        "--confirm-practice-readonly",
    ])
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("NO_TRADE", "0")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_ORDER_ARMED", "0")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "1")
    assert module.main() == 42
    assert '"PRACTICE_READONLY_REQUIRED"' in capsys.readouterr().out
