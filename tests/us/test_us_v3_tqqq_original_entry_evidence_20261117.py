"""TQQQ original BUY evidence must be actual KIS fill + exact epoch/cycle.

Pure fake SQL result (no broker, no DB writes). The production reader is used.
"""
from copy import deepcopy

import pytest

from trader.us.decision_evidence_5w1h import build_decision_evidence
from trader.us.infinite.repository import InfiniteRepository


CYCLE = "tqqq-proof-cycle"
KEY = f"TQQQ_INF_V3:{CYCLE}:2026-11-10:BUY"
EPOCH = "tqqq-practice-epoch"


class Result:
    def __init__(self, rows):
        self.rows = rows
    def mappings(self):
        return self
    def all(self):
        return self.rows


class Conn:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def execute(self, query, params):
        self.queries.append((str(query), params))
        return Result(self.rows)


class Engine:
    def __init__(self, rows):
        self.conn = Conn(rows)
    def connect(self):
        return self.conn


def _evidence():
    return build_decision_evidence(
        decision_id="TQQQ_INFINITE:BUY:" + KEY,
        side="BUY", strategy_owner="TQQQ_INFINITE", symbol="TQQQ",
        decision_at="2026-11-10T13:31:01+00:00",
        policy_version="TQQQ_INFINITE_V3", code_sha="a" * 40,
        position_lifecycle_id=CYCLE, decision_reason="FAST_DIP_ADD_BUY",
        facts={"decision_price_usd": 50.0},
        thresholds={}, market_context={}, sizing={"qty": 4},
    )


def _row():
    evidence = _evidence()
    return {
        "trade_date": "2026-11-10", "qty": 4, "side": "BUY",
        "order_no": "0000099887", "client_order_key": KEY,
        "fill_account_id": "practice-account",
        "intent_account_id": "practice-account",
        "intent_strategy": "TQQQ_INFINITE_V3",
        "fill_meta": {"fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                      "accounting_active": True, "is_synthetic": False},
        "intent_meta": {
            "strategy": "TQQQ_INFINITE_V3",
            "strategy_owner": "TQQQ_INFINITE",
            "cycle_id": CYCLE, "position_lifecycle_id": CYCLE,
            "book": "TQQQ_INFINITE",
            "entry_decision_evidence_5w1h": evidence,
            "entry_decision_evidence_sha256": evidence["sha256"],
        },
    }


def _repository(rows):
    repo = InfiniteRepository(engine=Engine(rows))
    repo._epoch_id = lambda bind, required=True: EPOCH
    return repo


def test_tqqq_exact_actual_filled_original_returns_verifiable_entry():
    row = _row()
    repo = _repository([row])
    result = repo.load_verified_cycle_entry_evidence(cycle_id=CYCLE)
    assert result["sha256"] == row["intent_meta"]["entry_decision_evidence_sha256"]
    assert repo.engine.conn.queries[0][1] == {
        "epoch": EPOCH, "prefix": f"TQQQ_INF_V3:{CYCLE}:%"
    }
    assert "f.trading_epoch_id=:epoch" in repo.engine.conn.queries[0][0]
    assert "i.trade_date<=f.trade_date" in repo.engine.conn.queries[0][0]


@pytest.mark.parametrize("tamper", [
    lambda r: r.update(fill_account_id="foreign-account"),
    lambda r: r.update(intent_account_id=""),
    lambda r: r.update(order_no=""),
    lambda r: r["fill_meta"].update(fill_evidence_type="BALANCE_DELTA_SYNTHETIC"),
    lambda r: r["fill_meta"].update(is_synthetic=True),
    lambda r: r["fill_meta"].update(accounting_active=False),
    lambda r: r["intent_meta"].update(strategy_owner="US_STANDARD"),
    lambda r: r["intent_meta"].update(position_lifecycle_id="other-cycle"),
    lambda r: r["intent_meta"].update(entry_decision_evidence_sha256="0"*64),
    lambda r: r["intent_meta"].update(entry_decision_evidence_5w1h={"sha256": "invalid"}),
    lambda r: r.update(qty=0),
    lambda r: r.update(client_order_key="TQQQ_INF_V3:foreign:2026-11-10:BUY"),
])
def test_original_proof_rejects_ack_only_synthetic_foreign_or_tampered_rows(tamper):
    row = deepcopy(_row())
    tamper(row)
    assert _repository([row]).load_verified_cycle_entry_evidence(cycle_id=CYCLE) is None


def test_dupe_same_buy_fill_must_share_one_immutable_evidence_hash():
    a, b = _row(), deepcopy(_row())
    assert _repository([a,b]).load_verified_cycle_entry_evidence(
        cycle_id=CYCLE,
    )["sha256"] == a["intent_meta"]["entry_decision_evidence_sha256"]
    b["intent_meta"]["entry_decision_evidence_5w1h"] = build_decision_evidence(
        decision_id="TQQQ_INFINITE:BUY:" + KEY,
        side="BUY", strategy_owner="TQQQ_INFINITE", symbol="TQQQ",
        decision_at="2026-11-10T13:33:01+00:00",
        policy_version="TQQQ_INFINITE_V3", code_sha="b"*40,
        position_lifecycle_id=CYCLE, decision_reason="FAST_DIP_ADD_BUY",
        facts={"decision_price_usd": 55.0},
        thresholds={}, market_context={}, sizing={"qty": 4},
    )
    b["intent_meta"]["entry_decision_evidence_sha256"] = (
        b["intent_meta"]["entry_decision_evidence_5w1h"]["sha256"]
    )
    assert _repository([a,b]).load_verified_cycle_entry_evidence(cycle_id=CYCLE) is None
