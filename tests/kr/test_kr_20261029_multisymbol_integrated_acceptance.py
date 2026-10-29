"""Multi-symbol KR PB1 regression: never treat the Oct 29 Mico case as a symbol-specific fix.

All tests use generated/isolated contract data. No production DB mutation,
WSL scheduler change, live broker call, or paper order submission.
"""
from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from trader.db.repos import _kr_buy_entry_contract_hash
from trader.pb1_runner import run_kr_close_policy_from_tagged_positions
from trader.trade_plan import build_entry_exit_plan
from tests.kr.test_kr_20261029_mico_broker_proven_day_close import (
    _fixtures,
    _Positions,
    _Fills,
    _Orders,
)

from tests.kr.test_kr_20261029_mico_fill_text_json_postgres import kr_mico_text_engine

KST = ZoneInfo("Asia/Seoul")
CODES = ["059090"] + [f"{i:06d}" for i in range(10001, 10030)]
assert len(CODES) == len(set(CODES)) == 30


def _make_contract(code: str, style: str = "ENTRY_MOMENTUM"):
    holding, order = _fixtures(style=style)
    order["code"] = code
    order["kis_odno"] = f"{int(code):010d}"
    order["client_order_key"] = "multisymbol-" + code + "-" + uuid4().hex
    request = order["request_json"]
    request["client_order_key"] = order["client_order_key"]
    request["entry_exit_plan"] = build_entry_exit_plan(
        code=code,
        market="KOSDAQ",
        entry_style_selected=style,
        entry_reason=style,
        entry_price=26050,
        features={"initial_stop": 25000},
    ).to_dict()
    request["_reconcile_observation"]["kis_row"]["pdno"] = code
    request["_reconcile_observation"]["kis_row"]["odno"] = order["kis_odno"]
    request["entry_contract_sha256"] = _kr_buy_entry_contract_hash(request)
    holding["pdno"] = code
    return holding, order


def _close(monkeypatch, holdings, orders):
    import trader.pb1_runner as runner
    monkeypatch.setattr(
        runner, "now_kst",
        lambda: datetime(2026, 10, 29, 15, 15, tzinfo=KST),
    )
    return run_kr_close_policy_from_tagged_positions(
        kis_holdings=holdings,
        positions_repo=_Positions(),
        fills_repo=_Fills(),
        orders_repo=_Orders(orders),
        kis_client=None,
        env="practice",
        strategy="pb1_pullback_close",
        dry_run=True,
    )


def test_thirty_distinct_symbols_preserve_exact_day_vs_swing_contracts(monkeypatch):
    contracts = [
        _make_contract(code, "ENTRY_MOMENTUM" if i % 2 == 0 else "ENTRY_PULLBACK")
        for i, code in enumerate(CODES)
    ]
    result = _close(monkeypatch, [a for a, _ in contracts], [b for _, b in contracts])
    day_orders = {order["code"]: order for i, (_, order) in enumerate(contracts) if i % 2 == 0}
    swing_codes = {order["code"] for i, (_, order) in enumerate(contracts) if i % 2}
    sells = result["policy_orders"]
    assert len(day_orders) == len(swing_codes) == 15
    assert len(sells) == 15
    assert {sell["code"] for sell in sells} == set(day_orders)
    assert not {sell["code"] for sell in sells}.intersection(swing_codes)
    for sell in sells:
        original = day_orders[sell["code"]]
        assert sell["side"] == "SELL"
        assert sell["qty"] == 79
        assert sell["reason"] == "KR_CLOSE_DAY_FORCE_EOD"
        assert sell["position_cycle_id"] == original["position_cycle_id"]
        assert sell["portfolio_epoch_id"] == original["portfolio_epoch_id"]
    assert all(item["result"] == "DRY_RUN" for item in result["policy_results"])


def test_bad_or_duplicate_symbol_does_not_contaminate_other_29(monkeypatch):
    contracts = [_make_contract(code) for code in CODES]
    holdings = [a for a, _ in contracts]
    orders = [b for _, b in contracts]
    # A source mismatch must suppress only this symbol, never borrow another.
    holdings[0]["pchs_amt"] = "999999"
    # Two different same-day BUY orders for one symbol are ambiguous.
    extra = dict(orders[1])
    extra["order_id"] = str(uuid4())
    extra["client_order_key"] = "ambiguous-" + uuid4().hex
    orders.append(extra)
    result = _close(monkeypatch, holdings, orders)
    sells = result["policy_orders"]
    expected = set(CODES[2:])
    assert len(sells) == 28
    assert {sell["code"] for sell in sells} == expected
    assert not any(sell["code"] in CODES[:2] for sell in sells)


@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="isolated disposable PostgreSQL is required; do not use production",
)
def test_physical_postgres_text_json_upsert_isolated_across_thirty_symbols(kr_mico_text_engine):
    # Reuse the disposable PostgreSQL + physical TEXT fixture, never the
    # production trading Supabase. The fixture is skipped if PG is unavailable.
    from trader.account_state import get_account_key
    from trader.db.repos import FillsRepo, OrdersRepo
    from trader.db.schema import schema_for_engine
    import sqlalchemy as sa
    engine = kr_mico_text_engine
    fills = FillsRepo(engine)
    orders = OrdersRepo(engine)
    for i, code in enumerate(CODES):
        order_id, created = orders.create_intent_idempotent(
            env="practice", run_id=None, strategy="pb1_pullback_close",
            sid=1, mode=1, code=code, market="KOSDAQ",
            side="BUY", ord_type="LIMIT", qty=i + 1, limit_price=26050,
            stage="PB1-AM", client_order_key="ms-pg-" + uuid4().hex,
            request_json={}, status="ACKED",
            account_id=get_account_key(env="practice"),
        )
        assert created
        broker_order = f"{i + 1:010d}"
        total = (i + 1) * 26050
        args = dict(
            env="practice", run_id=None, order_id=order_id,
            kis_odno=broker_order,
            trade_id=f"KR_KIS_CUM:practice:20261029:{broker_order}:{code}:BUY",
            code=code, market="KOSDAQ", side="BUY",
            qty=i + 1, price=26050, fee=0, tax=0,
            filled_at=datetime(2026, 10, 29, 0, 31, tzinfo=timezone.utc),
            raw_json={"kis_response": {
                "ord_dt": "20261029", "odno": broker_order, "pdno": code,
                "tot_ccld_qty": str(i + 1), "tot_ccld_amt": str(total),
            }},
            fill_meta_json={"evidence_kind": "KIS_ORDER_CUMULATIVE_EXECUTION"},
        )
        fills.upsert_fill(**args)
        fills.upsert_fill(**args)  # broker replay is idempotent
    schema = schema_for_engine(engine)
    with engine.connect() as conn:
        rows = conn.execute(sa.select(
            schema.fills.c.code, schema.fills.c.qty, schema.fills.c.trade_id
        ).where(schema.fills.c.code.in_(CODES))).mappings().all()
    assert len(rows) == 30
    by_code = {str(r["code"]): int(r["qty"]) for r in rows}
    assert by_code == {code: i + 1 for i, code in enumerate(CODES)}
    assert len({r["trade_id"] for r in rows}) == 30
