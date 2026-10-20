"""KR 2026-10-20: bounded dedupe reads and exact-order SELL evidence regressions.

No broker HTTP calls, production credentials, or live order submissions.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    return engine, schema


def _order(schema, *, key, code, side="BUY", status="ERROR", created_at=None, odno=None):
    return dict(
        order_id=str(uuid4()), env="practice", run_id=None,
        strategy="pb1_pullback_close", sid=1, mode=1,
        code=code, market="KRX", side=side, stage="PB1-AM",
        ord_type="LIMIT", qty=2, limit_price=10000.0,
        client_order_key=key, status=status, kis_odno=odno,
        response_json={"confirmed_fill_qty": 2, "confirmed_fill_price": None},
        created_at=created_at or datetime(2026, 10, 20, 9, 5),
    )


def test_prior_intents_batch_preserves_existing_and_retryable_states():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="k-error", code="251970", status="ERROR"),
            _order(schema, key="k-acked", code="192820", status="SUBMITTED", odno="0000001234"),
        ])
    repo = OrdersRepo(engine)
    rows = repo.list_orders_by_client_order_keys(
        "practice", ["k-acked", "k-error", "k-none", "k-acked"]
    )
    keyed = {r["client_order_key"]: r for r in rows}
    assert set(keyed) == {"k-error", "k-acked"}
    assert keyed["k-error"]["status"] == "ERROR"
    assert keyed["k-acked"]["kis_odno"] == "0000001234"
    assert repo.list_orders_by_client_order_keys("real", ["k-error"]) == []
    assert repo.list_orders_by_client_order_keys("practice", []) == []
    engine.dispose()


def test_prior_intents_query_failure_never_fails_open(monkeypatch):
    engine, _ = _db()
    repo = OrdersRepo(engine)
    seen = []

    def broken(*args, **kwargs):
        seen.append(kwargs["fail_open"])
        raise RuntimeError("simulated DB query timeout")

    monkeypatch.setattr(repo, "_read_mappings_with_guard", broken)
    with pytest.raises(RuntimeError, match="timeout"):
        repo.list_orders_by_client_order_keys("practice", ["k-unconfirmed"])
    assert seen == [False]
    engine.dispose()


def test_pending_sell_queue_keeps_ack_and_qty_without_broker_price_unresolved():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="sell-a", code="036540", side="SELL",
                   status="FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED", odno="0000004318"),
            _order(schema, key="sell-b", code="006400", side="SELL",
                   status="SUBMITTED", odno="0000035346"),
            _order(schema, key="buy-c", code="011200", side="BUY",
                   status="SUBMITTED", odno="0000010123"),
        ])
    report = audit_kr_pending_sell_orders(engine, env="practice", trade_date=date(2026, 10, 20))
    assert report["unsettled_sell_count"] == 2
    assert report["missing_order_number_count"] == 0
    assert report["missing_broker_price_count"] == 2
    assert {r["code"] for r in report["orders"]} == {"036540", "006400"}
    assert all(r["required_action"] == "MATCH_KIS_ORDER_NUMBER_AND_EXECUTION_PRICE" for r in report["orders"])
    engine.dispose()


def test_close_submit_then_reconcile_only_with_broker_evidence_contract():
    source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert source.index('"[KR_CLOSE][POLICY][DONE]') < source.index('"[KR_CLOSE][POSTSUBMIT_RECONCILE]')
    assert 'accepted_policy_sells > 0' in source
    assert 'not parse_bool_any(dry_run, default=True)' in source
    assert '"1" if str(env_effective).lower() == "practice" else "0"' in source
    assert '"[KR_CLOSE][SELL_EVIDENCE_QUEUE]' in source


def test_buyable_dedupe_bulk_guard_preserves_fallback():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert 'with self._stage_timer("entry.prior_intents_bulk_lookup")' in source
    assert 'if bulk_prior_intents_enabled:' in source
    assert 'if str(cf.client_order_key) in prior_intent_keys:' in source
    assert 'late_rows = self.orders_repo.list_orders_by_client_order_keys(' in source
    assert 'self.orders_repo.get_order_by_client_order_key(' in source


def test_broker_order_number_scoped_identity_excludes_other_dates_and_symbols():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="old", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 19, 9, 5)),
            _order(schema, key="same-id-different-code", code="042700", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 20, 9, 5)),
            _order(schema, key="correct", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318",
                   created_at=datetime(2026, 10, 20, 9, 6)),
        ])
    repo = OrdersRepo(engine)
    row = repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="SELL",
    )
    assert row["client_order_key"] == "correct"
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 21),
        code="036540", side="SELL",
    ) is None
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="BUY",
    ) is None
    engine.dispose()


def test_broker_order_number_ambiguity_is_not_attributed():
    engine, schema = _db()
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key="first", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318"),
            _order(schema, key="second", code="036540", side="SELL",
                   status="SUBMITTED", odno="0000004318"),
        ])
    repo = OrdersRepo(engine)
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000004318", trade_date=date(2026, 10, 20),
        code="036540", side="SELL",
    ) is None
    engine.dispose()


def test_retry_suffix_family_one_query_preserves_first_available_gap():
    from types import SimpleNamespace
    from trader.pb1_engine import PB1Engine

    engine, schema = _db()
    base = "practice:pb1_pullback_afternoon:2026-10-20:251970:ENTRY:BUY:PB1-PM:afternoon:1"
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key=base, code="251970"),
            _order(schema, key=base + ":retry2", code="251970"),
            _order(schema, key=base + ":retry4", code="251970"),
            _order(schema, key=base.replace("pb1_pullback", "pb1Xpullback") + ":retry3", code="251970"),
        ])
    repo = OrdersRepo(engine)
    seen = []
    actual_read = repo._read_mappings_with_guard

    def counted(*args, **kwargs):
        seen.append(kwargs["op_name"])
        return actual_read(*args, **kwargs)

    repo._read_mappings_with_guard = counted
    obj = SimpleNamespace(orders_repo=repo, env="practice")
    assert PB1Engine._next_retry_client_order_key(obj, base) == base + ":retry3"
    assert seen == ["orders.list_existing_client_order_keys_for_retry"]
    engine.dispose()


def test_retry_prefix_read_failure_keeps_candidate_fenced(monkeypatch):
    from types import SimpleNamespace
    from trader.pb1_engine import PB1Engine

    engine, _ = _db()
    repo = OrdersRepo(engine)

    def fail(*args, **kwargs):
        assert kwargs["fail_open"] is False
        raise RuntimeError("cannot prove retry key is unused")

    monkeypatch.setattr(repo, "_read_mappings_with_guard", fail)
    obj = SimpleNamespace(orders_repo=repo, env="practice")
    with pytest.raises(RuntimeError, match="cannot prove"):
        PB1Engine._next_retry_client_order_key(obj, "practice:pb1:test")
    engine.dispose()


def test_historical_reconcile_needs_explicit_trade_date_and_apply_flag():
    import inspect
    from trader.reconcile_kis import reconcile_today
    from scripts.kr244_reconcile_day import main

    assert "trade_date" in inspect.signature(reconcile_today).parameters
    with pytest.raises(SystemExit):
        main(["--env", "real", "--trade-date", "2026-10-20", "--apply-verified"])
    with pytest.raises(SystemExit):
        main(["--env", "practice", "--trade-date", "2030-01-01"])


def test_kr244_probe_requires_real_same_day_kis_fill_id_price_and_side():
    from scripts.kr244_kis_readonly_probe import evaluate_broker_sell_evidence
    from datetime import date

    base = {
        "ord_dt": "20261020", "pdno": "036540", "odno": "0000004318",
        "sll_buy_dvsn_cd": "01", "ccld_no": "TRADE-1",
        "ccld_qty": "2", "ccld_prc": "9000",
    }
    expected = {"036540": ("0000004318", 2)}
    exact = evaluate_broker_sell_evidence([base, base], date(2026, 10, 20), expected)
    assert exact["orders_verified"] == 1
    assert exact["results"][0]["priced_execution_qty"] == 2
    assert exact["results"][0]["weighted_execution_price"] == 9000
    for wrong in (
        {**base, "ord_dt": "20261019"},
        {**base, "pdno": "039030"},
        {**base, "sll_buy_dvsn_cd": "02"},
        {**base, "odno": "0000009999"},
        {**base, "ccld_no": ""},
        {**base, "ccld_prc": ""},
    ):
        outcome = evaluate_broker_sell_evidence([wrong], date(2026, 10, 20), expected)
        assert outcome["orders_verified"] == 0
        assert outcome["orders_unresolved"] == 1


def test_kr244_reconcile_today_strict_pagination_contract():
    import inspect
    from trader.reconcile_kis import reconcile_today
    source = inspect.getsource(reconcile_today)
    assert inspect.signature(reconcile_today).parameters["require_authoritative"].default is False
    assert "_daily_ccld_all_pages_for_date" in source
    assert "KR_KIS_EXECUTION_EVIDENCE_NOT_AUTHORITATIVE" in source
    close_source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    replay_source = Path("scripts/kr244_reconcile_day.py").read_text(encoding="utf-8")
    assert "reconcile_today(engine=engine, kis=kis, ctx=post_ctx, require_authoritative=True)" in close_source
    assert "trade_date=trade_date, require_authoritative=True" in replay_source
    assert "strict_sell_targets=EXPECTED_OCT20_SELLS" in replay_source


def test_kis_practice_ccld_uses_verified_official_tr_and_all_exchange(monkeypatch):
    from types import SimpleNamespace
    from trader import kis_wrapper

    calls = []
    class Response:
        status_code = 200
        headers = {"tr_cont": "D"}
        def json(self):
            return {"rt_cd": "0", "output1": [], "output2": []}

    def get(*args, **kwargs):
        calls.append(kwargs)
        return Response()

    monkeypatch.setattr(kis_wrapper, "kis_http_enabled", lambda: True)
    monkeypatch.setattr(
        kis_wrapper, "_pick_tr",
        lambda env, category: ["VTTC0081R"] if env == "practice" else ["TTTC8001R"],
    )
    api = kis_wrapper.KisAPI.__new__(kis_wrapper.KisAPI)
    api.CANO = "00000001"
    api.ACNT_PRDT_CD = "01"
    api.env = "practice"
    api.session = SimpleNamespace(request=get)
    api._headers = lambda tr_id: {"tr_id": tr_id}
    result = api.inquire_daily_ccld(start_date="20261020", end_date="20261020")
    assert result["rt_cd"] == "0"
    assert calls[0]["headers"]["tr_id"] == "VTTC0081R"
    assert calls[0]["params"]["EXCG_ID_DVSN_CD"] == "ALL"

    # Do not silently change real-account behavior in the KR practice P0 fix.
    api.env = "real"
    calls.clear()
    api.inquire_daily_ccld(start_date="20261020", end_date="20261020")
    assert "EXCG_ID_DVSN_CD" not in calls[0]["params"]


def test_kis_practice_official_ccld_default_is_not_legacy():
    from pathlib import Path
    s = Path("trader/kis_wrapper.py").read_text(encoding="utf-8")
    assert 'KIS_TR_ID_DAILY_CCLD", "VTTC0081R"' in s
    assert 'params["EXCG_ID_DVSN_CD"] = exchange_scope_override or "ALL"' in s


def test_kr244_official_kis_cumulative_sell_economics_without_individual_fill():
    from scripts.kr244_kis_readonly_probe import evaluate_broker_sell_evidence
    from datetime import date
    common = {
        "ord_dt": "20261020", "pdno": "036540", "odno": "0000004318",
        "sll_buy_dvsn_cd": "01",
        "tot_ccld_qty": "140", "tot_ccld_amt": "1272600",
        "avg_prvs": "9090", "ord_qty": "140", "ord_unpr": "9200",
    }
    expected = {"036540": ("0000004318", 140)}
    good = evaluate_broker_sell_evidence([common, common], date(2026,10,20), expected)
    assert good["orders_verified"] == 1
    assert good["results"][0]["priced_execution_qty"] == 140
    assert good["results"][0]["weighted_execution_price"] == 9090
    assert good["results"][0]["evidence"] == "KIS_CUMULATIVE_QTY_AMOUNT_AVG_PROVEN"

    invalids = [
        {**common, "tot_ccld_qty": "0"},
        {**common, "tot_ccld_amt": "0"},
        {**common, "avg_prvs": "0"},
        {**common, "tot_ccld_amt": "200000"},
        {**common, "tot_ccld_qty": "150"},
        {**common, "sll_buy_dvsn_cd": "02"},
        {**common, "ord_dt": "20261019"},
        {**common, "odno": "0000004319"},
    ]
    for bad in invalids:
        result = evaluate_broker_sell_evidence([bad], date(2026,10,20), expected)
        assert result["orders_verified"] == 0
    partial = evaluate_broker_sell_evidence([
        {**common, "tot_ccld_qty": "70", "tot_ccld_amt": "636300"}
    ], date(2026,10,20), expected)
    assert partial["orders_verified"] == 0
    assert partial["results"][0]["priced_execution_qty"] == 70


def test_kr244_kis_aggregate_sell_proof_requires_exact_identity_and_two_prices():
    from trader.reconcile_kis import _verified_kr_cumulative_sell_evidence
    source = {
        "env": "practice", "side": "SELL", "code": "036540",
        "kis_odno": "0000004318", "qty": 140,
        "order_id": "evidence-id", "client_order_key": "original-sell-key",
    }
    row = {
        "ord_dt": "20261020", "ord_tmd": "093000", "pdno": "036540",
        "sll_buy_dvsn_cd": "01", "odno": "0000004318",
        "ord_qty": "140", "tot_ccld_qty": "140",
        "tot_ccld_amt": "1272600", "avg_prvs": "9090",
    }
    kwargs = {
        "source_order": source, "side": "SELL",
        "trade_date": date(2026, 10, 20),
        "kis_odno": "0000004318", "env": "practice",
    }
    verified = _verified_kr_cumulative_sell_evidence(row, **kwargs)
    assert verified is not None
    assert verified["qty"] == 140
    assert verified["notional"] == 1272600
    assert verified["avg_price"] == 9090
    assert verified["kind"] == "KIS_ORDER_CUMULATIVE_EXECUTION"
    assert verified["evidence_id"] == _verified_kr_cumulative_sell_evidence(row, **kwargs)["evidence_id"]

    for bad in (
        {**row, "tot_ccld_qty": "139"},
        {**row, "tot_ccld_amt": "0"},
        {**row, "avg_prvs": "0"},
        {**row, "avg_prvs": "9000"},
        {**row, "ord_dt": "20261019"},
        {**row, "sll_buy_dvsn_cd": "02"},
        {**row, "pdno": "039030"},
        {**row, "odno": "0000009999"},
        {**row, "ccld_no": "REAL_EXECUTED_FILL"},
    ):
        assert _verified_kr_cumulative_sell_evidence(bad, **kwargs) is None


def test_kr244_cumulative_sell_avoids_duplicate_prior_individual_fills():
    from trader.reconcile_kis import _kr_source_has_individual_sell_fills
    engine, schema = _db()
    source_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), {
            **_order(schema, key="owned-sell", code="036540", side="SELL",
                     status="SUBMITTED", odno="0000004318"),
            "order_id": source_id,
        })
        conn.execute(schema.fills.insert(), {
            "fill_id": str(uuid4()), "order_id": source_id,
            "env": "practice", "code": "036540", "side": "SELL",
            "trade_id": "ACTUAL-TRADE", "broker_fill_id": "ACTUAL-TRADE",
            "qty": 1, "price": 9090.0, "fee": 0.0, "tax": 0.0,
            "filled_at": datetime(2026, 10, 20, 9, 5),
            "fill_meta_json": {"fill_source": "daily_ccld"},
        })
    from trader.db.repos import FillsRepo
    assert _kr_source_has_individual_sell_fills(
        fills_repo=FillsRepo(engine),
        source_order={"order_id": source_id}, env="practice",
    ) is True
    engine.dispose()


def test_kr244_cumulative_price_only_replay_preserves_existing_holding_qty(monkeypatch):
    import trader.reconcile_kis as rk
    from trader.run_context import RunContext
    from types import SimpleNamespace

    source = {
        "env": "practice", "side": "SELL", "code": "036540",
        "kis_odno": "0000004318", "qty": 140,
        "order_id": "original-id", "client_order_key": "original-key",
        "strategy": "pb1_pullback_close", "sid": 1, "mode": 1,
        "position_cycle_id": "same-cycle", "portfolio_epoch_id": "same-epoch",
        "request_json": {"pre_order_holding_qty": 140},
        "response_json": {"confirmed_fill_qty": 140, "confirmed_fill_notional": 0},
    }
    class Orders:
        def __init__(self):
            self.events = []
        def get_order_by_kis_odno(self, env, no):
            return source
        def upsert_reconciled_order(self, **kwargs):
            self.events.append(kwargs)
        def record_execution_claim_for_order(self, *args, **kwargs):
            return None
        def execution_claim_health(self, *, market=None):
            return {"available": True, "unresolved_execution_actions": 0}

    class Fills:
        def __init__(self):
            self.inserts = []
        def upsert_fill(self, **kwargs):
            self.inserts.append(kwargs)
            return "fill-id"

    class Positions:
        def __init__(self):
            self.sells = []
        def reconcile_sell_execution(self, **kwargs):
            self.sells.append(kwargs)
            return {"qty_applied": 0, "pnl_qty_applied": 140}

    class NoOp:
        def __getattr__(self, _name):
            return lambda **kwargs: None

    orders, fills, positions = Orders(), Fills(), Positions()
    monkeypatch.setattr(rk, "OrdersRepo", lambda _: orders)
    monkeypatch.setattr(rk, "FillsRepo", lambda _: fills)
    monkeypatch.setattr(rk, "PositionsRepo", lambda _: positions)
    monkeypatch.setattr(rk, "LedgerEventsRepo", lambda _: NoOp())
    monkeypatch.setattr(rk, "ReconcileLogRepo", lambda _: NoOp())
    monkeypatch.setattr(rk, "_resolve_reconcile_run_id", lambda *_: None)
    monkeypatch.setattr(rk, "_kr_source_has_individual_sell_fills", lambda **_: False)
    monkeypatch.setattr(rk, "_execution_claim_health", lambda *_args, **_kwargs: {"available": True})
    monkeypatch.setattr(rk, "_execution_claim_integrity_reason", lambda *_args, **_kwargs: None)
    class FakeKIS:
        def inquire_daily_ccld(self, **kwargs):
            return {"rt_cd": "0", "output1": [{
                "ord_dt": "20261020", "ord_tmd": "093000", "pdno": "036540",
                "sll_buy_dvsn_cd": "01", "odno": "0000004318",
                "ord_qty": "140", "tot_ccld_qty": "140",
                "tot_ccld_amt": "1272600", "avg_prvs": "9090",
            }], "output2": []}
    ctx = RunContext(
        run_id="test-kr244", env="practice", strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 20, 15, 0), dry_run=True,
    )
    first = rk.reconcile_today(engine=None, kis=FakeKIS(), ctx=ctx)
    assert first["orders"] == 1
    assert first["fills"] == 0  # qty was proven before broker price arrived
    assert len(positions.sells) == 1
    assert positions.sells[0]["confirmed_cumulative_qty"] == 140
    assert positions.sells[0]["fill_price"] == 9090
    assert len(fills.inserts) == 1
    assert fills.inserts[0]["fill_meta_json"]["fill_source"] == "daily_ccld_order_cumulative"
    assert orders.events[-1]["response_json"]["confirmed_fill_notional"] == 1272600
    assert orders.events[-1]["response_json"]["confirmed_fill_price"] == 9090


def test_kr244_247_candidate_keys_one_db_select_no_n_plus_one():
    """Real Oct20 afternoon had 247 finalized candidates and slow per-key DB calls.

    Verify SQL query complexity, not an invented live tick latency target.
    """
    engine, schema = _db()
    keys = [f"practice:kr:test:2026-10-20:{i:06d}:BUY" for i in range(247)]
    with engine.begin() as conn:
        conn.execute(schema.orders.insert(), [
            _order(schema, key=keys[i], code=f"{i:06d}")
            for i in range(0, 247, 17)
        ])
    queries = []
    def track(_conn, _cursor, statement, _params, _context, _many):
        if "SELECT" in statement.upper() and "orders" in statement.lower():
            queries.append(statement)
    sa.event.listen(engine, "before_cursor_execute", track)
    try:
        rows = OrdersRepo(engine).list_orders_by_client_order_keys(
            "practice", keys,
        )
    finally:
        sa.event.remove(engine, "before_cursor_execute", track)
    assert len(rows) == len(range(0, 247, 17))
    assert len(queries) == 1
    engine.dispose()
