"""PR257 conservative protected SELL / post-exit duplicate reconciliation contracts."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4
import inspect

import sqlalchemy as sa

from trader.db.schema import schema_for_engine
import trader.kr.protective_sell_handoff as proof
from trader.kr.reconcile_lanes_257 import (
    can_skip_post_exit_full_reconcile, prove_no_kr_infinite_unresolved,
    exit_pass_proves_zero_sell,
)


KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 10, 26, 9, 46, tzinfo=KST)


def _known_balance():
    return {
        "output1": [{"pdno": "053800", "hldg_qty": "12"}],
        "output2": [{"tot_evlu_amt": "1000000"}],
        "_kr_verified_fresh_at_kst": NOW.isoformat(),
    }


def test_readonly_early_sell_proof_requires_active_account_epoch(monkeypatch):
    monkeypatch.setattr(proof, "now_kst", lambda: NOW)
    monkeypatch.setattr(proof, "get_account_key", lambda **kw: "account-A")
    monkeypatch.setattr("trader.account_state.account_reset_mode", lambda: False)
    # Existing test validates PB1 position-cycle/portfolio association; a
    # separate regression below exercises the real KR_INFINITE intent table.
    monkeypatch.setattr(
        "trader.kr.reconcile_lanes_257.prove_no_kr_infinite_unresolved",
        lambda **kwargs: (True, "KR_INFINITE_NO_OPEN_ACTIVE_EPOCH_INTENTS"),
    )
    engine = sa.create_engine("sqlite:///:memory:")
    tables = schema_for_engine(engine)
    tables.metadata.create_all(engine)
    pe = str(uuid4())
    te = str(uuid4())
    cycle = str(uuid4())
    with engine.begin() as conn:
        conn.execute(tables.portfolio_epochs.insert().values(
            portfolio_epoch_id=pe, trading_epoch_id=te,
            env="practice", account_id="account-A", sid=1, mode=1,
            strategy="pb1_pullback_close", status="ACTIVE",
        ))

    class SafeOrders:
        def __init__(self, *args): pass
        def get_open_orders(self, *args, **kwargs):
            assert kwargs["fail_open"] is False
            return []
        def execution_claim_health(self, *args, **kwargs):
            return {"available": True, "in_flight": 0, "uncertain": 0, "unresolved": 0}

    current = {
        "code": "053800", "status": "OPEN", "strategy": "pb1_pullback_close",
        "qty": 12, "avg_buy_price": 90000,
        "portfolio_epoch_id": pe, "trading_epoch_id": te,
        "position_cycle_id": cycle, "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_meta_json": {"entry_style_selected": "ENTRY_PULLBACK"},
        "entry_exit_plan_json": {"exit_policy_family": "SWING_STAGED_EXIT"},
    }
    monkeypatch.setattr(proof, "OrdersRepo", SafeOrders)
    monkeypatch.setattr(proof, "PositionsRepo", lambda *_: type(
        "Positions", (), {"list_active_positions_for_exit": lambda *a: [current]}
    )())
    kwargs = dict(
        engine=engine, kis=object(), env="practice", strategy="pb1_pullback_close",
        trade_date=date(2026, 10, 26), balance_snapshot=_known_balance(),
    )
    assert proof.prove_protective_sell_only_preflight(**kwargs) == (
        True, "SAFE_BROKER_BALANCE_SELL_ONLY_POST_RECONCILE_REQUIRED"
    )
    current["trading_epoch_id"] = str(uuid4())
    assert proof.prove_protective_sell_only_preflight(**kwargs)[0] is False
    engine.dispose()


def test_early_sell_is_never_allowed_on_stale_or_unresolved_account(monkeypatch):
    monkeypatch.setattr(proof, "now_kst", lambda: NOW)
    kwargs = dict(engine=object(), kis=object(), env="practice",
                  strategy="pb1_pullback_close", trade_date=date(2026, 10, 26))
    assert proof.prove_protective_sell_only_preflight(
        **kwargs, balance_snapshot={**_known_balance(),
                                    "_kr_verified_fresh_at_kst": (NOW-timedelta(seconds=21)).isoformat()}
    )[0] is False
    assert proof.prove_protective_sell_only_preflight(
        **{**kwargs, "env": "real"}, balance_snapshot=_known_balance()
    ) == (False, "SCOPE_NOT_VALIDATED")


def test_two_sided_proof_only_skips_full_when_zero_submit_activity():
    clean = dict(holdings=1, checked=1, submit_attempt_count=0,
                 accepted_sell_count=0, fill_confirmed_sell_count=0,
                 submitted=0, blocked_count=0)
    base = dict(pre_exit_exact=True, post_exit_exact=True,
                exit_pass_ok=True, exit_summary=clean)
    assert can_skip_post_exit_full_reconcile(**base)[0] is True
    assert not can_skip_post_exit_full_reconcile(**{**base, "pre_exit_exact": False})[0]
    assert not can_skip_post_exit_full_reconcile(**{**base, "post_exit_exact": False})[0]
    assert not can_skip_post_exit_full_reconcile(**{**base, "account_reset_mode": True})[0]
    assert not can_skip_post_exit_full_reconcile(**{**base, "exit_pass_ok": False})[0]
    for k in ("submit_attempt_count", "accepted_sell_count", "fill_confirmed_sell_count",
              "submitted", "blocked_count"):
        assert not can_skip_post_exit_full_reconcile(
            **{**base, "exit_summary": {**clean, k: 1}}
        )[0]
    assert not can_skip_post_exit_full_reconcile(
        **{**base, "exit_summary": {**clean, "checked": 0}}
    )[0]


def test_normal_sell_only_proof_can_skip_full_with_post_exact_and_identical_account():
    clean = dict(holdings=1, checked=1, submit_attempt_count=0,
                 accepted_sell_count=0, fill_confirmed_sell_count=0,
                 submitted=0, blocked_count=0)
    pre = _known_balance()
    pre["output1"].append({"pdno": "122630", "hldg_qty": "7"})
    post = {**pre, "output1": list(reversed(pre["output1"]))}
    base = dict(
        pre_exit_exact=False, pre_exit_sell_only=True,
        pre_exit_balance=pre, post_exit_balance=post,
        post_exit_exact=True, exit_pass_ok=True, exit_summary=clean,
    )
    assert can_skip_post_exit_full_reconcile(**base) == (
        True, "SELL_ONLY_PLUS_POST_EXACT_NO_SELL_SKIP_HISTORICAL_RECOVERY"
    )
    assert not can_skip_post_exit_full_reconcile(
        **{**base, "pre_exit_sell_only": False}
    )[0]
    assert not can_skip_post_exit_full_reconcile(
        **{**base, "post_exit_exact": False}
    )[0]
    assert not can_skip_post_exit_full_reconcile(
        **{**base, "post_exit_balance": {
            **post, "output1": [{"pdno": "053800", "hldg_qty": "11"},
                                 {"pdno": "122630", "hldg_qty": "7"}],
        }}
    )[0]
    for k in ("submit_attempt_count", "blocked_count", "accepted_sell_count"):
        assert not can_skip_post_exit_full_reconcile(
            **{**base, "exit_summary": {**clean, k: 1}}
        )[0]
    assert not can_skip_post_exit_full_reconcile(
        **{**base, "account_reset_mode": True}
    )[0]


def test_sell_only_proof_does_not_trust_malformed_or_duplicate_broker_holdings():
    clean = dict(holdings=1, checked=1, submit_attempt_count=0,
                 accepted_sell_count=0, fill_confirmed_sell_count=0,
                 submitted=0, blocked_count=0)
    base = dict(pre_exit_exact=False, pre_exit_sell_only=True,
                post_exit_exact=True, exit_pass_ok=True, exit_summary=clean,
                pre_exit_balance=_known_balance(), post_exit_balance=_known_balance())
    for invalid in (
        [{"pdno": "053800", "hldg_qty": "NaN"}],
        [{"pdno": "053800", "hldg_qty": "12.5"}],
        [{"pdno": "053800", "hldg_qty": "-1"}],
        [{"pdno": "053800", "hldg_qty": "12"},
         {"pdno": "053800", "hldg_qty": "12"}],
        [{"pdno": "053800", "hldg_qty": None}],
        [{"pdno": "", "hldg_qty": "12"}],
    ):
        assert not can_skip_post_exit_full_reconcile(
            **{**base, "post_exit_balance": {
                **_known_balance(), "output1": invalid
            }}
        )[0]


def test_proof_rejects_nonintegral_or_nonfinite_broker_quantity():
    assert proof._valid_nonnegative_integral("3.1") is None
    assert proof._valid_nonnegative_integral("NaN") is None
    assert proof._valid_nonnegative_integral("inf") is None
    assert proof._valid_nonnegative_integral("-1") is None
    assert proof._valid_nonnegative_integral("5.0") == 5


def test_runner_preserves_sell_before_reconcile_and_post_sell_buy_fence():
    from trader import pb1_runner
    source = inspect.getsource(pb1_runner.run_once)
    assert source.index("prove_protective_sell_only_preflight(") < source.index("prove_safe_protective_sell_handoff(")
    assert "if ((pre_exit_exact_proof or sell_only_lane)" in source
    assert "_zero_sell, _zero_sell_reason = exit_pass_proves_zero_sell(" in source
    assert "pre_exit_balance=balance_snapshot_raw" in source
    assert "post_exit_balance=_post_snapshot" in source
    assert source.index("prove_protective_sell_only_preflight(") < source.index("reconcile_kis(")
    assert source.index("if not deferred_full_reconcile:") < source.index("prove_safe_protective_sell_handoff(")
    assert "can_skip_post_exit_full_reconcile(" in source
    assert "BLOCK_ALL_NEW_BUY_KEEP_SELL_RECEIPTS" in source
    assert '"submit_attempt_count"' in inspect.getsource(exit_pass_proves_zero_sell)



def test_cross_owner_active_epoch_pending_intent_blocks_pb1_optimization(monkeypatch):
    from sqlalchemy import text
    monkeypatch.setattr(
        "trader.db.trading_epoch.get_account_key", lambda **kwargs: "account-A",
    )
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE trading_epochs(
                trading_epoch_id TEXT, env TEXT, account_id TEXT,
                status TEXT, started_at TEXT, created_at TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE kr_infinite_order_intents(
                trading_epoch_id TEXT NOT NULL, status TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO trading_epochs VALUES
            ('epoch-a', 'practice','account-A','ACTIVE','2026-10-26','2026-10-26'),
            ('epoch-old', 'practice','account-A','ENDED','2026-10-22','2026-10-22')
        """))
        conn.execute(text("""
            INSERT INTO kr_infinite_order_intents VALUES ('epoch-old', 'SUBMITTED')
        """))
    kwargs = {"engine": engine, "env": "practice", "account_id": "account-A"}
    assert prove_no_kr_infinite_unresolved(**kwargs)[0] is True
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO kr_infinite_order_intents VALUES ('epoch-a', 'RECONCILE_PENDING')
        """))
    allowed, reason = prove_no_kr_infinite_unresolved(**kwargs)
    assert not allowed and reason == "KR_INFINITE_UNRESOLVED_ORDERS"
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE kr_infinite_order_intents SET status='FILLED'
            WHERE trading_epoch_id='epoch-a'
        """))
    assert prove_no_kr_infinite_unresolved(**kwargs)[0] is True
    engine.dispose()


def test_cross_owner_missing_epoch_table_fails_closed():
    engine = sa.create_engine("sqlite:///:memory:")
    allowed, reason = prove_no_kr_infinite_unresolved(
        engine=engine, env="practice", account_id="account-A",
    )
    assert allowed is False
    assert reason.startswith("KR_INFINITE_INTENT_READ_FAILED_")
    engine.dispose()


def test_cross_owner_proof_in_both_protective_sell_lanes():
    src = inspect.getsource(proof.prove_protective_sell_only_preflight)
    exact = inspect.getsource(proof.prove_safe_protective_sell_handoff)
    assert "prove_no_kr_infinite_unresolved(" in src
    assert "prove_no_kr_infinite_unresolved(" in exact



def test_exit_activity_is_screened_before_expensive_post_kis_history():
    clean = dict(holdings=1, checked=1, submit_attempt_count=0,
                 accepted_sell_count=0, fill_confirmed_sell_count=0,
                 submitted=0, blocked_count=0)
    assert exit_pass_proves_zero_sell(exit_pass_ok=True, exit_summary=clean)[0]
    for key in ("submit_attempt_count", "accepted_sell_count", "submitted",
                "blocked_count", "fill_confirmed_sell_count"):
        assert not exit_pass_proves_zero_sell(
            exit_pass_ok=True, exit_summary={**clean, key: 1}
        )[0]
    assert not exit_pass_proves_zero_sell(exit_pass_ok=True, exit_summary={})[0]
    assert not exit_pass_proves_zero_sell(exit_pass_ok=False, exit_summary=clean)[0]
