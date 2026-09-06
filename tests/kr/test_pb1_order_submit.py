from __future__ import annotations

from types import SimpleNamespace

from trader.kr.pb1.order_submit import submit_exit_sell_order


class _Repo:
    def __init__(self):
        self.submitted = []
        self.acked = []
        self.errors = []
        self.position_updates = []

    def mark_submitted(self, env, client_key, kis_odno, resp, submitted_qty):
        self.submitted.append((env, client_key, kis_odno, submitted_qty, resp))

    def mark_acked(self, env, kis_odno, resp):
        self.acked.append((env, kis_odno, resp))

    def mark_error(self, env, client_key, resp):
        self.errors.append((env, client_key, resp))

    def update_position_fields(self, **kwargs):
        self.position_updates.append(kwargs)


class _Kis:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []
        self.invalidations = []

    def sell_stock_market(self, code, qty):
        self.calls.append((code, qty))
        return self.resp

    def invalidate_balance_cache(self, *, reason, codes=None):
        self.invalidations.append((reason, list(codes or [])))


class _NoSellableTracker:
    def __init__(self):
        self.calls = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)


def _engine(resp):
    kis = _Kis(resp)
    repo = _Repo()
    submitted = []
    no_sellable = _NoSellableTracker()

    engine = SimpleNamespace(
        _today="2026-09-05",
        env="practice",
        STRATEGY_NAME="PB1",
        window_internal="day",
        kis=kis,
        orders_repo=repo,
        positions_repo=repo,
        session_exit_submitted_codes=set(),
        no_sellable_qty_terminal_codes=set(),
        _register_session_sell_accepted=lambda **kwargs: submitted.append(kwargs),
        _register_session_no_sellable=no_sellable,
        _classify_submit_terminal_status=lambda **kwargs: (
            "BROKER_REJECTED" if kwargs.get("response") and kwargs["response"].get("rt_cd") != "0"
            else "ACCEPTED_PENDING_FILL"
        ),
        _format_order_result_reason=lambda value: (
            "ORDER_OK" if value and value.get("rt_cd") == "0"
            else f"ORDER_FAIL_BIZ_{value.get('msg_cd')}" if value else "ORDER_FAIL_API(no_response)"
        ),
    )
    return engine, kis, repo, submitted, no_sellable


def test_submit_exit_sell_order_success_persists_cooldown_and_last_exit(monkeypatch, caplog):
    monkeypatch.setattr("trader.kr.pb1.order_submit.emit_event", lambda **kwargs: None)
    engine, kis, repo, submitted, no_sellable = _engine(
        {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": "S-1"}}
    )
    payload = {"submit_attempted": 0, "accepted": 0, "failed": 0, "skipped_reason": "", "order_id": "OID-1"}
    exit_meta = {"exit_reason": "EXIT_HARD_STOP", "exit_stage": "FULL_EXIT"}

    with caplog.at_level("INFO"):
        result = submit_exit_sell_order(
            engine=engine,
            exit_eval_payload=payload,
            exit_eval=SimpleNamespace(primary_reason="EXIT_HARD_STOP", secondary_reasons=[]),
            code="010060",
            display_code="010060",
            stock_name="TEST",
            market="J",
            mode=1,
            sid=1,
            orderable_qty=7,
            mark=12345.0,
            stage="FULL_EXIT",
            client_key="client-1",
            exit_policy_family="FULL_EXIT",
            reason_family="HARD_STOP",
            lifecycle_id="life-1",
            ret_pct=-1.2,
            avg=10000.0,
            kis_qty=7,
            kis_sellable_qty=7,
            position_meta={},
            cycle_id="cycle-1",
            sell_baseline=None,
            days_held=3,
            cooldown_until="2026-09-10",
            exit_meta=exit_meta,
            order_id="OID-1",
        )

    assert result["submitted"] == 1
    assert result["order_result"] == "ORDER_OK"
    assert "terminal_event" not in result
    assert "rejected" not in result
    assert "submit_terminal_status" not in result
    assert kis.calls == [("010060", 7)]
    assert repo.submitted and repo.acked and not repo.errors
    assert submitted
    assert engine.session_exit_submitted_codes == {"010060"}
    assert kis.invalidations == [("sell_ack:010060", ["010060"])]
    assert any("[ORDER][ACCEPTED]" in rec.message for rec in caplog.records)
    assert repo.position_updates[0]["fields"] == {"cooldown_until": "2026-09-10"}
    assert repo.position_updates[1]["fields"]["last_exit_eval_json"] is result
    assert repo.position_updates[1]["fields"]["last_exit_plan_eval_json"] == exit_meta
    assert not no_sellable.calls


def test_submit_exit_sell_order_reject_registers_no_sellable_and_persists_last_exit(monkeypatch):
    monkeypatch.setattr("trader.kr.pb1.order_submit.emit_event", lambda **kwargs: None)
    engine, kis, repo, submitted, no_sellable = _engine(
        {"rt_cd": "1", "msg_cd": "NO_SELLABLE_QTY", "msg1": "sellable quantity is zero"}
    )
    payload = {"submit_attempted": 0, "accepted": 0, "failed": 0, "skipped_reason": "", "order_id": "OID-1"}
    exit_meta = {"exit_reason": "EXIT_HARD_STOP", "exit_stage": "FULL_EXIT"}

    result = submit_exit_sell_order(
        engine=engine,
        exit_eval_payload=payload,
        exit_eval=SimpleNamespace(primary_reason="EXIT_HARD_STOP", secondary_reasons=[]),
        code="010060",
        display_code="010060",
        stock_name="TEST",
        market="J",
        mode=1,
        sid=1,
        orderable_qty=7,
        mark=12345.0,
        stage="FULL_EXIT",
        client_key="client-1",
        exit_policy_family="FULL_EXIT",
        reason_family="HARD_STOP",
        lifecycle_id="life-1",
        ret_pct=-1.2,
        avg=10000.0,
        kis_qty=7,
        kis_sellable_qty=7,
        position_meta={},
        cycle_id="cycle-1",
        sell_baseline=None,
        days_held=3,
        cooldown_until=None,
        exit_meta=exit_meta,
        order_id="OID-1",
    )

    assert result["order_result"] == "ORDER_FAIL_BIZ_NO_SELLABLE_QTY"
    assert "terminal_event" not in result
    assert "rejected" not in result
    assert result["failed"] == 0
    assert "submit_terminal_status" not in result
    assert kis.calls == [("010060", 7)]
    assert repo.submitted and repo.errors and not repo.acked
    assert no_sellable.calls and no_sellable.calls[0]["reason"] == "KIS_NO_SELLABLE_QTY"
    assert "010060" in engine.no_sellable_qty_terminal_codes
    assert repo.position_updates[0]["fields"]["last_exit_eval_json"] is result
    assert repo.position_updates[0]["fields"]["last_exit_plan_eval_json"] == exit_meta
