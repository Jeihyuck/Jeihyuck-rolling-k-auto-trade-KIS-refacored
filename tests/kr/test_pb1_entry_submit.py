from __future__ import annotations

from types import SimpleNamespace

from trader.kr.pb1.entry_submit import submit_entry_buy_order


class _Repo:
    def __init__(self, created=True):
        self.created = created
        self.created_calls = []
        self.submitted = []
        self.acked = []
        self.errors = []
        self.final_skips = []
        self.close_entry_records = []

    def get_order_by_client_order_key(self, env, key):
        return None

    def create_intent_idempotent(self, **kwargs):
        self.created_calls.append(kwargs)
        return "OID-1", self.created

    def mark_submitted(self, env, client_key, kis_odno, resp, entry_meta_json, submitted_qty):
        self.submitted.append((env, client_key, kis_odno, submitted_qty, resp, entry_meta_json))

    def mark_acked(self, env, kis_odno, resp, entry_meta_json=None):
        self.acked.append((env, kis_odno, resp, entry_meta_json))

    def mark_error(self, env, client_key, resp):
        self.errors.append((env, client_key, resp))


class _Kis:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def buy_stock_limit(self, code, qty, price):
        self.calls.append((code, qty, price))
        return self.resp


def _engine(resp, *, created=True, dry_run=False):
    repo = _Repo(created=created)
    kis = _Kis(resp) if resp is not None else None
    intents = []
    final_skips = []
    close_records = []

    engine = SimpleNamespace(
        _today="2026-09-05",
        env="practice",
        run_id="run-1",
        STRATEGY_NAME="PB1",
        dry_run=dry_run,
        kis=kis,
        orders_repo=repo,
        _name_for_code=lambda code: "TEST",
        _log_final_skip=lambda **kwargs: final_skips.append(kwargs),
        _pretrade_check=lambda **kwargs: True,
        _append_ledger_event=lambda **kwargs: intents.append(kwargs),
        _format_order_result_reason=lambda value: (
            "ORDER_OK" if value and value.get("rt_cd") == "0" else f"ORDER_FAIL_BIZ_{value.get('msg_cd')}" if value else "ORDER_FAIL_API(no_response)"
        ),
        _classify_submit_terminal_status=lambda **kwargs: (
            "BROKER_REJECTED" if kwargs.get("response") and kwargs["response"].get("rt_cd") != "0" else "ACCEPTED_PENDING_FILL"
        ),
        _is_retryable_entry_order_status=lambda status: False,
        _next_retry_client_order_key=lambda key: f"{key}-retry",
        _append_close_entry_record=lambda payload: close_records.append(payload),
    )
    return engine, repo, kis, intents, final_skips, close_records


def test_submit_entry_buy_order_success_persists_ack_and_terminal_event(monkeypatch, caplog):
    monkeypatch.setattr("trader.kr.pb1.entry_submit.emit_event", lambda **kwargs: None)
    engine, repo, kis, intents, final_skips, close_records = _engine(
        {"rt_cd": "0", "msg_cd": "0", "msg1": "accepted", "output": {"ODNO": "B-1"}}
    )
    cf = SimpleNamespace(code="009150", market="KOSPI", mode=1, planned_qty=3, client_order_key="key-1", features={"name": "TEST"})
    status = {"submit_attempted": 0, "accepted": 0, "failed": 0, "skipped_reason": ""}
    entry_meta = {
        "entry_reason": "ENTRY_BREAKOUT",
        "entry_decision_family": "SETUP_OVERRIDE",
        "stop_price_at_entry": 100.0,
        "pivot_price_at_entry": 200.0,
        "score_final_at_entry": 99.0,
        "trace_id": "trace-1",
    }
    entry_exit_plan = {"plan": 1}

    with caplog.at_level("INFO"):
        result = submit_entry_buy_order(
            engine=engine,
            cf=cf,
            display_code="009150",
            stock_name="TEST",
            cap=474000.0,
            reasons=["close_entry"],
            base_from="tp",
            base=470000.0,
            cap_buffer_pct=1.0,
            entry_meta=entry_meta,
            entry_exit_plan_dict=entry_exit_plan,
            gate_snapshot={"kis_holding_qty": 2},
            status=status,
        )

    assert result["accepted"] == 1
    assert result["submitted"] == 1
    assert result["terminal_event"] == "API_RESULT"
    assert "rejected" not in result
    assert "submit_terminal_status" in result and result["submit_terminal_status"] == "ACCEPTED_PENDING_FILL"
    assert kis.calls == [("009150", 3, 474000)]
    assert repo.submitted and repo.acked and not repo.errors
    assert intents[0]["event_type"] == "ORDER_INTENT"
    assert intents[1]["event_type"] == "ORDER_SUBMIT_ATTEMPT"
    assert len(close_records) == 1
    assert close_records[0]["order_id"] == "OID-1"
    assert close_records[0]["code"] == "009150"
    assert close_records[0]["qty"] == 3
    assert close_records[0]["cap_price"] == 474000.0
    assert close_records[0]["client_order_key"] == "key-1"
    assert close_records[0]["kis_odno"] == "B-1"
    assert "created_at" in close_records[0]
    assert any("[ORDER][API_RESULT]" in rec.message for rec in caplog.records)
    assert final_skips == []


def test_submit_entry_buy_order_reject_sets_failure_fields(monkeypatch):
    monkeypatch.setattr("trader.kr.pb1.entry_submit.emit_event", lambda **kwargs: None)
    engine, repo, kis, intents, final_skips, close_records = _engine(
        {"rt_cd": "1", "msg_cd": "BIZ_REJECT", "msg1": "rejected"}
    )
    cf = SimpleNamespace(code="009150", market="KOSPI", mode=1, planned_qty=3, client_order_key="key-1", features={"name": "TEST"})
    status = {"submit_attempted": 0, "accepted": 0, "failed": 0, "skipped_reason": ""}
    entry_meta = {
        "entry_reason": "ENTRY_BREAKOUT",
        "entry_decision_family": "SETUP_OVERRIDE",
        "stop_price_at_entry": 100.0,
        "pivot_price_at_entry": 200.0,
        "score_final_at_entry": 99.0,
        "trace_id": "trace-1",
    }

    result = submit_entry_buy_order(
        engine=engine,
        cf=cf,
        display_code="009150",
        stock_name="TEST",
        cap=474000.0,
        reasons=["close_entry"],
        base_from="tp",
        base=470000.0,
        cap_buffer_pct=1.0,
        entry_meta=entry_meta,
        entry_exit_plan_dict={"plan": 1},
        gate_snapshot={"kis_holding_qty": 2},
        status=status,
    )

    assert result["rejected"] == 1
    assert result["failed"] == 1
    assert result["submit_terminal_status"] == "BROKER_REJECTED"
    assert result["terminal_event"] == "API_RESULT"
    assert kis.calls == [("009150", 3, 474000)]
    assert repo.submitted and repo.errors and not repo.acked
    assert intents[0]["event_type"] == "ORDER_INTENT"
    assert intents[1]["event_type"] == "ORDER_SUBMIT_ATTEMPT"
    assert intents[2]["event_type"] == "ORDER_SUBMIT_REJECTED"
    assert close_records == []
    assert final_skips == []
