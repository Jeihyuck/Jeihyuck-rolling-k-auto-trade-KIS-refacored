import inspect


def test_practice_fills_params_are_official_blank_scope():
    from trader.us.execution.kis_us_client import KisUSClient
    client = KisUSClient(env="practice", offline=True)
    params = client._build_us_fills_params("20260716", "PRACTICE_RANGE")
    assert params["PDNO"] == ""
    assert params["OVRS_EXCG_CD"] == ""
    assert params["ORD_DT"] == ""
    assert params["ORD_STRT_DT"] == "20260716"
    assert params["ORD_END_DT"] == "20260716"
    assert params["CCLD_NCCS_DVSN"] == "00"


def test_fills_pagination_uses_kis_context(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient
    client = KisUSClient(env="practice", offline=False)
    monkeypatch.setattr(client, "_assert_not_offline", lambda *_: None)
    monkeypatch.setattr(client, "_build_headers", lambda *_: {"tr_id": "VTTS3035R"})
    calls = []
    def fake_get(path, headers, params, suppress_final_log=False):
        calls.append((dict(headers), dict(params)))
        if len(calls) == 1:
            return {"output": [{"odno": "1"}], "ctx_area_nk200": "NK", "ctx_area_fk200": "FK", "_response_meta": {"tr_cont": "M"}}
        return {"output": [{"odno": "2"}], "_response_meta": {"tr_cont": ""}}
    monkeypatch.setattr(client, "_get", fake_get)
    rows = client.get_us_fills_today("2026-07-16")
    assert [r["odno"] for r in rows] == ["1", "2"]
    assert calls[1][0]["tr_cont"] == "N"
    assert calls[1][1]["CTX_AREA_NK200"] == "NK"
    assert calls[1][1]["CTX_AREA_FK200"] == "FK"


def test_unfilled_kis_rows_are_not_accounting_fills():
    from trader.us.execution.fills import get_fills_today
    class Client:
        def get_us_fills_today(self, trade_date=None):
            return [
                {"odno":"U","pdno":"AMD","sll_buy_dvsn_cd":"01","ft_ord_qty":"3","ft_ccld_qty":"0","nccs_qty":"3","ft_ccld_unpr3":"0","ord_dt":"20260716"},
                {"odno":"F","pdno":"AMD","sll_buy_dvsn_cd":"01","ft_ord_qty":"3","ft_ccld_qty":"2","nccs_qty":"1","ft_ccld_unpr3":"100","ord_dt":"20260716"},
            ]
    class Provider:
        _offline = False
        def _get_client(self): return Client()
    result = get_fills_today(provider=Provider(), trade_date="2026-07-16")
    assert result["status"] == "OK"
    assert [row["order_no"] for row in result["fills"]] == ["F"]
    assert result["fills"][0]["qty"] == 2


def test_tick_and_router_have_no_typeerror_trade_date_retry():
    import trader.us.runner.trade_tick_runner as tick
    import trader.us.execution.order_router as router
    tick_src = inspect.getsource(tick)
    router_src = inspect.getsource(router)
    assert "reconcile_positions(provider=provider)" not in tick_src
    assert "save_fills(fills_today)" not in tick_src
    assert "save_position_snapshot(recon_positions)" not in tick_src
    assert "save_reconcile_log(payload)" not in tick_src
    assert "return func(payload)" not in router_src
    assert "_load_positions_for_reconcile_skip(trade_date)" in tick_src
    assert "db_load_positions(trade_date)" in tick_src


def test_authoritative_empty_balance_is_persisted(monkeypatch):
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repos
    captured = []
    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_fetch_status": "OK", "balance_parse_status": "OK", "raw_output1_count": 0, "total_pvs": 1000}
    monkeypatch.setattr(repos, "save_position_snapshot", lambda positions, **kwargs: captured.append((list(positions), kwargs)) or 0)
    result = reconcile.reconcile_positions(provider=Provider(), trade_date="2026-07-16")
    assert result["status"] == "OK"
    assert captured and captured[0][0] == []
    assert captured[0][1]["authoritative_positions"] is True
    assert captured[0][1]["preserve_previous_positions"] is False


def test_journal_replay_rejects_regressed_broker_evidence(monkeypatch):
    import trader.us.execution.order_journal as journal
    import trader.us.db.repos as repos
    events = [
        {"event_type":"BROKER_SUBMIT_STARTED","trade_date":"2026-07-16","client_order_key":"K","symbol":"AMD","side":"SELL","qty":10},
        {"event_type":"BROKER_ACK_RECEIVED","trade_date":"2026-07-16","client_order_key":"K","symbol":"AMD","side":"SELL","qty":10,"broker_order_no":"O1"},
    ]
    monkeypatch.setattr(journal, "load_order_events", lambda *a, **k: events)
    monkeypatch.setattr(journal, "append_order_event", lambda *a, **k: {})
    monkeypatch.setattr(repos, "save_order_ack", lambda *a, **k: True)
    called = []
    monkeypatch.setattr(repos, "mark_order_filled_by_reconcile", lambda **k: called.append(k) or {"status":"OK"})
    class Provider:
        def get_balance(self, force_refresh=False): return {"positions": []}
        def get_today_orders(self, trade_date=None): return []
        def get_fills_by_order_no(self, **kwargs):
            return {"filled_qty":7,"cumulative_filled_qty":7,"avg_price":100,"symbol":"AMD","side":"SELL","status":"EVIDENCE_QUANTITY_REGRESSION"}
    result = journal.replay_order_journal("2026-07-16", provider=Provider())
    assert result["status"] == "ERROR"
    assert result["failed_count"] == 1
    assert called == []


def test_close_calls_final_ack_reconcile():
    import trader.us.runner.trade_close_runner as close
    src = inspect.getsource(close)
    assert "reconcile_ack_orders_with_balance(" in src
    assert '"ack_reconcile_result"' in src


def test_repair_overflow_guard():
    from scripts.repair_us_trade_integrity import validate_repair_cumulative
    assert validate_repair_cumulative(qty=11, requested_qty=10)["status"] == "EVIDENCE_QUANTITY_OVERFLOW"
    assert validate_repair_cumulative(qty=10, requested_qty=10)["status"] == "OK"
