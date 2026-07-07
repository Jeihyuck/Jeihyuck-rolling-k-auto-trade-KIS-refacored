from trader.pb1_engine import normalize_kr_order_price
from trader import kis_wrapper
import trader.config as trader_config


def test_buy_price_181905_rounds_up_to_182000():
    normalized, tick = normalize_kr_order_price(181_905, side="BUY")
    assert tick == 500
    assert normalized == 182_000
    assert normalized % tick == 0


def test_sell_price_181905_rounds_down_to_181500():
    normalized, tick = normalize_kr_order_price(181_905, side="SELL")
    assert tick == 500
    assert normalized == 181_500
    assert normalized % tick == 0


def test_buy_stock_limit_payload_never_sends_181905(monkeypatch):
    sent = {}

    class Gate:
        allow_live_gate = True
        force_block_live = False
        reason = "ok"
        window = "day"
        class Now:
            @staticmethod
            def isoformat():
                return "2026-07-07T00:00:00+09:00"
        now_kst = Now()

    class Resp:
        status_code = 200
        text = '{"rt_cd":"0"}'
        def json(self):
            return {"rt_cd": "0", "msg_cd": "0", "msg1": "OK", "output": {"ODNO": "1"}}

    client = object.__new__(kis_wrapper.KisAPI)
    client.CANO = "12345678"
    client.ACNT_PRDT_CD = "01"
    client.env = "practice"
    client._recent_sells_lock = None

    monkeypatch.setattr(trader_config, "get_live_gate_status_fresh", lambda reason: Gate())
    monkeypatch.setattr(kis_wrapper, "_assert_orders_allowed", lambda source: None)
    monkeypatch.setattr(kis_wrapper, "_order_block_reason", lambda now: None)
    monkeypatch.setattr(client, "_create_hashkey", lambda body: "hash")
    monkeypatch.setattr(client, "_headers", lambda tr_id, hk: {})
    monkeypatch.setattr(client, "_wait_before_order_submit", lambda: None)
    monkeypatch.setattr(kis_wrapper, "_pick_tr", lambda env, kind: ["TTTC0802U"])
    monkeypatch.setattr(kis_wrapper, "append_fill", lambda **kwargs: None)

    def fake_safe_request(method, url, **kwargs):
        body = kis_wrapper.json.loads(kwargs["data"].decode("utf-8"))
        sent.update(body)
        return Resp()

    monkeypatch.setattr(client, "_safe_request", fake_safe_request)

    resp = client.buy_stock_limit("033780", 1, 181_905)

    assert resp["rt_cd"] == "0"
    assert int(sent["ORD_UNPR"]) == 182_000
    assert int(sent["ORD_UNPR"]) != 181_905
    assert isinstance(int(sent["ORD_UNPR"]), int)
    assert int(sent["ORD_UNPR"]) % 500 == 0


def test_kis_tick_size_error_is_permanent_not_temporary():
    body = {"rt_cd": "1", "msg_cd": "40030000", "msg1": "호가단위 오류"}
    assert kis_wrapper._is_kis_tick_size_error_body(body)
