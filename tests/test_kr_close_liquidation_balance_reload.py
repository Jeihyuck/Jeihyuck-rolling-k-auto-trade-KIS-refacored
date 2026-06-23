import logging

from trader import pb1_runner


class FakeKis:
    def __init__(self):
        self.get_balance_calls = 0
        self.sell_calls = []

    def get_balance(self):
        self.get_balance_calls += 1
        return {"output1": [{"code": "000660", "qty": 1}]}

    def sell_stock_market(self, code, qty):
        self.sell_calls.append({"code": code, "qty": qty})
        return {"rt_cd": "0", "msg_cd": "OK", "msg1": "accepted"}


def test_close_liquidation_reloads_balance_when_snapshot_missing(caplog):
    caplog.set_level(logging.INFO)
    fake_kis = FakeKis()
    results = pb1_runner.run_close_liquidation_from_kis_holdings(
        kis_client=fake_kis,
        env="practice",
        holdings=None,
        dry_run=False,
    )
    assert fake_kis.get_balance_calls == 1
    assert fake_kis.sell_calls == [{"code": "000660", "qty": 1}]
    assert results[0]["result"] == "ACCEPTED"
    assert "[KR_CLOSE][LIQUIDATION][BALANCE_RELOAD][START] source=kis_client" in caplog.text
    assert "[KR_CLOSE][LIQUIDATION][BALANCE_RELOAD][DONE] holdings=1" in caplog.text
    assert "NO_KIS_HOLDINGS" not in caplog.text


def test_close_liquidation_empty_snapshot_is_passed_as_reload(caplog):
    caplog.set_level(logging.INFO)
    fake_kis = FakeKis()
    holdings_for_liquidation = None
    balance_snapshot_raw = {"output1": []}
    snapshot_holdings = balance_snapshot_raw.get("output1") or []
    if snapshot_holdings:
        holdings_for_liquidation = list(snapshot_holdings)
    results = pb1_runner.run_close_liquidation_from_kis_holdings(
        kis_client=fake_kis,
        env="practice",
        holdings=holdings_for_liquidation,
        dry_run=False,
    )
    assert fake_kis.get_balance_calls == 1
    assert fake_kis.sell_calls == [{"code": "000660", "qty": 1}]
    assert results[0]["result"] == "ACCEPTED"
    assert "NO_KIS_HOLDINGS" not in caplog.text
