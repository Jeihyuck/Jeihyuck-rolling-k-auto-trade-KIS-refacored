from tests.kr.test_kr_sell_no_sellable_session_block import FakeKis, _make_engine


class QuoteKis(FakeKis):
    def __init__(self):
        super().__init__()
        self.quote_calls = 0

    def get_price_snapshot(self, code, market="J"):
        self.quote_calls += 1
        return {"code": code, "price": 10000}


def test_exit_entry_and_presubmit_share_one_tick_quote():
    kis = QuoteKis()
    engine, _ = _make_engine(kis=kis)
    exit_quote = engine._get_price_snapshot_cached("005930")
    entry_quote = engine._get_price_snapshot_cached("005930")
    presubmit_quote = engine._get_price_snapshot_cached("005930")
    assert exit_quote == entry_quote == presubmit_quote
    assert kis.quote_calls == 1
