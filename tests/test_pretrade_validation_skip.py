from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


class DummyLedgerRepo:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def append_event(self, **payload) -> None:
        self.events.append(payload)


class FakeKis:
    def get_quote_safe(self, code: str, diag_mode: bool = False):
        return {"rt_cd": "1"}


def test_pretrade_validation_skips_untradeable():
    ledger_repo = DummyLedgerRepo()
    engine = PB1Engine(
        universe_repo=object(),
        orders_repo=object(),
        fills_repo=object(),
        positions_repo=object(),
        ledger_repo=ledger_repo,
        kis=FakeKis(),
        window=WindowDecision(name="day", phase="trade"),
        window_label="day",
        phase="trade",
        dry_run=True,
        env="practice",
        run_id="run-1",
    )

    ok = engine._pretrade_check(
        code="218410",
        market="KOSDAQ",
        mode=1,
        side="BUY",
        qty=10,
        price=1000.0,
        client_order_key="test",
        stage="PB1-CLOSE",
    )

    assert ok is False
    assert ledger_repo.events
    assert ledger_repo.events[0]["event_type"] == "ORDER_SKIP"
