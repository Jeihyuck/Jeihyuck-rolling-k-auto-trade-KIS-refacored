import json
from pathlib import Path

from trader.pb1_engine import PB1Engine, CandidateFeature
from trader.window_router import WindowDecision


class DummyKis:
    def __init__(self):
        self.buy_called = False
        self.shadow_checks = 0

    def validate_buy(self, code, qty, price):
        return True, "ok"

    def validate_sell(self, code, qty, price):
        return True, "ok"

    def check_orderable(self, **_kwargs):
        self.shadow_checks += 1
        return {"ok": True, "reason": "ok", "rt_cd": "0"}

    def buy_stock_market(self, *_args, **_kwargs):
        self.buy_called = True
        raise AssertionError("buy_stock_market should not be called in shadow mode")


def test_shadow_executor_never_places_orders(monkeypatch, tmp_path):
    ledger_dir = tmp_path / "ledger"
    monkeypatch.setattr("trader.pb1_engine.LEDGER_BASE_DIR", ledger_dir)
    monkeypatch.setattr("trader.pb1_engine.persist_run_files", lambda *args, **kwargs: None)

    window = WindowDecision(name="afternoon", phase="entry")
    dummy_kis = DummyKis()

    engine = PB1Engine(
        kis=dummy_kis,
        worktree_dir=tmp_path,
        window=window,
        phase_override="auto",
        dry_run=False,
        env="shadow",
        run_id="test-run",
        order_mode="shadow",
        diag_level=2,
    )

    cf = CandidateFeature(
        code="123456",
        market="KOSPI",
        features={"close": 1000},
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        client_order_key="test-key",
        planned_qty=10,
    )

    paths = engine._place_entry(cf)
    ack_file = ledger_dir / "orders_ack" / engine._today / f"run_{engine.run_id}.jsonl"
    assert ack_file.exists()
    content = ack_file.read_text(encoding="utf-8").strip().splitlines()
    last_row = json.loads(content[-1])
    assert last_row["payload"]["mode"] == "shadow"
    assert last_row["ok"] is True
    assert dummy_kis.buy_called is False
    shadow_file = ledger_dir / "orders_shadow_check" / engine._today / f"run_{engine.run_id}.jsonl"
    assert shadow_file.exists()
    shadow_rows = shadow_file.read_text(encoding="utf-8").strip().splitlines()
    assert json.loads(shadow_rows[-1])["ok"] is True
    assert dummy_kis.shadow_checks == 1
