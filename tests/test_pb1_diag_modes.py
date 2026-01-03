from trader.pb1_engine import PB1Engine, CandidateFeature
from trader.window_router import WindowDecision
import trader.pb1_runner as pb1_runner


def test_resolve_order_mode_prefers_shadow_for_diag_level_2() -> None:
    assert pb1_runner._resolve_order_mode(1, False, "run") == "intent_only"
    assert pb1_runner._resolve_order_mode(2, False, "run") == "intent_only"
    assert pb1_runner._resolve_order_mode(2, True, "run") == "shadow"


class DummyKis:
    def __init__(self):
        self.shadow_checks = 0

    def check_orderable(self, **_kwargs):
        self.shadow_checks += 1
        return {"ok": True, "reason": "ok", "rt_cd": "0"}


def test_diag_level1_intent_only_does_not_call_kis(monkeypatch, tmp_path):
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
        dry_run=True,
        env="paper",
        run_id="diag1",
        order_mode="intent_only",
        diag_level=1,
    )

    cf = CandidateFeature(
        code="123456",
        market="KOSPI",
        features={"close": 1000},
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        client_order_key="intent-key",
        planned_qty=10,
    )

    engine._place_entry(cf)
    assert dummy_kis.shadow_checks == 0


def test_diag_level2_shadow_adds_shadow_check(monkeypatch, tmp_path):
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
        run_id="diag2",
        order_mode="shadow",
        diag_level=2,
    )

    cf = CandidateFeature(
        code="654321",
        market="KOSPI",
        features={"close": 2000},
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        client_order_key="shadow-key",
        planned_qty=5,
    )

    engine._place_entry(cf)
    assert dummy_kis.shadow_checks == 1
    shadow_file = ledger_dir / "orders_shadow_check" / engine._today / f"run_{engine.run_id}.jsonl"
    assert shadow_file.exists()
