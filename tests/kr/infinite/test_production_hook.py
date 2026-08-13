from pathlib import Path


def test_pb1_runner_calls_sleeve_once_at_authoritative_balance_boundary():
    source=Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert source.count("run_production_sleeve(")==1
    assert "balance_snapshot=balance_snapshot_raw, now=now" in source
    assert "run_isolated(lambda: run_production_sleeve" in source


def test_all_pb1_final_order_boundaries_guard_122630():
    source=Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    for path in ("path=entry", "path=close_entry", "path=add_on", "path=force_buy", "path=exit"):
        assert path in source
