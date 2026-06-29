from __future__ import annotations

from datetime import datetime
from pathlib import Path

from trader.kr.runner.session_policy import KST, kr_prep_schedule_guard


def test_kr_prep_shell_and_python_window_start_0630():
    assert kr_prep_schedule_guard(datetime(2026, 6, 29, 6, 29, tzinfo=KST), allow_outside=False).action == "BLOCK"
    assert kr_prep_schedule_guard(datetime(2026, 6, 29, 6, 30, tzinfo=KST), allow_outside=False).action == "PROCEED"
    script = Path("scripts/wsl/run-kr-prep.sh").read_text(encoding="utf-8")
    assert '"$NOW_HM" < "06:30"' in script
    assert "action=shell_block_no_python" in script


def test_kr_prep_outside_window_does_not_quarantine_artifacts(monkeypatch):
    import trader.kr.runner.trade_session_runner as runner

    called = {"quarantine": 0}

    def fake_quarantine(**_kwargs):
        called["quarantine"] += 1

    monkeypatch.setattr(runner, "quarantine_stale_kr_artifacts", fake_quarantine)
    monkeypatch.setattr(
        runner,
        "kr_prep_schedule_guard",
        lambda: type("D", (), {"action": "BLOCK", "reason": "OUTSIDE_PREP_WINDOW"})(),
    )
    result = runner._run_prep("practice")
    assert result["reason"] == "OUTSIDE_PREP_WINDOW"
    assert result["exit_code"] == 2
    assert called["quarantine"] == 0


def test_marketcap_provider_includes_fid_prc_cls_code():
    from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider

    captured = {}

    class Resp:
        def json(self):
            return {"output": [{"stck_shrn_iscd": "005930"}]}

    class Limiter:
        def wait(self, _key):
            pass

    class FakeKis:
        env = "practice"
        _limiter = Limiter()

        def _headers(self, _tr_id):
            return {}

        def _safe_request(self, *args, **kwargs):
            captured.update(kwargs.get("params") or {})
            return Resp()

    provider = KISMarketcapTopProvider(kis=FakeKis())
    assert provider.get_marketcap_top("KOSPI", 30) == ["005930"]
    assert "FID_PRC_CLS_CODE" in captured
    assert captured["FID_COND_MRKT_DIV_CODE"] == "J"
    assert captured["FID_INPUT_CNT_1"] == "30"


def test_balance_fail_soft_engine_does_not_requery_kis(monkeypatch):
    from trader.pb1_engine import PB1Engine

    class FakeKis:
        def get_balance_cached(self, **_kwargs):
            raise AssertionError("should not requery KIS balance")

    monkeypatch.setenv("PB1_BALANCE_FAIL_SOFT", "1")
    engine = PB1Engine(universe_repo=None, orders_repo=None, fills_repo=None, positions_repo=None, ledger_repo=None, kis=FakeKis(), dry_run=True, env="practice", run_id="test", entry_block_reason="balance_unknown")
    snap = engine._fetch_holdings_snapshot()
    assert snap["_fail_soft"] is True
    assert snap["output1"] == []


def test_kr_entry_uses_final30_not_universe_195_static_contract():
    text = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert "canonical_codes = [str((row or {}).get" not in text
    assert "source=final30 has_rank_final30=%s canonical_order_match=%s final30_rows=%s universe_rows=%s scan_rows=%s" in text
    assert "FINAL30_CONTRACT_INVALID" in text


def test_pm_sell_ack_preserved_when_entry_raises_static_contract():
    text = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert "[PB1][PARTIAL_OK][EXIT_DONE_ENTRY_FAILED]" in text
    assert "PARTIAL_OK_EXIT_DONE_ENTRY_FAILED" in text
    assert '"sell_orders_ack": sell_ack' in text
