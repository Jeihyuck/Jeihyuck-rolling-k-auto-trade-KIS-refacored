"""2026-10-12 KR incident: actual KIS 091 request and PREP-failure SELL liveness.

All tests are offline. Never talk to KIS or write to the live account/database.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider


class _Response:
    def json(self):
        return {
            "rt_cd": "0",
            "output": [
                {"mksc_shrn_iscd": "005930", "hts_kor_isnm": "Samsung"},
                {"mksc_shrn_iscd": "000660", "hts_kor_isnm": "SK Hynix"},
            ],
        }


class _FakeKis:
    env = "practice"

    def __init__(self):
        self.params = []

        class Limiter:
            def wait(self, key):
                assert key == "marketcap-top"

        self._limiter = Limiter()

    def _headers(self, tr_id):
        assert tr_id == "FHPST01740000"
        return {"tr_id": tr_id}

    def _safe_request(self, method, url, *, headers, params, timeout):
        assert method == "GET"
        assert url.endswith("/uapi/domestic-stock/v1/ranking/market-cap")
        self.params.append(dict(params))
        return _Response()


def test_kis_marketcap_correct_091_tr_sector_and_fields_for_both_readers(monkeypatch):
    # Oct12 error was caused by a fluctuation TR ID sent to the market-cap URL.
    monkeypatch.delenv("KIS_MKTCAP_SORT_CODE", raising=False)
    fake = _FakeKis()
    provider = KISMarketcapTopProvider(kis=fake, env="practice")
    assert provider.get_marketcap_top("KOSPI", 150) == ["005930", "000660"]
    assert provider.get_marketcap_top_with_meta("KOSDAQ", 150)[0]["code"] == "005930"

    assert len(fake.params) == 2
    for params, market in zip(fake.params, ("0001", "1001")):
        assert "FID_RANK_SORT_CLS_CODE" not in params
        assert params["FID_COND_SCR_DIV_CODE"] == "20174"
        assert params["FID_COND_MRKT_DIV_CODE"] == "J"
        assert params["FID_INPUT_ISCD"] == market
        assert params["FID_INPUT_ISCD"] == "0000"
        assert "FID_INPUT_CNT_1" not in params
        assert "FID_PRC_CLS_CODE" not in params


def test_kis_marketcap_pagination_reads_exact_150_and_preserves_market(monkeypatch):
    class PageResponse:
        def __init__(self, start, finish, continuation):
            self.headers = {"tr_cont": continuation}
            self.start, self.finish = start, finish

        def json(self):
            return {
                "rt_cd": "0",
                "output": [
                    {"mksc_shrn_iscd": f"{i:06d}", "hts_kor_isnm": f"Stock {i}"}
                    for i in range(self.start, self.finish)
                ],
            }

    class PageKis(_FakeKis):
        def __init__(self):
            super().__init__()
            self.count = 0

        def _safe_request(self, method, url, *, headers, params, timeout):
            assert params["FID_INPUT_ISCD"] == "1001"
            assert params["FID_COND_MRKT_DIV_CODE"] == "J"
            assert headers["tr_id"] == "FHPST01740000"
            if self.count:
                assert headers.get("tr_cont") == "N"
            else:
                assert "tr_cont" not in headers
            self.count += 1
            return PageResponse(1 + (self.count-1)*60, 1 + min(self.count*60, 150),
                                "M" if self.count < 3 else "")

    provider = KISMarketcapTopProvider(kis=PageKis())
    rows = provider.get_marketcap_top_with_meta("KOSDAQ", 150)
    assert len(rows) == 150
    assert len({item["code"] for item in rows}) == 150
    assert provider.kis.count == 3


def test_marketcap_nonzero_broker_response_rejected_even_if_rows_exist():
    class Rejected(_FakeKis):
        def _safe_request(self, method, url, *, headers, params, timeout):
            class Response:
                def json(self):
                    return {"rt_cd": "2", "msg1": "BROKER_REJECT", "output": [{"mksc_shrn_iscd": "005930"}]}
            return Response()

    provider = KISMarketcapTopProvider(kis=Rejected())
    assert provider.get_marketcap_top("KOSPI", 150) == []
    with pytest.raises(RuntimeError, match="broker_reject"):
        provider.get_marketcap_top_with_meta("KOSPI", 150)


def test_prep_failure_sell_only_explicitly_disables_buy_and_force_liquidation(monkeypatch):
    from trader.kr.runner.trade_session_runner import _arm_prep_failure_protective_exit

    monkeypatch.delenv("KR_PREP_FAILURE_PROTECT_SELL_ENABLED", raising=False)
    # The helper mutates process env exactly as a live isolated WSL session
    # would; restore every flag to avoid leaking a SELL-only mode to other tests.
    for key in (
        "PB1_ENTRY_ENABLED", "ENTRY_ENABLED", "ALLOW_NEW_BUY",
        "PB1_EXIT_ENABLED", "PB1_EXIT_ONLY_MODE",
        "PB1_PREP_FAILED_SELL_ONLY", "PB1_CLOSE_LIQUIDATION_ENABLED",
        "FORCE_PB1_PHASE", "FORCE_MARKET_WINDOW",
        "FORCE_ENTRY_DISABLED_REASON", "PB1_FORCE_ENTRY_WINDOW_OVERRIDE",
        "PB1_DIAG_FULL_EXEC",
    ):
        monkeypatch.setenv(key, os.getenv(key, ""))
    assert _arm_prep_failure_protective_exit(
        session="afternoon",
        precheck={"status": "FAIL", "reason": "CANONICAL_PREP_ARTIFACT_INVALID"},
    )
    assert os.environ["PB1_ENTRY_ENABLED"] == "0"
    assert os.environ["ENTRY_ENABLED"] == "0"
    assert os.environ["ALLOW_NEW_BUY"] == "0"
    assert os.environ["PB1_EXIT_ENABLED"] == "1"
    assert os.environ["PB1_EXIT_ONLY_MODE"] == "1"
    assert os.environ["PB1_PREP_FAILED_SELL_ONLY"] == "1"
    assert os.environ["PB1_CLOSE_LIQUIDATION_ENABLED"] == "0"
    assert os.environ["FORCE_PB1_PHASE"] == "exit"
    assert os.environ["FORCE_MARKET_WINDOW"] == "day"
    assert os.environ["PB1_FORCE_ENTRY_WINDOW_OVERRIDE"] == "0"
    assert os.environ["PB1_DIAG_FULL_EXEC"] == "0"


@pytest.mark.parametrize("session,reason", [
    ("close", "CANONICAL_PREP_ARTIFACT_INVALID"),
    ("am", "KIS_BALANCE_UNAVAILABLE"),
    ("afternoon", "LOCK_UNAVAILABLE"),
])
def test_prep_failure_sell_only_does_not_bypass_other_failures(monkeypatch, session, reason):
    from trader.kr.runner.trade_session_runner import _arm_prep_failure_protective_exit
    assert not _arm_prep_failure_protective_exit(
        session=session, precheck={"status": "FAIL", "reason": reason},
    )


def test_prep_failure_sell_only_opt_out_is_fail_closed(monkeypatch):
    from trader.kr.runner.trade_session_runner import _arm_prep_failure_protective_exit
    monkeypatch.setenv("KR_PREP_FAILURE_PROTECT_SELL_ENABLED", "0")
    assert not _arm_prep_failure_protective_exit(
        session="am",
        precheck={"status": "FAIL", "reason": "CANONICAL_PREP_ARTIFACT_INVALID"},
    )


def _engine_with_sell_gate():
    from trader.pb1_engine import PB1Engine

    engine = PB1Engine.__new__(PB1Engine)
    engine.trading_day = True
    engine.order_allowed = False
    engine.balance_fail_soft_active = False
    engine.market_window_name = "morning"
    engine.force_entry_window_override = False
    engine.session_recovery_continue = False
    engine.force_block_live = False
    engine.intended_live = True
    engine.strategy_mode = "LIVE"
    engine.phase = "exit"
    engine.dry_run = False
    return engine


def test_prep_failed_sell_only_allows_sell_gate_but_never_buy(monkeypatch):
    for key, value in {
        "PB1_PREP_FAILED_SELL_ONLY": "1", "PB1_EXIT_ONLY_MODE": "1",
        "PB1_ENTRY_ENABLED": "0", "PB1_SESSION": "am",
        "KR_ORDER_ARMED": "1", "LIVE_TRADING_ENABLED": "1",
        "DISABLE_LIVE_TRADING": "0",
    }.items():
        monkeypatch.setenv(key, value)
    engine = _engine_with_sell_gate()
    assert engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT") == []
    assert "order_blocked" in engine._order_precheck_gate_reasons(side="BUY", stage="PB1-ENTRY")

    monkeypatch.setenv("KR_ORDER_ARMED", "0")
    assert "order_blocked" in engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT")
    monkeypatch.setenv("KR_ORDER_ARMED", "1")
    engine.dry_run = True
    assert "order_blocked" in engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT")
    engine.dry_run = False
    engine.force_block_live = True
    assert "order_blocked" in engine._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT")


def test_exit_only_recovery_does_not_repurpose_close_policy_or_touch_kr_infinite(monkeypatch):
    from trader import pb1_runner

    monkeypatch.setenv("PB1_EXIT_ONLY_MODE", "1")
    ctx = pb1_runner._load_universe_context(
        engine=object(), as_of="2026-10-12", env="practice", strategy="best_k_meta",
    )
    assert ctx.is_empty
    assert ctx.meta.get("exit_only") is True
    source = Path(pb1_runner.__file__).read_text(encoding="utf-8")
    assert "if getattr(universe_ctx, \"is_empty\", False) and not protective_exit_only:" in source
    assert "and not protective_exit_only):" in source
    assert "not protective_exit_only" in source
    # Never invoke entry-only Final30 injection if PREP has definitively failed.
    assert "if not protective_exit_only and (" in source
    # Existing PB1 owner guard excludes the infinite sleeve from generic exits.
    assert 'str(holding.code).zfill(6) != "122630"' in Path(
        "trader/pb1_engine.py"
    ).read_text(encoding="utf-8")
