from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import trader.kr.artifacts as artifacts
import trader.watchlist_builder as wb


def _rows(n=30):
    out = []
    for i in range(1, n + 1):
        score = float(100 - i)
        out.append({
            "code": f"{i:06d}",
            "name": f"N{i}",
            "rank": i,
            "rank_final30": i,
            "as_of": "2026-06-25",
            "close": 100.0 + i,
            "last_close": 100.0 + i,
            "ma20": 100.0,
            "ma50": 95.0,
            "ma150": 90.0,
            "volume": 1000,
            "volume_avg20": 1000,
            "atr_pct": 2.0,
            "rs_percentile": 90.0,
            "vcp_score": 80.0,
            "score_final": score,
            "final_score": score,
            "score": score,
            "tech_score": score,
            "breakout_score": 1.0,
            "pullback_score": 1.0,
            "momentum_score": 1.0,
            "entry_style_selected": "PULLBACK",
            "meta": {"rank_final30": i},
        })
    return out


class FakeBuilder:
    last = None

    def __init__(self, *args, **kwargs):
        self.flow_provider = kwargs.get("flow_provider")
        self.last_bundle = {"universe_scored": _rows(40), "pool120": _rows(40), "top50": _rows(40)}
        FakeBuilder.last = self

    def build(self, *, members, as_of):
        assert self.flow_provider is None
        self.last_bundle["final30"] = _rows(30)
        self.last_bundle["final30_scored"] = _rows(30)
        logging.getLogger(wb.__name__).info("[WATCHLIST][BUILD][DONE] as_of=%s pool120=40 top50=40 final30=30", as_of)
        return _rows(30)


class FakeRepo:
    calls = []

    def __init__(self, engine):
        pass

    def save_watchlist(self, **kwargs):
        FakeRepo.calls.append("save_watchlist")
        raise RuntimeError("(EDBHANDLEREXITED) connection to database closed during rollback failure")

    def load_watchlist_scored(self, **kwargs):
        raise RuntimeError("(EDBHANDLEREXITED) connection to database closed")


def _patch_common(monkeypatch, tmp_path):
    FakeRepo.calls = []
    monkeypatch.setattr(wb, "WatchlistBuilder", FakeBuilder)
    monkeypatch.setattr(wb, "WatchlistRepo", FakeRepo)
    monkeypatch.setattr(wb, "_enrich_watchlist_rows", lambda **kwargs: kwargs["rows"])
    monkeypatch.setattr(wb, "_prepare_final30_scored_rows_for_save", lambda rows, **kwargs: (rows, 0, {}))
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)


def test_core_artifact_written_before_repo_save_watchlist(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path)
    order = []
    real_publish = artifacts.publish_kr_prep_artifacts_core_fast

    def wrapped_publish(**kwargs):
        order.append("publish")
        return real_publish(**kwargs)

    monkeypatch.setattr(artifacts, "publish_kr_prep_artifacts_core_fast", wrapped_publish)
    orig_save = FakeRepo.save_watchlist

    def save(self, **kwargs):
        order.append("save")
        return orig_save(self, **kwargs)

    monkeypatch.setattr(FakeRepo, "save_watchlist", save)
    _, bundle = wb.build_and_save_watchlist(
        engine=SimpleNamespace(), env="practice", strategy="pb1_watchlist", as_of=date(2026, 6, 25),
        members=_rows(40), ohlcv_provider=None, minervini_config={}, force_rebuild=True, use_cache=False,
        return_bundle=True, save_intermediate_bundle=False, core_artifact_trade_date=date(2026, 6, 26),
        core_artifact_expected_as_of=date(2026, 6, 25), core_artifact_env="practice", core_artifact_db_exact_rows=30,
    )
    assert order[:2] == ["publish", "save"]
    assert bundle["core_artifact_saved"] is True
    assert bundle["db_save_failed_after_core"] is True


def test_artifact_files_exist_even_when_repo_save_fails(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path)
    wb.build_and_save_watchlist(
        engine=SimpleNamespace(), env="practice", strategy="pb1_watchlist", as_of=date(2026, 6, 25),
        members=_rows(40), ohlcv_provider=None, minervini_config={}, force_rebuild=True, use_cache=False,
        return_bundle=True, save_intermediate_bundle=False, core_artifact_trade_date=date(2026, 6, 26),
        core_artifact_expected_as_of=date(2026, 6, 25), core_artifact_env="practice", core_artifact_db_exact_rows=30,
    )
    for rel in [
        "runtime/kr/watchlist/2026-06-26/prep_contract.json",
        "runtime/kr/watchlist/2026-06-26/final30_scored.json",
        "runtime/kr/watchlist/2026-06-26/prep_done.json",
        "runtime/kr/prep_status/2026-06-26/prep_status.json",
        "signals/kr/latest_prep_contract.json",
        "signals/kr/latest_final30_scored.json",
        "signals/kr/latest_prep_status.json",
    ]:
        assert (tmp_path / rel).exists(), rel


def test_flow_enabled_runs_only_after_core_done(monkeypatch, tmp_path, caplog):
    _patch_common(monkeypatch, tmp_path)

    def enrich(**kwargs):
        logging.getLogger(wb.__name__).info("[FLOW][TRY] provider=mock")
        return kwargs["rows"]

    monkeypatch.setattr(wb, "_enrich_watchlist_rows", enrich)
    caplog.set_level(logging.INFO)
    wb.build_and_save_watchlist(
        engine=SimpleNamespace(), env="practice", strategy="pb1_watchlist", as_of=date(2026, 6, 25),
        members=_rows(40), ohlcv_provider=None, minervini_config={}, force_rebuild=True, use_cache=False,
        flow_provider=lambda *a: None, return_bundle=True, save_intermediate_bundle=False,
        core_artifact_trade_date=date(2026, 6, 26), core_artifact_expected_as_of=date(2026, 6, 25), core_artifact_env="practice",
    )
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert text.index("[KR_PREP][CORE_DONE]") < text.index("[FLOW][TRY]")


def test_flow_disabled_no_flow_try(monkeypatch, tmp_path, caplog):
    _patch_common(monkeypatch, tmp_path)
    caplog.set_level(logging.INFO)
    wb.build_and_save_watchlist(
        engine=SimpleNamespace(), env="practice", strategy="pb1_watchlist", as_of=date(2026, 6, 25),
        members=_rows(40), ohlcv_provider=None, minervini_config={}, force_rebuild=True, use_cache=False,
        flow_provider=None, return_bundle=True, save_intermediate_bundle=False,
        core_artifact_trade_date=date(2026, 6, 26), core_artifact_expected_as_of=date(2026, 6, 25), core_artifact_env="practice",
    )
    assert "[FLOW][TRY]" not in "\n".join(r.getMessage() for r in caplog.records)
