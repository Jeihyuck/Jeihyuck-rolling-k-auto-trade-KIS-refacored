from __future__ import annotations

import logging

import pandas as pd
import trader.pb1_engine as pb1_engine_module

from trader.pb1_engine import PB1Engine


def _make_engine() -> PB1Engine:
    engine = PB1Engine.__new__(PB1Engine)
    engine._precomputed_final30_map = {}
    engine._precomputed_derived_map = {}
    engine._precomputed_universe_map = {}
    engine._data_metrics = {"precomputed_hits": 0}
    engine.min_candles = 20
    engine.require_volume = False
    engine._load_universe = lambda: [{"code": "005930", "market": "KOSPI"}]
    engine._fetch_daily = lambda code, days=60: (pd.DataFrame(), {})
    return engine


def test_precomputed_final30_row_becomes_data_ok_candidate(caplog) -> None:
    engine = _make_engine()
    engine._precomputed_final30_map["005930"] = {
        "code": "005930",
        "close": 71000.0,
        "ma20": 68000.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "breakout_score": 81.0,
        "pullback_score": 64.0,
        "momentum_score": 77.0,
        "rs_percentile": 92.0,
        "vcp_score": 74.0,
        "trend_score": 88.0,
        "entry_style_selected": "breakout",
        "pullback_pct": 8.5,
        "atr_pct": 0.034,
    }

    with caplog.at_level(logging.INFO):
        candidates = engine._compute_candidates_from_codes(["005930"])

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.features["data_ok"] is True
    assert candidate.features["precomputed_usable_row"] is True
    assert candidate.features["source_mode"] == "precomputed"
    assert candidate.features["ma20"] == 68000.0
    assert candidate.features["ma50"] == 65000.0
    assert candidate.features["ma150"] == 60000.0
    assert "[PB1][PRECOMPUTED][USABLE_CHECK] code=005930" in caplog.text
    assert "usable=1" in caplog.text
    assert "[PB1][FEATURE_SOURCE] code=005930" in caplog.text
    assert "ma=True" in caplog.text


def test_precomputed_mapping_uses_alias_and_preserves_richer_final30_values() -> None:
    engine = _make_engine()
    engine._precomputed_final30_map["005930"] = {
        "code": "005930",
        "close": 71000.0,
        "ma20": 68000.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "breakout_score": 81.0,
        "pullback_score": 64.0,
        "momentum_score": 77.0,
        "rs_pctile": 91.0,
        "vcp_score": 74.0,
        "trend_score": 88.0,
        "entry_style_selected": "breakout",
        "pullback_pct": 8.5,
        "atr_pct": 0.034,
    }
    engine._precomputed_universe_map["005930"] = {
        "code": "005930",
        "ma20": None,
        "ma50": "",
        "ma150": float("nan"),
        "rs_percentile": None,
        "vcp_score": None,
    }

    mapped, checks, reasons, usable_precomputed_row, precomputed_data_ok = engine._map_precomputed_candidate_row("005930")

    assert mapped["rs_percentile"] == 91.0
    assert mapped["ma20"] == 68000.0
    assert mapped["ma50"] == 65000.0
    assert mapped["ma150"] == 60000.0
    assert checks["has_rs_percentile"] == 1
    assert reasons == []
    assert usable_precomputed_row is True
    assert precomputed_data_ok is True


def test_data_ok_allows_required_precomputed_fields_even_when_usable_extra_fields_missing() -> None:
    engine = _make_engine()
    engine._precomputed_final30_map["005930"] = {
        "code": "005930",
        "close": 71000.0,
        "ma20": 68000.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "breakout_score": 81.0,
        "pullback_score": 64.0,
        "momentum_score": 77.0,
        "rs_percentile": 92.0,
        "vcp_score": 74.0,
        "trend_score": 88.0,
    }

    mapped, checks, reasons, usable_precomputed_row, precomputed_data_ok = engine._map_precomputed_candidate_row("005930")

    assert mapped["close"] == 71000.0
    assert checks["has_close"] == 1
    assert usable_precomputed_row is False
    assert precomputed_data_ok is True
    assert "missing_entry_style_selected" in reasons
    assert "missing_price_context" in reasons

    candidates = engine._compute_candidates_from_codes(["005930"])

    assert len(candidates) == 1
    assert candidates[0].features["data_ok"] is True
    assert candidates[0].features["precomputed_usable_row"] is False


def test_precomputed_ratio_pullback_is_normalized_for_pb1_setup() -> None:
    engine = _make_engine()
    engine._precomputed_final30_map["005930"] = {
        "code": "005930",
        "close": 71000.0,
        "ma20": 68000.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "breakout_score": 81.0,
        "pullback_score": 64.0,
        "momentum_score": 77.0,
        "rs_percentile": 92.0,
        "vcp_score": 74.0,
        "trend_score": 88.0,
        "entry_style_selected": "pullback",
        "pullback_pct": 0.085,
        "atr_pct": 0.034,
        "vol_contraction": 0.8,
        "volu_contraction": 0.8,
        "ma20_slope": 12.0,
    }

    mapped, _, _, usable_precomputed_row, _ = engine._map_precomputed_candidate_row("005930")
    candidates = engine._compute_candidates_from_codes(["005930"])

    assert mapped["pullback_pct"] == 8.5
    assert usable_precomputed_row is True
    assert len(candidates) == 1
    assert candidates[0].features["pullback_pct"] == 8.5
    assert candidates[0].features["setup_loose_ok"] is True


def test_short_ohlcv_fill_overwrites_zero_like_precomputed_context(monkeypatch) -> None:
    engine = _make_engine()
    engine._precomputed_final30_map["005930"] = {
        "code": "005930",
        "close": 71000.0,
        "ma20": 0.0,
        "ma50": 65000.0,
        "ma150": 60000.0,
        "breakout_score": 81.0,
        "pullback_score": 64.0,
        "momentum_score": 77.0,
        "rs_percentile": 92.0,
        "vcp_score": 74.0,
        "trend_score": 88.0,
        "entry_style_selected": "pullback",
        "pullback_pct": 0.0,
        "atr_pct": 0.0,
    }
    engine._fetch_daily = lambda code, days=60: (pd.DataFrame({"close": [1.0] * 20}), {})

    def _fake_compute_features(df, *, min_candles=20):
        return {
            "close": 71000.0,
            "ma20": 68000.0,
            "ma50": 65000.0,
            "ma150": 60000.0,
            "ma10": 69000.0,
            "atr_pct": 0.034,
            "vol_contraction": 0.8,
            "volu_contraction": 0.8,
            "ma20_slope": 12.0,
            "high20": 77600.0,
            "pullback_pct": 8.5,
            "tr_range_pct": 2.4,
            "trend_strength": 1.09,
            "value20": 1200000000.0,
            "volume_missing": False,
        }

    monkeypatch.setattr(pb1_engine_module, "compute_pb1_features", _fake_compute_features)

    candidates = engine._compute_candidates_from_codes(["005930"])

    assert len(candidates) == 1
    assert candidates[0].features["ma20"] == 68000.0
    assert candidates[0].features["atr_pct"] == 0.034
    assert candidates[0].features["pullback_pct"] == 8.5
    assert candidates[0].features["setup_loose_ok"] is True