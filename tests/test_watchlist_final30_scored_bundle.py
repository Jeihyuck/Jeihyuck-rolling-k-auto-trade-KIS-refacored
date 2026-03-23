from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd


def test_build_and_save_watchlist_returns_final30_scored_bundle(monkeypatch, tmp_path) -> None:
    from trader import watchlist_builder

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist(self, **_kwargs):
            return [], None

        def save_watchlist(self, **_kwargs):
            return None

    class FakeBuilder:
        def __init__(self, **_kwargs):
            self.last_bundle = {}

        def build(self, members, as_of):
            final30_scored = [
                {
                    "code": "000001",
                    "name": "A",
                    "tech_score": 10.0,
                    "score_final": 20.0,
                    "breakout_score": 30.0,
                    "pullback_score": 40.0,
                    "momentum_score": 50.0,
                }
            ]
            self.last_bundle = {
                "as_of": as_of,
                "universe_scored": list(final30_scored),
                "pool120": list(final30_scored),
                "top50": list(final30_scored),
                "final30": list(final30_scored),
                "final30_scored": pd.DataFrame(final30_scored),
                "weights": {},
                "weights_effective": {},
                "formula": "",
                "reject_summary": {},
                "degrade": {},
                "final_count": 1,
                "shortage_reason": "",
                "contract_failures": [],
            }
            return list(final30_scored)

    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    monkeypatch.setattr(watchlist_builder, "WatchlistRepo", FakeRepo)
    monkeypatch.setattr(watchlist_builder, "WatchlistBuilder", FakeBuilder)
    monkeypatch.setattr(watchlist_builder, "_enrich_watchlist_rows", lambda **kwargs: kwargs.get("rows", []))
    monkeypatch.setattr(watchlist_builder, "save_bundle", lambda **_kwargs: None)

    watchlist, bundle = watchlist_builder.build_and_save_watchlist(
        engine=MagicMock(),
        env="practice",
        strategy="pb1_watchlist",
        as_of=date(2026, 3, 6),
        members=[{"code": "000001", "name": "A"}],
        ohlcv_provider=lambda *_args, **_kwargs: (None, {}),
        minervini_config={},
        force_rebuild=True,
        use_cache=False,
        return_bundle=True,
    )

    assert len(watchlist) == 1
    assert "final30_scored" in bundle
    assert "top50_scored" in bundle
    assert "pool120_scored" in bundle
    assert "universe_scored_df" in bundle
    assert "final30_saved" in bundle
    assert "final30_snapshot_df" in bundle
    assert isinstance(bundle["final30_scored"], pd.DataFrame)
    assert bundle["final30_scored"].iloc[0]["tech_score"] > 0
    assert set(bundle["final30_saved"][0].keys()) == {"code", "meta", "rank", "score"}


def test_watchlist_builder_rejects_multiple_build_calls() -> None:
    from trader.watchlist_builder import WatchlistBuilder

    builder = WatchlistBuilder(
        ohlcv_provider=lambda *_args, **_kwargs: (None, {}),
        minervini_config={},
    )

    final30 = [
        {
            "code": f"{idx:06d}",
            "name": f"N{idx:06d}",
            "tech_score": 10.0,
            "score_final": 20.0,
            "ma20": 10.0,
            "breakout_score": 30.0,
            "pullback_score": 40.0,
            "momentum_score": 50.0,
        }
        for idx in range(1, 31)
    ]

    builder._stage_a_liquidity_filter = lambda members, as_of: (list(final30), list(final30))
    builder._merge_derived_scores = lambda rows, as_of=None: list(rows)
    builder._attach_scores = lambda rows, stage: list(rows)
    builder._assert_nonzero_scores = lambda rows, stage, score_key=None: None

    builder.build(members=list(final30), as_of=date(2026, 3, 6))

    try:
        builder.build(members=list(final30), as_of=date(2026, 3, 6))
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "WATCHLIST_BUILD_CALLED_MULTIPLE_TIMES" in str(exc)

    assert raised
