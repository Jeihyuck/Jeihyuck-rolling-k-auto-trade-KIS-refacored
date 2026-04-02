from __future__ import annotations

from trader import pb1_runner


def test_plain_watchlist_payload_is_rejected_for_trade(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(pb1_runner, "resolve_repo_root", lambda: tmp_path)
    monkeypatch.setattr(pb1_runner, "repo_root", lambda: tmp_path)
    pb1_runner._FINAL30_LOAD_CACHE.clear()

    class FakeRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def verify_watchlist_scored_contract(self, **_kwargs):
            return {
                "ok": False,
                "rows": 30,
                "uniq_codes": 30,
                "uniq_ranks": 30,
                "null_critical": 0,
                "missing_fields": ["score_final"],
                "columns": ["code", "rank", "score"],
                "rows_data": [{"code": f"{idx:06d}", "rank": idx, "score": float(idx)} for idx in range(1, 31)],
                "errors": ["missing_required_columns"],
            }

        def load_watchlist(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "WatchlistRepo", FakeRepo)

    result = pb1_runner.load_trade_final30_scored(engine=object(), env="practice", as_of="2026-03-11")

    assert result["source_name"] == "none"
    assert result["usable"] is False
    assert "score_final" in result["missing_scored_cols"]
