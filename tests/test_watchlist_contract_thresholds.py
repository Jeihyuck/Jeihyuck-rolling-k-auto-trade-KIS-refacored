from trader.watchlist_builder import validate_watchlist_contract


def _rows(n: int):
    return [
        {
            "code": f"{i:06d}",
            "score_final": 90.0,
            "tech_score": 80.0,
            "breakout_score": 70.0,
            "pullback_score": 60.0,
            "momentum_score": 50.0,
            "rs_percentile": 88.0,
            "vcp_score": 77.0,
            "entry_style_selected": "BREAKOUT",
        }
        for i in range(1, n + 1)
    ]


def test_candidate_pool_based_allows_universe_120():
    failures = validate_watchlist_contract(
        universe_scored=_rows(120),
        pool120=_rows(120),
        top50=_rows(50),
        final30=_rows(30),
        contract_mode="candidate_pool_based",
        universe_scored_source="test",
    )
    assert failures == []


def test_candidate_pool_based_allows_40_40_40_30_when_scores_exist():
    rows = _rows(40)
    for row in rows:
        row.update(
            {
                "score_final": 90.0,
                "tech_score": 80.0,
                "breakout_score": 70.0,
                "pullback_score": 60.0,
                "momentum_score": 50.0,
                "rs_percentile": 88.0,
                "vcp_score": 77.0,
                "entry_style_selected": "BREAKOUT",
            }
        )

    failures = validate_watchlist_contract(
        universe_scored=rows,
        pool120=rows,
        top50=rows,
        final30=rows[:30],
        contract_mode="candidate_pool_based",
        universe_scored_source="candidate_pool",
        candidate_pool_size=40,
    )
    assert failures == []


def test_broad_universe_based_rejects_universe_119():
    failures = validate_watchlist_contract(
        universe_scored=_rows(119),
        pool120=_rows(120),
        top50=_rows(50),
        final30=_rows(30),
        contract_mode="broad_universe_based",
        universe_scored_source="test",
    )
    assert any("contract_universe_too_small" in f for f in failures)
