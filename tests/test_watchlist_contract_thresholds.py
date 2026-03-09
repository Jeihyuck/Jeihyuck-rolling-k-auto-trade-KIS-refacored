from trader.watchlist_builder import validate_watchlist_contract


def _rows(n: int):
    return [{"code": f"{i:06d}"} for i in range(1, n + 1)]


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


def test_broad_universe_based_rejects_universe_120():
    failures = validate_watchlist_contract(
        universe_scored=_rows(120),
        pool120=_rows(120),
        top50=_rows(50),
        final30=_rows(30),
        contract_mode="broad_universe_based",
        universe_scored_source="test",
    )
    assert any("contract_universe_too_small" in f for f in failures)
