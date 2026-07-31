from trader.kr.regime import build_kr_regime_snapshot, market_allows_buy, write_snapshot


def observations(up=True):
    sign = 1 if up else -1
    return {
        "close": 120 if up else 80, "ma20": 110, "ma50": 105, "ma200": 100,
        "ma20_slope_5d": sign, "breadth_ma20": .7 if up else .3,
        "breadth_ma50": .65 if up else .35, "advance_ratio": .65 if up else .35,
        "median_return_5d": .02 * sign, "return_5d": .03 * sign,
        "return_20d": .08 * sign, "drawdown_20d": 0 if up else -.12,
        "intraday_return": 0 if up else -.03,
    }


def test_missing_primary_market_fails_closed_but_policy_is_entry_only():
    snap = build_kr_regime_snapshot({"KOSPI": observations(True)})
    assert snap.data_quality == "BLOCKED"
    assert snap.execution_policy.allow_new_buy is False
    assert snap.execution_policy.budget_multiplier == 0


def test_markets_are_gated_independently():
    snap = build_kr_regime_snapshot({"KOSPI": observations(True), "KOSDAQ": observations(False)})
    assert market_allows_buy(snap, "KOSPI")
    assert not market_allows_buy(snap, "KOSDAQ")
    assert not market_allows_buy(snap, "UNKNOWN")


def test_strong_state_requires_sector_quality_and_snapshot_artifact(tmp_path):
    snap = build_kr_regime_snapshot({"KOSPI": observations(True), "KOSDAQ": observations(True)}, sector_data_suspect=True)
    assert all(state.state != "KR_STRONG_RISK_ON" for state in snap.market_states.values())
    target = write_snapshot(snap, tmp_path / "kr_regime_snapshot.json")
    assert target.exists()
    assert '"market_states"' in target.read_text()
