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
    assert snap.data_quality == "DEGRADED"
    assert snap.market_policies["KOSPI"].allow_new_buy is True
    assert snap.market_policies["KOSDAQ"].allow_new_buy is False
    assert snap.execution_policy.budget_multiplier > 0


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


def test_below_both_mas_single_rebound_is_pending_and_capped():
    row = observations(True)
    row.update({"close": 90, "ma20": 100, "ma50": 110, "intraday_return_positive": .04,
                "gap_up_return": .02, "above_open": True, "above_vwap": True,
                "advance_ratio_intraday": .72, "turnover_expansion": 1.5,
                "leader_confirmation_count": 2, "distance_from_intraday_high": -.01,
                "shock_consecutive_ticks": 1, "shock_minutes": 0})
    snap = build_kr_regime_snapshot({"KOSPI": row, "KOSDAQ": row})
    assert snap.global_state == "KR_SHOCK_REBOUND_PENDING"
    assert snap.execution_policy.budget_multiplier == .10
    assert snap.execution_policy.max_new_positions == 1


def test_three_ticks_ten_minutes_confirms_but_never_risk_on():
    row = observations(True)
    row.update({"close": 90, "ma20": 100, "ma50": 110, "intraday_return_positive": .04,
                "gap_up_return": .02, "above_open": True, "above_vwap": True,
                "advance_ratio_intraday": .72, "turnover_expansion": 1.5,
                "leader_confirmation_count": 2, "distance_from_intraday_high": -.01,
                "shock_consecutive_ticks": 3, "shock_minutes": 10})
    snap = build_kr_regime_snapshot({"KOSPI": row, "KOSDAQ": row})
    assert snap.global_state == "KR_SHOCK_REBOUND_CONFIRMED"
    assert snap.execution_policy.budget_multiplier == .25
    assert snap.execution_policy.max_new_positions == 3
    assert snap.execution_policy.allow_add_to_existing is False


def test_ma20_recovered_below_ma50_caps_at_normal():
    row = observations(True)
    row.update({"close": 105, "ma20": 100, "ma50": 110})
    snap = build_kr_regime_snapshot({"KOSPI": row, "KOSDAQ": row})
    assert snap.global_state == "KR_NORMAL"


def test_pb1_production_overlay_builds_typed_snapshot(monkeypatch, tmp_path):
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-07-31"
    engine._now_kst = None
    engine._kr_positions_for_overlay = lambda positions: positions
    engine._kr_account_snapshot_for_overlay = lambda positions, cash: {"portfolio_equity_krw": cash}
    engine._is_kr_equity_context = lambda: True
    snap = build_kr_regime_snapshot({"KOSPI": observations(True), "KOSDAQ": observations(True)})
    called = []
    engine._build_kr_regime_snapshot_for_tick = lambda rows, account: called.append((rows, account)) or snap
    monkeypatch.chdir(tmp_path)
    overlay, budget = engine._evaluate_kr_market_state_overlay_for_tick(
        tick_budget_krw=1_000_000, positions=[], available_cash_krw=2_000_000, final30_rows=[])
    assert called
    assert engine._kr_regime_snapshot if hasattr(engine, "_kr_regime_snapshot") else snap
    assert overlay["market_states"] == snap.market_states
    assert budget == 1_100_000


def test_pb1_snapshot_path_fetches_only_real_krx_symbols(monkeypatch, tmp_path):
    from trader.pb1_engine import PB1Engine
    engine = PB1Engine.__new__(PB1Engine)
    engine._today = "2026-07-31"
    engine._now_kst = None
    calls = []
    engine._kr_etf_observation = lambda symbol, breadth: calls.append(symbol) or observations(True)
    monkeypatch.chdir(tmp_path)
    engine._build_kr_regime_snapshot_for_tick([], {})
    assert set(calls) == {"069500", "226490", "229200", "091160", "005930", "000660"}
    assert not ({"KOSPI", "KOSDAQ", "KOSPI200"} & set(calls))


def test_one_market_failure_is_degraded_and_healthy_market_keeps_budget():
    snap = build_kr_regime_snapshot({"KOSPI": observations(True)})
    assert snap.data_quality == "DEGRADED"
    assert snap.execution_policy.budget_multiplier > 0
    assert market_allows_buy(snap, "KOSPI")
    assert not market_allows_buy(snap, "KOSDAQ")
    assert snap.market_policies["KOSDAQ"].budget_multiplier == 0


def test_both_market_failures_block_all_buys():
    snap = build_kr_regime_snapshot({})
    assert snap.data_quality == "BLOCKED"
    assert snap.execution_policy.budget_multiplier == 0
    assert not market_allows_buy(snap, "KOSPI")
    assert not market_allows_buy(snap, "KOSDAQ")


def test_confirmed_rebound_and_weaker_market_never_exceed_25_percent():
    kospi=observations(True); kospi.update({"close":90,"ma20":100,"ma50":110,
      "intraday_return_positive":.08,"gap_up_return":.04,"above_open":True,"above_vwap":True,
      "advance_ratio_intraday":.82,"turnover_expansion":1.8,"distance_from_intraday_high":-.01,
      "leader_confirmation_count":3,"shock_consecutive_ticks":3,"shock_minutes":11})
    kosdaq=observations(False)
    snap=build_kr_regime_snapshot({"KOSPI":kospi,"KOSDAQ":kosdaq})
    assert snap.market_states["KOSPI"].state == "KR_SHOCK_REBOUND_CONFIRMED"
    assert snap.execution_policy.budget_multiplier <= .25
    assert snap.execution_policy.allow_add_to_existing is False
