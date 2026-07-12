from trader.us.db import repos


def setup_function(): repos.reset_memory_stores()

def test_final30_daily_metrics_survive_memory_roundtrip():
    row={"symbol":"NVDA","exchange":"NASDAQ","strategy":"us_pb1","score":1.0,"locked":True,"ma20":1,"ma50":2,"ma150":3,"ma200":4,"ma200_slope":.01,"rs_20d":.1,"rs_60d":.2,"rs_120d":.3,"daily_bar_count":260,"daily_metrics_as_of":"2026-07-10","daily_metrics_source":"price_daily","daily_history_quality":"OK"}
    repos.clear_and_save_locked_us_watchlist([row], "2026-07-13", "run", "OK")
    out=repos.load_locked_us_watchlist("2026-07-13", min_count=1)
    meta=out[0].get("meta") or {}
    for k in ["ma20","ma50","ma150","ma200","ma200_slope","rs_20d","rs_60d","rs_120d","daily_bar_count","daily_metrics_as_of","daily_metrics_source","daily_history_quality"]:
        assert meta.get(k) == row[k]


def test_prep_runner_benchmark_failure_contract_fields_present_in_source():
    import inspect
    from trader.us.runner import prep_runner
    src = inspect.getsource(prep_runner.run_prep)
    assert 'watchlist_result["market_state_overlay"] = dict(market_state_overlay)' in src
    assert '"trade_block_reason": "BENCHMARK_DAILY_DATA_UNAVAILABLE"' in src
    assert '"effective_capital_scale": 0.0' in src
    assert '"effective_max_new_positions": 0' in src
