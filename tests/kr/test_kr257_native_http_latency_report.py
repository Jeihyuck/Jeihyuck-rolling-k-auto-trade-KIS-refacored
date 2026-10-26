"""All ticks (including timeouts) and true SELL HTTP boundary SLO audit."""
from scripts.kr257_native_http_latency_report import analyze_kr_latency_lines, p95


def _tick(index, seconds, status="OK"):
    return (f"2026-10-26 09:35 INFO [KR_LATENCY][TICK_END] "
            f"kind=am index={index} total_elapsed_s={seconds:.3f} "
            f"result_status={status} sha=abc123 run_id=R{index} tick_run_id=T{index}")


def _sell(index, seconds):
    return (f"2026-10-26 09:35 INFO [KR_LATENCY][NATIVE_HTTP_START] "
            f"side=SELL code=053800 tick_elapsed_s={seconds:.3f} "
            f"tick_index={index} run_id=R{index}")


def test_no_p95_without_twenty_real_observations():
    result = analyze_kr_latency_lines([_tick(1, 9), _sell(1, 4)])
    assert result["protected_sell_target_60s"] == "INSUFFICIENT_SAMPLES"
    assert result["all_tick_target_180s"] == "INSUFFICIENT_SAMPLES"


def test_twenty_samples_include_failed_or_watchdog_timeout():
    lines = [_tick(i, 50) for i in range(1, 20)]
    lines += [_tick(20, 190, "TIMEOUT_OR_FAILURE")]
    lines += [_sell(i, 15) for i in range(1, 21)]
    result = analyze_kr_latency_lines(lines)
    assert result["tick_attempts_including_timeout"] == 20
    assert result["tick_failure_or_timeout"] == 1
    assert result["protected_sell_target_60s"] == "PASS"
    assert result["all_tick_target_180s"] == "PASS"  # p95 nearest rank 19/20
    assert p95([50] * 18 + [190] * 2) == 190


def test_real_sell_http_requires_native_boundary_not_order_ready():
    lines = [_tick(i, 180) for i in range(1, 21)]
    lines += ["[ORDER_READY] code=053800 side=SELL qty=10"] * 30
    result = analyze_kr_latency_lines(lines)
    assert result["protected_sell_native_http_samples"] == 0
    assert result["protected_sell_target_60s"] == "INSUFFICIENT_SAMPLES"


def test_native_http_bad_samples_miss_threshold():
    lines = [_tick(i, 120) for i in range(1, 21)]
    lines += [_sell(i, 79) for i in range(1, 21)]
    result = analyze_kr_latency_lines(lines)
    assert result["protected_sell_target_60s"] == "FAIL"


def test_actual_kis_request_logs_native_before_session_request():
    import inspect
    from trader.kis_wrapper import KisAPI
    src = inspect.getsource(KisAPI._safe_request)
    assert src.index("[KR_LATENCY][NATIVE_HTTP_START]") < src.index("resp = self.session.request(")
    assert "KR_TICK_ORIGIN_MONOTONIC" in src
