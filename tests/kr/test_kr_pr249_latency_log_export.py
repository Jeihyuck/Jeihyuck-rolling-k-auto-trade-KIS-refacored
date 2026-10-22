"""Offline parser tests. Synthetic strings verify rejection paths only."""
import pytest
from scripts.kr.pr249_latency_log_export import export_measured_ticks


def sample(*, status="OK", ack=True, include_end=True, sha="abc123"):
    lines = [
        "INFO [PB1][TICK][CALL_RUN_ONCE] kind=am now=2026-10-22T09:37:00+09:00 session_end=x",
        "INFO [KR_LATENCY][SELL_SUBMIT_START] owner=KR_STANDARD code=253590 cycle=good "
        "run_id=tick-uuid order_id=order-uuid client_key=hello from_tick_start_s=39.500 "
        "tick_origin_source=RUNNER_FULL_TICK from_policy_ready_s=1.100",
        "INFO [KR_LATENCY][SELL_SUBMIT_RETURN] owner=KR_STANDARD code=253590 "
        "run_id=tick-uuid order_id=order-uuid elapsed_s=0.200 "
        f"ack_observed={1 if ack else 0} exception_type=NONE",
    ]
    if include_end:
        lines.append(
            "INFO [KR_LATENCY][TICK_END] kind=am index=1 total_elapsed_s=150.000 "
            f"result_status={status} sha={sha} run_id=session-uuid tick_run_id=tick-uuid"
        )
    return lines


def test_real_tag_export_measures_full_tick_to_broker_submit_not_engine_boot():
    rows = export_measured_ticks(sample(), revision="after", workload_id="30-5-kr")
    assert len(rows) == 1
    assert rows[0]["first_protective_sell_submit_sec"] == 39.5
    assert rows[0]["tick_elapsed_sec"] == 150.0
    assert rows[0]["git_sha"] == "abc123"
    assert rows[0]["tick_id"].endswith(":tick-uuid")


def test_failing_tick_is_preserved_in_export_not_success_only():
    lines = sample(status="TIMEOUT_OR_FAILURE")
    rows = export_measured_ticks(lines, revision="after", workload_id="30-5-kr")
    assert rows[0]["result_status"] == "TIMEOUT_OR_FAILURE"


def test_clipped_timeout_tick_refuses_performance_claim():
    with pytest.raises(ValueError, match="MISSING_TICK_END"):
        export_measured_ticks(sample(include_end=False), revision="after", workload_id="30-5-kr")


def test_rejected_or_ambiguous_broker_ack_is_not_latency_success():
    with pytest.raises(ValueError, match="UNRESOLVED_SELL_SUBMIT"):
        export_measured_ticks(sample(ack=False), revision="after", workload_id="30-5-kr")


def test_unversioned_tick_blocks_merge_proof():
    with pytest.raises(ValueError, match="UNVERSIONED_OR_UNSCOPED_TICK"):
        export_measured_ticks(sample(sha="UNVERSIONED"), revision="after", workload_id="30-5-kr")


def test_wrong_engine_only_timing_is_not_broker_submit_evidence():
    lines = [x.replace("RUNNER_FULL_TICK","ENGINE_ONLY_UNQUALIFIED") for x in sample()]
    with pytest.raises(ValueError, match="SELL_ORIGIN_NOT_FULL_TICK"):
        export_measured_ticks(lines, revision="after", workload_id="30-5-kr")
