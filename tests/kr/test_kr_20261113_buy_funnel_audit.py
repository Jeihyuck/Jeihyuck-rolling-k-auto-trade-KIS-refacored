"""PR-C: read-only execution evidence and missing-stage honest classifications."""
from trader.kr.entry_observability import (
    classify_skip, reasons_from_ledger, summarize_buy_skips,
)


def _skip(code, reason, run="run-1", stage="PB1-AM", ts="2026-11-13T01:00:00Z"):
    return {"event_type": "ORDER_SKIP", "side": "BUY",
            "code": code, "reasons": '["' + reason + '","name:synthetic"]',
            "run_id": run, "stage": stage, "ts": ts}


def test_volume_proof_unknown_is_not_auto_labeled_implementation_bug():
    kind, detail = classify_skip("KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE")
    assert (kind, detail) == ("DATA_UNVERIFIED", "VOLUME_OR_QUOTE_PROOF")


def test_timing_gate_without_context_is_unverified_not_fabricated_wait():
    assert classify_skip("KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH") == (
        "UNVERIFIED", "TIMING_DETAIL_NOT_EVIDENCED")


def test_holding_gate_without_original_kis_position_is_not_proven_expected():
    assert classify_skip("BUYABLE_EXISTING_HOLDING_KIS")[0] == "UNVERIFIED"


def test_reasons_drop_name_while_preserving_failure():
    assert reasons_from_ledger('["KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING", "name:ABC"]') == [
        "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING"
    ]


def test_event_vs_symbol_vs_unique_run_differ_without_account_assertion():
    events = [
        _skip("219130", "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE"),
        _skip("219130", "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_STALE_OR_FUTURE",
              run="run-2", ts="2026-11-13T02:00:00Z"),
        _skip("028050", "BUYABLE_EXISTING_HOLDING_KIS"),
    ]
    result = summarize_buy_skips(events, trade_date="2026-11-13")
    assert result["event_count"] == 3
    assert result["unique_codes"] == 2
    assert result["unique_run_code_pairs"] == 3
    assert result["by_category_reason_occurrences"] == {
        "DATA_UNVERIFIED": 2, "UNVERIFIED": 1
    }
    assert result["broker_holdings_verified"] is False
    assert result["timing_gate_evaluation_verified"] is False
    assert result["unobserved_stages"].startswith("NOT_EVALUATED")


def test_non_buy_non_skip_are_not_counted():
    events = [{"event_type": "ORDER_SKIP", "side": "SELL",
               "code": "028050", "run_id": "r", "reasons": "[]", "stage": "SELL"},
              {"event_type": "RECONCILE", "side": "BUY",
               "code": "028050", "run_id": "r", "reasons": "[]", "stage": "AM"}]
    report = summarize_buy_skips(events, trade_date="2026-11-13")
    assert report["event_count"] == 0


def test_max_rows_is_explicitly_truncation_marked():
    report = summarize_buy_skips([_skip("005930", "NO_SIGNAL")],
                                 trade_date="2026-11-13", max_rows=1)
    assert not report["truncated"]  # exactly at limit is complete; caller probes limit+1


def test_kst_query_boundaries_are_utc_attested_not_midnight_naive():
    from datetime import date
    from scripts.kr_entry_funnel_audit import report_for_date

    class FakeResult:
        def mappings(self):
            return []

    class FakeConnection:
        def execute(self, statement, params):
            assert "r.env = 'practice'" in str(statement)
            assert "r.strategy = 'pb1_pullback_close'" in str(statement)
            assert "e.ts::timestamptz >= :start_at" in str(statement)
            assert "r.started_at >= :start_at" not in str(statement)
            assert "ORDER BY e.ts::timestamptz, e.ledger_event_id" in str(statement)
            assert params["max_rows_plus_one"] == 10001
            assert params["start_at"].isoformat() == "2026-11-12T15:00:00+00:00"
            assert params["stop_at"].isoformat() == "2026-11-13T15:00:00+00:00"
            return FakeResult()

    class FakeContext:
        def __enter__(self):
            return FakeConnection()
        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def connect(self):
            return FakeContext()

    report = report_for_date(day=date(2026, 11, 13), engine=FakeEngine())
    assert report["event_count"] == 0
    assert report["query_scope"]["environment"] == "practice"
    assert report["query_scope"]["date_basis"] == "ledger_event_ts_kst_not_run_started_at"
    assert report["broker_holdings_verified"] is False


def test_broker_unverified_holding_and_market_policy_are_not_auto_expected():
    assert classify_skip("BUYABLE_EXISTING_HOLDING_KIS")[0] == "UNVERIFIED"
    assert classify_skip("RISK_GATE_BLOCKED")[0] == "UNVERIFIED"


def test_owner_breakout_trigger_is_separate_from_data_missing():
    assert classify_skip("KR_VOLUME_PROOF_BREAKOUT_PRICE_TRIGGER_MISSING")[0] == "EXPECTED"
    assert classify_skip("KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING")[0] == "DATA_UNVERIFIED"
    assert classify_skip("KR_VOLUME_PROOF_GENERIC_UNKNOWN")[0] == "UNVERIFIED"


def test_event_ids_are_real_keys_not_timestamp_reason_heuristics():
    first = _skip("219130", "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING")
    second = dict(first)
    first["ledger_event_id"] = "ledger-id-1"
    second["ledger_event_id"] = "ledger-id-2"
    report = summarize_buy_skips([first, second], trade_date="2026-11-13")
    assert report["event_count"] == 2
    assert report["distinct_ledger_event_ids"] == 2
    assert report["event_id_duplicates_observed"] == 0


def test_missing_code_is_not_fabricated_000000():
    record = _skip("", "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING")
    report = summarize_buy_skips([record], trade_date="2026-11-13")
    assert report["invalid_or_missing_code_events"] == 1
    assert report["unique_codes"] == 0
    assert "000000" not in report["by_code"]
    assert report["by_code"]["UNKNOWN_CODE"]["events"] == 1


def test_proof_category_can_override_opaque_volume_reason():
    record = _skip("005930", "KR_VOLUME_PROOF_OTHER")
    record["payload_json"] = {"features": {"volume_proof": {
        "failure_category": "STRATEGY_TRIGGER_NOT_MET",
        "confirmation_state": "FAILED",
    }}}
    report = summarize_buy_skips([record], trade_date="2026-11-13")
    assert report["by_category_reason_occurrences"] == {"EXPECTED": 1}


def test_limit_plus_one_marks_partial_not_exact_limit():
    from datetime import date
    from scripts.kr_entry_funnel_audit import report_for_date

    class FakeResult:
        def mappings(self):
            return [
                {"ledger_event_id": str(i), **_skip("219130", "KR_VOLUME_PROOF_KIS_EXCHANGE_QUOTE_TIME_MISSING")}
                for i in range(3)
            ]

    class FakeConnection:
        def execute(self, statement, params):
            assert params["max_rows_plus_one"] == 3
            return FakeResult()

    class FakeContext:
        def __enter__(self):
            return FakeConnection()
        def __exit__(self, *args):
            return False

    class FakeEngine:
        def connect(self):
            return FakeContext()

    report = report_for_date(day=date(2026, 11, 13), engine=FakeEngine(), limit=2)
    assert report["event_count"] == 2
    assert report["truncated"] is True
    assert report["truncated_after_events"] == 2
