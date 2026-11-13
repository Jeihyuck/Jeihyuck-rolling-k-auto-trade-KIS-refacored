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


def test_duplicate_holding_gate_is_expected():
    assert classify_skip("BUYABLE_EXISTING_HOLDING_KIS")[0] == "EXPECTED"


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
        "DATA_UNVERIFIED": 2, "EXPECTED": 1
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
    assert report["truncated"]
