from __future__ import annotations


def test_no_candidate_classification_is_ok_no_trade() -> None:
    from trader.pb1_engine import _classify_no_candidate_result

    status, reason = _classify_no_candidate_result()

    assert status == "OK_NO_TRADE"
    assert reason == "NO_CANDIDATES_AFTER_RELAX"
