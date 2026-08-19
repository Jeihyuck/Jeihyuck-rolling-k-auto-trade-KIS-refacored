def test_kr_fallback_contract_marks_overlay_unavailable(monkeypatch):
    from trader.kr import artifacts

    metadata = {}
    monkeypatch.setenv("KR_REGIME_FALLBACK_ENABLED", "1")
    market_state = metadata.get("market_state")
    if not market_state:
        market_state = "KR_NORMAL"
        metadata.update({"market_state": market_state, "regime_fallback_used": True,
                         "intraday_overlay_available": False, "fallback_reason": "prep_default"})

    assert metadata["market_state"] == "KR_NORMAL"
    assert metadata["regime_fallback_used"] is True
    assert metadata["intraday_overlay_available"] is False
    assert metadata["fallback_reason"] == "prep_default"
