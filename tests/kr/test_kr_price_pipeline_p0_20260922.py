from __future__ import annotations

from pathlib import Path


def test_price_circuit_is_symbol_scoped():
    from trader.kis_wrapper import _PriceCache

    cache = _PriceCache(ttl_sec=1, circuit_sec=15)
    cache.open_circuit(code="005930", rate_limited=True)
    assert cache.is_circuit_open("005930") is True
    assert cache.is_circuit_open("000660") is False


def test_kr_entry_identity_uses_actual_session_not_hardcoded_close():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert 'self._client_order_key(cf.code, cf.mode, "BUY", "close", "PB1")' not in source
    assert 'self._client_order_key(cf.code, cf.mode, "BUY", self.window_internal, self._entry_stage_name())' in source


def test_pretrade_failure_closes_unsubmitted_created_intent_for_retry():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert '"rt_cd": "LOCAL_PRETRADE_BLOCK"' in source
    assert '"msg_cd": "PRETRADE_CHECK_FAILED"' in source
    assert "[ORDER][PRETRADE][INTENT_CLOSED]" in source


def test_retryable_unsubmitted_intent_is_not_duplicate_blocker():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert "retryable_unsubmitted = bool(" in source
    assert "duplicate_intent_exists = bool(existing_order) and not retryable_unsubmitted" in source


def test_opening_buy_block_is_classified_as_policy_skip():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    marker = '"OPENING_30MIN_BUY_BLOCK",'
    assert marker in source
