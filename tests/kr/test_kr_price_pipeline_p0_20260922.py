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


def test_strategy_owner_reject_counts_as_terminal_skip():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    marker = 'ownership_ok, ownership_reason = enforce_kr_order_ownership(cf.code, "KR_STANDARD")'
    owner_block = source[source.index(marker):]
    owner_block = owner_block[:owner_block.index('status: dict[str, Any] = self._empty_order_status()')]
    assert '"skipped": 1' in owner_block
    assert '"api_submitted": 0' in owner_block
    assert '"submit_terminal_status": "SKIPPED_BY_POLICY"' in owner_block


def test_unsubmitted_created_intent_is_not_open_buy_blocker():
    from trader.pb1_engine import PB1Engine

    assert PB1Engine._is_blocking_open_buy_row({
        "status": "CREATED",
        "submitted_at": None,
        "acked_at": None,
        "kis_odno": None,
        "broker_order_id": None,
    }) is False
    assert PB1Engine._is_blocking_open_buy_row({
        "status": "INTENT",
        "submitted_at": None,
        "acked_at": None,
        "kis_odno": None,
        "broker_order_id": None,
    }) is False


def test_broker_boundary_evidence_still_blocks_duplicate_buy():
    from trader.pb1_engine import PB1Engine

    assert PB1Engine._is_blocking_open_buy_row({"status": "SUBMITTED"}) is True
    assert PB1Engine._is_blocking_open_buy_row({"status": "UNRESOLVED_ACK"}) is True
    assert PB1Engine._is_blocking_open_buy_row({
        "status": "CREATED",
        "kis_odno": "1234567890",
    }) is True
    assert PB1Engine._is_blocking_open_buy_row({
        "status": "ERROR",
        "submitted_at": "2026-09-22T09:37:05+09:00",
    }) is True


def test_open_buy_code_derivation_uses_broker_boundary_filter():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert "and self._is_blocking_open_buy_row(row)" in source
    assert "[PB1][OPEN_BUY][PREBROKER_IGNORED]" in source


def test_retryable_intent_bypass_exists_in_both_buyable_gates():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert source.count("duplicate_intent_exists = bool(existing_order) and not retryable_unsubmitted") >= 2
    assert "[BUYABLE_GATE][RETRYABLE_PRIOR_INTENT]" in source


def test_unsubmitted_intents_do_not_consume_today_spent_budget():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    anchor = "today_spent = 0.0"
    block = source[source.index(anchor):]
    block = block[:block.index("planned_spent = today_spent")]
    assert "if not self._is_blocking_open_buy_row(row):" in block
    assert "continue" in block


def test_submit_time_policy_skip_drives_zero_api_classification():
    source = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    assert 'submit_policy_blocker = next(' in source
    assert 'for result in (submit_result.get("results") or [])' in source
    assert '"OPENING_30MIN_BUY_BLOCK"' in source
    assert '"BUYABLE_OPEN_ORDER"' in source
    assert '"BUYABLE_DUPLICATE_INTENT"' in source
    assert '"KR_INF_OWNERSHIP_RESERVED"' in source
    assert '"OWNERSHIP_RESERVED"' in source
    assert "top_policy_blocker = drop_policy_blocker or submit_policy_blocker" in source
