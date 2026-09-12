from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, got {count}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


def replace_count(path: str, old: str, new: str, expected: int) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != expected:
        raise RuntimeError(f"{path}: expected {expected} matches, got {count}: {old!r}")
    p.write_text(text.replace(old, new), encoding="utf-8")


# Preserve the pre-PR127 TTL reconciler result shape when no escalation occurs.
replace_once(
    "trader/us/infinite/integration.py",
    '    result = {"expired": len(expired), "cancel_requested": 0, "terminal": 0, "pending": 0, "escalated": 0}\n',
    '    result = {"expired": len(expired), "cancel_requested": 0, "terminal": 0, "pending": 0}\n',
)
replace_once(
    "trader/us/infinite/integration.py",
    '                result["escalated"] += 1\n',
    '                result["escalated"] = int(result.get("escalated", 0)) + 1\n',
)

# Fail closed on stale current-price objects in BOTH PB1 BUY lookup paths, not only exits.
replace_once(
    "trader/us/pb1/us_entry_engine.py",
    '''    return allowed\n\n\ndef generate_entry_intents(\n''',
    '''    return allowed\n\n\ndef _quote_is_stale(current: Any) -> bool:\n    if not isinstance(current, dict):\n        return False\n    quality = str(current.get("quality") or "").strip().lower()\n    return bool(\n        current.get("stale")\n        or current.get("suspect")\n        or current.get("_stale_date")\n        or quality in {"stale", "suspect", "degraded"}\n    )\n\n\ndef generate_entry_intents(\n''',
)
replace_count(
    "trader/us/pb1/us_entry_engine.py",
    '''                current = provider.get_current_price(symbol, exchange)\n''',
    '''                current = provider.get_current_price(symbol, exchange)\n                if _quote_is_stale(current):\n                    track_skip(symbol, "stale_current_price", {\n                        "source": current.get("source") if isinstance(current, dict) else None,\n                        "quality": current.get("quality") if isinstance(current, dict) else None,\n                        "asof": (current.get("asof") or current.get("_stale_date") or current.get("asof_epoch")) if isinstance(current, dict) else None,\n                    })\n                    logger.warning(\n                        "[US_ENTRY][QUOTE_STALE_BLOCK] symbol=%s exchange=%s source=%s quality=%s action=skip_buy",\n                        symbol, exchange,\n                        current.get("source") if isinstance(current, dict) else "unknown",\n                        current.get("quality") if isinstance(current, dict) else "unknown",\n                    )\n                    continue\n''',
    2,
)

# Make forced KR close phase skips fatal regardless of whether the PB1 durable result
# was written. The explicit engine status is stronger evidence than a generic missing-result
# fallback and must never normalize back to OK.
replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''    if session == "close" and status == "OK" and str(os.getenv("PB1_LAST_RESULT_STATUS") or "").upper() == "SKIP_PHASE_WINDOW":\n        status = "FAIL"\n        exit_code = 2\n        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")\n''',
    '''    if session == "close" and pb1_last == "SKIP_PHASE_WINDOW":\n        status = "FAIL"\n        exit_code = max(exit_code, 2)\n        summary_reason = "CLOSE_PHASE_NOT_EXECUTED"\n        logger.error("[KR_CLOSE][FAIL] reason=CLOSE_PHASE_NOT_EXECUTED")\n''',
)

# Do not report liquidation enabled when the close engine is only performing normal exits.
replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''                    "close_liquidation_enabled": False,\n''',
    '''                    "close_liquidation_enabled": str(os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")).strip() == "1",\n''',
)
replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''            "phase_executed": status != "FAIL",\n            "force_phase": True,\n            "skip_phase_window": status == "FAIL" and exit_code == 2,\n            "entry_enabled": False,\n            "exit_enabled": True,\n            "close_enabled": True,\n            "close_liquidation_enabled": True,\n''',
    '''            "phase_executed": status not in {"FAIL", "FAILED"},\n            "force_phase": True,\n            "skip_phase_window": status == "FAIL" and exit_code == 2,\n            "entry_enabled": False,\n            "exit_enabled": True,\n            "close_enabled": True,\n            "close_liquidation_enabled": str(os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")).strip() == "1",\n''',
)

# Critical latent bug: **count_reconcile used to be expanded last, so its generic
# status/reason could overwrite the already-determined session FAIL/reason. Namespace
# reconciliation status/reason and keep session truth authoritative.
replace_once(
    "trader/kr/runner/trade_session_runner.py",
    '''    result = {"status": status, "final_status": status, "reason": summary_reason, "exit_code": exit_code, "completed": bool(completed), "retryable": bool(retryable), "engine_started": bool(pb1_result_present and not lock_unavailable), "pb1_result_present": bool(pb1_result_present), "orders_intent": orders_intent, "orders_submitted": orders_submitted, "orders_ack": orders_ack, "fills_confirmed": filled_confirmed, "ack_without_confirmed_fill": ack_without_fill, "unresolved_ack": int(pb1_result.get("unresolved_acks", 0) or 0), **count_reconcile}\n''',
    '''    reconciliation_fields = {k: v for k, v in count_reconcile.items() if k not in {"status", "reason"}}\n    result = {\n        **reconciliation_fields,\n        "reconciliation_status": count_reconcile.get("status"),\n        "reconciliation_reason": count_reconcile.get("reason"),\n        "status": status, "final_status": status, "reason": summary_reason,\n        "exit_code": exit_code, "completed": bool(completed), "retryable": bool(retryable),\n        "engine_started": bool(pb1_result_present and not lock_unavailable),\n        "pb1_result_present": bool(pb1_result_present), "orders_intent": orders_intent,\n        "orders_submitted": orders_submitted, "orders_ack": orders_ack,\n        "fills_confirmed": filled_confirmed, "ack_without_confirmed_fill": ack_without_fill,\n        "unresolved_ack": int(pb1_result.get("unresolved_acks", 0) or 0),\n    }\n''',
)

# Fix PR127's new source-contract test to anchor inside _run_pb1_session, not an earlier
# unrelated close conditional in this large module.
p = Path("tests/kr/test_kr_pr127_close_contract.py")
text = p.read_text(encoding="utf-8")
text = text.replace(
    '    start = text.index(\'if session == "close":\')\n    section = text[start:start + 1800]\n',
    '    start = text.index("def _run_pb1_session")\n    section = text[start:start + 7000]\n',
)
text += '''\n\ndef test_reconciliation_status_is_namespaced_and_cannot_overwrite_session_status():\n    source = Path("trader/kr/runner/trade_session_runner.py").read_text(encoding="utf-8")\n    assert '"reconciliation_status": count_reconcile.get("status")' in source\n    assert '"reconciliation_reason": count_reconcile.get("reason")' in source\n    assert '**count_reconcile}' not in source\n'''
p.write_text(text, encoding="utf-8")

p = Path("tests/us/test_us_pr127_residual_integrity.py")
text = p.read_text(encoding="utf-8")
text = text.replace('    assert result["escalated"] == 1\n', '    assert result.get("escalated") == 1\n')
text += '''\n\ndef test_pb1_entry_has_stale_quote_fail_closed_on_both_lookup_paths():\n    from trader.us.pb1.us_entry_engine import _quote_is_stale\n    assert _quote_is_stale({"last": "100", "stale": True, "source": "DB_STALE"}) is True\n    assert _quote_is_stale({"last": "100", "quality": "degraded"}) is True\n    assert _quote_is_stale({"last": "100", "quality": "fresh"}) is False\n    source = Path("trader/us/pb1/us_entry_engine.py").read_text(encoding="utf-8")\n    assert source.count('if _quote_is_stale(current):') == 2\n    assert source.count('[US_ENTRY][QUOTE_STALE_BLOCK]') == 2\n'''
p.write_text(text, encoding="utf-8")

print("PR127 regression fixups applied")
