from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, got {count}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


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
    '''            "phase_executed": status != "FAIL",\n            "force_phase": True,\n            "skip_phase_window": status == "FAIL" and exit_code == 2,\n            "entry_enabled": False,\n            "exit_enabled": True,\n            "close_enabled": True,\n            "close_liquidation_enabled": True,\n''',
    '''            "phase_executed": status not in {"FAIL", "FAILED"},\n            "force_phase": True,\n            "skip_phase_window": status == "FAIL" and exit_code == 2,\n            "entry_enabled": False,\n            "exit_enabled": True,\n            "close_enabled": True,\n            "close_liquidation_enabled": str(os.getenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")).strip() == "1",\n''',
)

# Fix PR127's new source-contract test to anchor inside _run_pb1_session, not an earlier
# unrelated close conditional in this large module.
p = Path("tests/kr/test_kr_pr127_close_contract.py")
text = p.read_text(encoding="utf-8")
text = text.replace(
    '    start = text.index(\'if session == "close":\')\n    section = text[start:start + 1800]\n',
    '    start = text.index("def _run_pb1_session")\n    section = text[start:start + 7000]\n',
)
p.write_text(text, encoding="utf-8")

p = Path("tests/us/test_us_pr127_residual_integrity.py")
text = p.read_text(encoding="utf-8")
text = text.replace('    assert result["escalated"] == 1\n', '    assert result.get("escalated") == 1\n')
p.write_text(text, encoding="utf-8")

print("PR127 regression fixups applied")
