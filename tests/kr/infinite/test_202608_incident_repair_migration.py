from pathlib import Path


SQL = Path("migrations/0051_repair_kr_infinite_20260819_20260824.sql").read_text(encoding="utf-8")
UPPER = SQL.upper()


def test_incident_repair_is_exactly_scoped_to_proven_122630_cycle() -> None:
    assert "KR_INFINITE_V1" in SQL
    assert "122630" in SQL
    assert "KRINF-20260819-1d1a9a3d" in SQL
    assert "2026-08-19" in SQL
    assert "2026-08-24" in SQL
    assert "NEW_CYCLE_BUY" in SQL
    assert "TAKE_PROFIT_TP1" in SQL
    assert "REQUESTED_QTY = 6" in UPPER
    assert "REQUESTED_QTY = 3" in UPPER
    assert "BROKER_ORDER_ID IS NOT NULL" in UPPER


def test_incident_repair_terminalizes_only_proven_pending_rows_and_clears_tp1_fence() -> None:
    assert UPPER.count("SET STATUS = 'FILLED'") == 2
    assert "'PRE_ORDER_HOLDING_QTY', 0" in UPPER
    assert "'PRE_ORDER_HOLDING_QTY', 6" in UPPER
    assert "'INCIDENT_PROVEN_POST_ORDER_HOLDING_QTY', 6" in UPPER
    assert "'INCIDENT_PROVEN_POST_ORDER_HOLDING_QTY', 3" in UPPER
    assert "'PROFIT_STAGE', 'TP1_FILLED'" in UPPER
    assert "'PENDING_PROFIT_STAGE', NULL" in UPPER
    assert "KR_INF_INCIDENT_REPAIR_V1" in SQL


def test_incident_repair_preserves_audit_history_and_never_blanket_deletes() -> None:
    assert " DELETE " not in f" {UPPER} "
    assert " TRUNCATE " not in f" {UPPER} "
    assert " DROP TABLE " not in f" {UPPER} "
    assert "UPDATE KR_INFINITE_ORDER_INTENTS" in UPPER
    assert "UPDATE KR_INFINITE_STATE" in UPPER
