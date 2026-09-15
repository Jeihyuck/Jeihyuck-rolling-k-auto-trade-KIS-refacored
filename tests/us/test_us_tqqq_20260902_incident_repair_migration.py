from pathlib import Path


SQL = Path("migrations/0052_repair_us_tqqq_20260902_zero_fill.sql").read_text(encoding="utf-8")
UPPER = SQL.upper()


def test_exact_identity_and_zero_fill_evidence_are_required():
    assert "TQQQ_INF_V3:A2E52345-37B9-46DE-AF62-596EE706A50B:2026-09-02:BUY" in UPPER
    assert "O.TRADE_DATE = DATE '2026-09-02'" in UPPER
    assert "O.SYMBOL = 'TQQQ'" in UPPER
    assert "O.SIDE = 'BUY'" in UPPER
    assert "O.QTY_REQUESTED = 3" in UPPER
    assert "COALESCE(O.QTY_FILLED, 0) = 0" in UPPER
    assert "NULLIF(BTRIM(COALESCE(O.ORDER_NO, '')), '') IS NOT NULL" in UPPER


def test_repair_refuses_any_positive_fill_evidence():
    assert "NOT EXISTS" in UPPER
    assert "FROM US_FILLS F" in UPPER
    assert "F.CLIENT_ORDER_KEY = O.CLIENT_ORDER_KEY" in UPPER
    assert "COALESCE(F.QTY, 0) > 0" in UPPER
    assert "ACCOUNTING_ACTIVE" in UPPER


def test_repair_only_expires_zero_fill_and_never_invents_fill_price():
    assert "SET STATUS = 'EXPIRED'" in UPPER
    assert "QTY_FILLED = 0" in UPPER
    assert "INCIDENT_PROVEN_FILLED_QTY" in UPPER
    assert "NOT_APPLICABLE_ZERO_FILL" in UPPER
    assert "INSERT INTO US_FILLS" not in UPPER
    assert "PRICE_USD" not in UPPER


def test_repair_records_broker_evidence_contract():
    assert "ORIGINAL_DATE_QUERY_ZERO_FILL_PLUS_ORIGINAL_ORDER_NOT_FOUND" in UPPER
    assert "INCIDENT_REPAIR_VERSION" in UPPER
    assert "TQQQ_INF_20260902_ZERO_FILL_V2" in UPPER
