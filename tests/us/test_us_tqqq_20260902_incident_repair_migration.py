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


def test_repair_refuses_to_expire_any_order_with_positive_fill_evidence():
    assert "NOT EXISTS" in UPPER
    assert "FROM US_FILLS F" in UPPER
    assert "F.CLIENT_ORDER_KEY = O.CLIENT_ORDER_KEY" in UPPER
    assert "COALESCE(F.QTY, 0) > 0" in UPPER
    assert "ACCOUNTING_ACTIVE" in UPPER


def test_repair_is_zero_fill_expiry_only_and_never_invents_price_or_fill():
    assert "SET STATUS = 'EXPIRED'" in UPPER
    assert "QTY_FILLED = 0" in UPPER
    assert "INCIDENT_PROVEN_FILLED_QTY" in UPPER
    assert "NOT_APPLICABLE_ZERO_FILL" in UPPER
    assert "PRICE_USD" not in UPPER
    assert "INSERT INTO US_FILLS" not in UPPER


def test_repair_is_tqqq_only_and_does_not_touch_scheduler_or_strategy_policy():
    assert "US_ORDERS" in UPPER
    assert "WINDOWS" not in UPPER
    assert "SCHEDULER" not in UPPER
    assert "TAKE_PROFIT" not in UPPER
    assert "MARKET_REGIME" not in UPPER
