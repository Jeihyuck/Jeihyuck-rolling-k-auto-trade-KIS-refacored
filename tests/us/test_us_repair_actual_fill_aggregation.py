from contextlib import contextmanager
from pathlib import Path

SRC = Path('scripts/repair_us_trade_integrity.py').read_text()


def test_repair_selects_cumulative_snapshot_instead_of_summing_inquire_ccnl_rows():
    assert 'grouped_actual = {}' in SRC
    forbidden = 'grouped_actual[key]["qty"]' + ' += qty'
    assert forbidden not in SRC
    assert 'canonical_kis_order_cumulative_key' in SRC
    assert 'KIS_ORDER_CUMULATIVE_ACTUAL' in SRC


class R:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class RepairEngine:
    def __init__(self):
        self.order = {"qty_filled": 0, "status": "ACK"}
        self.fill_rows = []
        self.synthetic_active = True
        self.updated = False

    @contextmanager
    def begin(self):
        yield self

    def execute(self, stmt, params=None):
        sql = str(stmt)
        params = params or {}
        if "CREATE TABLE IF NOT EXISTS us_trade_integrity_quarantine" in sql:
            return R(0)
        if "UPDATE us_orders SET qty_filled" in sql:
            self.updated = True
            self.order["qty_filled"] = params["qty"]
            self.order["status"] = "PARTIALLY_FILLED" if params["qty"] < 10 else "FILLED"
            return R(1)
        if "INSERT INTO us_fills" in sql:
            self.fill_rows.append(dict(params))
            return R(1)
        if "superseded_by_kis_actual" in sql:
            self.synthetic_active = False
            return R(1)
        if "realized_pnl_usd" in sql:
            return R(0)
        return R(0)


def test_apply_integrity_plan_skips_order_when_later_snapshot_regresses():
    from scripts.repair_us_trade_integrity import apply_integrity_plan

    engine = RepairEngine()
    result = apply_integrity_plan(
        engine,
        "2026-07-17",
        {"issues": []},
        actual_fills=[
            {"order_no": "O1", "symbol": "AMD", "side": "SELL", "cumulative_filled_qty": 7, "avg_price_usd": 100, "observed_at": "2026-07-17T14:00:00Z"},
            {"order_no": "O1", "symbol": "AMD", "side": "SELL", "cumulative_filled_qty": 3, "avg_price_usd": 100, "observed_at": "2026-07-17T14:00:00Z"},
        ],
    )

    assert engine.updated is False
    assert engine.fill_rows == []
    assert engine.synthetic_active is True
    assert any(row.get("reason") == "REPAIR_CUMULATIVE_SNAPSHOT_CONFLICT" for row in result["row_ids"])
