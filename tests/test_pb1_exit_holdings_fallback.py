from __future__ import annotations

import json

import sqlalchemy as sa

from trader.pb1_engine import PB1Engine
from trader.window_router import WindowDecision


class DummyOrdersRepo:
    def __init__(self) -> None:
        self.engine = sa.create_engine("sqlite:///:memory:")


class DummyFillsRepo:
    def __init__(self, fills=None) -> None:
        self._fills = list(fills or [])

    def list_latest_buy_fills_by_codes(self, _env, codes):
        result = {}
        for fill in self._fills:
            code = str(fill.get("code") or "").zfill(6)
            if code in codes and str(fill.get("side") or "").upper() == "BUY" and code not in result:
                result[code] = dict(fill)
        return result

    def list_fills_in_window(self, _env, *, start_at, end_at, side=None, code=None, codes=None):
        del start_at, end_at, side, code, codes
        return list(self._fills)


class DummyLedgerRepo:
    pass


def _make_engine(*, fills=None):
    return PB1Engine(
        universe_repo=object(),
        orders_repo=DummyOrdersRepo(),
        fills_repo=DummyFillsRepo(fills=fills),
        positions_repo=object(),
        ledger_repo=DummyLedgerRepo(),
        kis=None,
        window=WindowDecision(name="day", phase="trade"),
        window_label="day",
        phase="trade",
        dry_run=True,
        env="practice",
        run_id="run-1",
        intended_live=False,
    )


def test_load_effective_holdings_for_exit_uses_db_positions_when_balance_empty():
    engine = _make_engine()
    holdings, meta = engine.load_effective_holdings_for_exit(
        [],
        [
            {
                "code": "005930",
                "qty": 3,
                "avg_buy_price": 201000,
                "market": "KOSPI",
                "entry_ts": "2026-03-19T09:20:00+09:00",
            }
        ],
    )

    assert meta["source"] == "db_positions"
    assert len(holdings) == 1
    assert holdings[0].code == "005930"
    assert holdings[0].source == "db_positions"


def test_load_effective_holdings_for_exit_reconstructs_from_fills_when_positions_empty():
    fills = [
        {"code": "005930", "side": "BUY", "qty": 3, "price": 201000, "filled_at": "2026-03-19T09:20:00+09:00"},
        {"code": "032830", "side": "BUY", "qty": 2, "price": 229000, "filled_at": "2026-03-19T09:21:00+09:00"},
    ]
    engine = _make_engine(fills=fills)
    holdings, meta = engine.load_effective_holdings_for_exit([], [])

    assert meta["source"] == "ledger_reconstruct"
    assert {holding.code for holding in holdings} == {"005930", "032830"}


def test_load_effective_holdings_for_exit_uses_injected_test_holdings(monkeypatch):
    engine = _make_engine()
    monkeypatch.setenv(
        "PB1_EXIT_TEST_HOLDINGS_JSON",
        json.dumps(
            [
                {
                    "code": "005930",
                    "qty": 3,
                    "avg_price": 201000,
                    "bought_at": "2026-03-19T09:20:00+09:00",
                }
            ]
        ),
    )

    holdings, meta = engine.load_effective_holdings_for_exit([], [])

    assert meta["source"] == "injected_test_holdings"
    assert len(holdings) == 1
    assert holdings[0].source == "injected_test_holdings"