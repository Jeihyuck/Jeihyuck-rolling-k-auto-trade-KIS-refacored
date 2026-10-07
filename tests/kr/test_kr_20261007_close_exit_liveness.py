from __future__ import annotations

from pathlib import Path

from trader import pb1_runner


class _Positions:
    def list_positions_by_codes(self, *, env, strategy, codes):
        return [
            {
                "code": "293490",
                "position_meta": {
                    "position_book": "SWING_BOOK",
                    "trade_horizon": "SWING",
                    "force_eod_close": False,
                },
            }
        ]


class _Fills:
    def list_latest_buy_fills_by_codes(self, env, codes):
        return {}


class _Orders:
    def list_today_buy_orders(self, env, code=None):
        return []


class _Kis:
    def __init__(self):
        self.sell_calls = []

    def sell_stock_market(self, code, qty):
        self.sell_calls.append((code, qty))
        return {"rt_cd": "0", "output": {"ODNO": "1"}}


def test_close_liquidation_off_does_not_short_circuit_owner_policy():
    source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert 'return [], True, {}, phase_for_log, "EXIT_SHORTCIRCUIT"' not in source
    assert "[KR_CLOSE][PREPOLICY_RECONCILE]" in source
    assert "run_kr_close_policy_from_tagged_positions(" in source


def test_close_policy_reports_positions_evaluated_even_when_policy_holds():
    kis = _Kis()
    result = pb1_runner.run_kr_close_policy_from_tagged_positions(
        kis_holdings=[{"code": "293490", "qty": 78}],
        positions_repo=_Positions(),
        fills_repo=_Fills(),
        orders_repo=_Orders(),
        kis_client=kis,
        env="practice",
        dry_run=False,
    )

    assert result["evaluated_positions"] == 1
    assert result["policy_sell_candidates"] == 0
    assert result["accepted_policy_sells"] == 0
    assert kis.sell_calls == []


def test_emergency_liquidation_flag_stays_separate_from_normal_policy(monkeypatch):
    monkeypatch.setenv("PB1_CLOSE_LIQUIDATION_ENABLED", "0")
    source = Path("trader/pb1_runner.py").read_text(encoding="utf-8")
    assert "close_liquidation_enabled_for_engine" in source
    assert "run_emergency_close_liquidation_from_kis_holdings(" in source
    assert "run_kr_close_policy_from_tagged_positions(" in source
