"""Existing Samsung E&A adoption: no inherited V1 rewrite / no unverified KIS."""
import copy

import pytest

from trader.kr import us_profit_existing_amendment as amendment
from trader.kr.us_standard_profit_parity import (
    build_new_swing_parity_contract, verified_contract,
)


class BrokerProof:
    env = "practice"

    def __init__(self, *, qty=33, avg="60200", include_original=True, extra_sell=False):
        self.qty, self.avg = qty, avg
        self.include_original = include_original
        self.extra_sell = extra_sell
        self.calls = []

    def get_balance(self, force=True):
        assert force
        self.calls.append("balance")
        return {
            "rt_cd": "0",
            "output1": [{
                "pdno": "028050", "hldg_qty": str(self.qty),
                "ord_psbl_qty": str(self.qty), "pchs_avg_pric": self.avg,
            }],
        }

    def inquire_daily_ccld(self, *, start_date, end_date):
        assert start_date == "20261106"
        assert end_date >= start_date
        self.calls.append("daily-ccld")
        original = {
            "odno": "0000011628", "ord_dt": "20261106",
            "pdno": "028050", "sll_buy_dvsn_cd": "02",
            "tot_ccld_qty": "33", "avg_prvs": "60200", "cncl_yn": "N",
        }
        sell = {
            "odno": "0000099999", "ord_dt": "20261110",
            "pdno": "028050", "sll_buy_dvsn_cd": "01",
            "tot_ccld_qty": "0", "cncl_yn": "N",
        }
        rows = [original] if self.include_original else []
        if self.extra_sell:
            rows.append(sell)
        return {"rt_cd": "0", "output1": rows}


def test_exact_original_kis_buy_and_current_33_shares_are_required():
    kis = BrokerProof()
    proof = amendment._broker_proof(kis)
    assert kis.calls == ["balance", "daily-ccld"]
    assert proof["source"] == "KIS_DAILY_CCLD_AND_FRESH_BALANCE"
    assert proof["verified_qty"] == 33
    assert proof["original_buy_odno"] == amendment.TARGET["original_buy_odno"]


@pytest.mark.parametrize("kis", [
    BrokerProof(qty=32), BrokerProof(avg="60000"),
    BrokerProof(include_original=False), BrokerProof(extra_sell=True),
])
def test_amendment_rejects_missing_qty_price_original_order_or_pending_sell(kis):
    with pytest.raises(ValueError):
        amendment._broker_proof(kis)


def test_existing_amendment_contract_is_distinct_from_new_buy_and_signed(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    proof = amendment._broker_proof(BrokerProof())
    c = build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="EXPLICIT_POSITION_AMENDMENT",
        account_id="practice:account", trading_epoch_id=amendment.TARGET["epoch"],
        position_cycle_id=amendment.TARGET["cycle"], runner_version="US_V3",
        code="028050", amendment_proof=proof,
    )
    assert verified_contract(c)
    assert c["position_action"] == "EXPLICIT_POSITION_AMENDMENT"
    assert c["activation_mode"] == "ACTIVE_PRACTICE"
    modified = copy.deepcopy(c)
    modified["amendment_proof"]["verified_qty"] = 34
    assert not verified_contract(modified)
    modified = copy.deepcopy(c)
    modified["position_action"] = "NEW_POSITION_BUY"
    assert not verified_contract(modified)


def test_missing_amendment_proof_never_builds_explicit_contract(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    with pytest.raises(ValueError, match="AMENDMENT_EVIDENCE_REQUIRED"):
        build_new_swing_parity_contract(
            strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
            trade_horizon="SWING", position_action="EXPLICIT_POSITION_AMENDMENT",
            account_id="acc", trading_epoch_id="epoch",
            position_cycle_id="cycle", runner_version="US_V3",
            code="028050",
        )


def test_other_cycle_never_reads_broker_or_updates_db(monkeypatch):
    amendment._ATTEMPTED.clear()
    assert amendment.maybe_amend_samsung_ea_position(
        engine=object(), kis=BrokerProof(), env="practice",
        position_cycle_id="OLD_EPOCH_CYCLE",
    ) is None


def test_broker_stub_cannot_approve_amendment():
    kis = BrokerProof()
    kis.get_balance = lambda force=True: {"rt_cd": "0", "output1": [], "_diag_stub": True}
    with pytest.raises(ValueError, match="BALANCE_UNVERIFIED"):
        amendment._broker_proof(kis)
