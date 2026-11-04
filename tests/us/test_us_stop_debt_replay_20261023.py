"""Durable stop debt replay re-enters only the canonical PB1 path for exact scope."""
from datetime import datetime, timezone
from trader.us.entry_exit_contract import build_us_entry_exit_contract
from trader.us.protective_stop_obligation import replay_confirmed_protective_intents


def _snapshot(monkeypatch,*,symbol="AMD",account="practice-a",life="life-a",orderable=4):
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT","0.03")
    monkeypatch.setenv("US_HARD_STOP_PCT","0.06")
    contract=build_us_entry_exit_contract({
        "symbol":symbol,"strategy_owner":"US_STANDARD","entry_strategy":"us_pb1"
    })
    return {"symbol":symbol,"strategy_owner":"US_STANDARD",
            "account_id":account,"position_lifecycle_id":life,
            "qty":4,"holding_qty":4,"orderable_qty":orderable,
            "exchange":"NASDAQ","entry_price":100,
            "current_price":92,"resolved_current_price":92,
            "meta":{"entry_exit_contract":contract}}


def _debt(**updates):
    debt={"symbol":"AMD","account_id":"practice-a","lifecycle_id":"life-a",
          "obligation_state":"PENDING","severity":3,
          "hard_stop_pct":0.06,"soft_stop_pct":0.03,
          "first_signal_at":"2026-10-22T15:00:00+00:00"}
    debt.update(updates)
    return debt


def test_replay_exact_lifecycle_via_existing_intent_builder(monkeypatch):
    captured=[]
    def fake_builder(**kwargs):
        captured.append(kwargs)
        return {"side":"SELL","symbol":kwargs["symbol"],"qty":kwargs["qty"],
                "exit_type":kwargs["exit_type"]}
    monkeypatch.setattr("trader.us.pb1.us_exit_engine._make_exit_intent",fake_builder)
    pos=_snapshot(monkeypatch)
    intents=replay_confirmed_protective_intents(snapshots=[pos],pending=[_debt()],
        existing_intents=[],now=datetime(2026,10,23,tzinfo=timezone.utc))
    assert intents==[{"side":"SELL","symbol":"AMD","qty":4,"exit_type":"hard_stop_loss"}]
    assert captured[0]["position_lifecycle_id"]=="life-a"
    assert captured[0]["meta_extra"]["stop_obligation_replay"] is True
    assert captured[0]["policy_cfg"]["hard_stop"]==0.06


def test_replay_blocks_other_account_old_lifecycle_and_duplicate(monkeypatch):
    calls=[]
    monkeypatch.setattr("trader.us.pb1.us_exit_engine._make_exit_intent",
                        lambda **kwargs:calls.append(kwargs) or {"symbol":"AMD","side":"SELL"})
    pos=_snapshot(monkeypatch)
    assert replay_confirmed_protective_intents(
        snapshots=[pos],pending=[_debt(account_id="practice-b"),_debt(lifecycle_id="other")],
        existing_intents=[],now=None)==[]
    assert replay_confirmed_protective_intents(
        snapshots=[pos],pending=[_debt()],
        existing_intents=[{"symbol":"AMD","side":"SELL"}],now=None)==[]
    assert calls==[]


def test_replay_never_submits_satisfied_soft_or_unorderable(monkeypatch):
    calls=[]
    monkeypatch.setattr("trader.us.pb1.us_exit_engine._make_exit_intent",
                        lambda **kwargs:calls.append(kwargs) or {})
    snapshots=[_snapshot(monkeypatch,orderable=0)]
    debts=[_debt(),_debt(obligation_state="SATISFIED"),
           _debt(severity=1),_debt(obligation_state="UNKNOWN")]
    assert replay_confirmed_protective_intents(
        snapshots=snapshots,pending=debts,existing_intents=[],now=None)==[]
    assert calls==[]


def test_wsl_replay_rejects_same_account_and_lifecycle_from_different_epoch(monkeypatch):
    """A prior portfolio generation is not the current holding, even for same symbol."""
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    builder_calls = []
    monkeypatch.setattr(
        "trader.us.pb1.us_exit_engine._make_exit_intent",
        lambda **kwargs: builder_calls.append(kwargs) or
        {"side": "SELL", "symbol": kwargs["symbol"], "qty": kwargs["qty"]},
    )
    current = {**_snapshot(monkeypatch), "trading_epoch_id": "epoch-current",
               "portfolio_epoch_id": "portfolio-current",
               "position_cycle_id": "life-a"}
    debt_old = _debt(trading_epoch_id="epoch-previous")
    debt_unknown = _debt(trading_epoch_id="")
    for prior in (debt_old, debt_unknown):
        assert replay_confirmed_protective_intents(
            snapshots=[current], pending=[prior],
            existing_intents=[], now=None,
        ) == []
    missing_current_epoch = dict(current)
    missing_current_epoch.pop("trading_epoch_id")
    assert replay_confirmed_protective_intents(
        snapshots=[missing_current_epoch], pending=[_debt(trading_epoch_id="epoch-current", portfolio_epoch_id="portfolio-current",
              position_cycle_id="life-a")],
        existing_intents=[], now=None,
    ) == []
    assert builder_calls == []

    confirmed = replay_confirmed_protective_intents(
        snapshots=[current], pending=[_debt(trading_epoch_id="epoch-current", portfolio_epoch_id="portfolio-current",
              position_cycle_id="life-a")],
        existing_intents=[], now=None,
    )
    assert len(confirmed) == 1
    assert confirmed[0]["side"] == "SELL"
    assert builder_calls[0]["position_lifecycle_id"] == "life-a"


def test_wsl_replay_rejects_other_account_and_position_cycle_at_matching_epoch(monkeypatch):
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    calls = []
    monkeypatch.setattr(
        "trader.us.pb1.us_exit_engine._make_exit_intent",
        lambda **kwargs: calls.append(kwargs) or {"side": "SELL"},
    )
    current = {**_snapshot(monkeypatch), "trading_epoch_id": "epoch-current",
               "portfolio_epoch_id": "portfolio-current",
               "position_cycle_id": "life-a"}
    prior_debts = [
        _debt(account_id="wrong-account", trading_epoch_id="epoch-current",
              portfolio_epoch_id="portfolio-current", position_cycle_id="life-a"),
        _debt(lifecycle_id="different-position-cycle", trading_epoch_id="epoch-current",
              portfolio_epoch_id="portfolio-current", position_cycle_id="different-position-cycle"),
        _debt(trading_epoch_id="epoch-current", portfolio_epoch_id="wrong-portfolio",
              position_cycle_id="life-a"),
    ]
    assert replay_confirmed_protective_intents(
        snapshots=[current], pending=prior_debts,
        existing_intents=[], now=None,
    ) == []
    assert calls == []
