from datetime import date

from trader.kr.infinite.repository import execution_action_identity


def _identity(*, stage="TP1", day=date(2026, 10, 2), owner_account="account"):
    return execution_action_identity(
        env="practice",
        account_key=owner_account,
        market="KR",
        trading_epoch_id="epoch-1",
        symbol="RANDOM_KR_SYMBOL",
        cycle_id="cycle-1",
        side="SELL_PARTIAL",
        reason="TAKE_PROFIT",
        stage=stage,
        trade_date=day,
    )


def test_kr_infinite_identity_survives_trade_date_rollover_and_separates_exit_stages():
    original = _identity()
    next_day = _identity(day=date(2026, 10, 5))
    next_stage = _identity(stage="TP2")

    assert original.action_key == next_day.action_key
    assert original.action_key != next_stage.action_key


def test_kr_infinite_identity_separates_account_and_cycle():
    original = _identity()
    other_account = _identity(owner_account="other-account")
    other_cycle = execution_action_identity(
        env="practice",
        account_key="account",
        market="KR",
        trading_epoch_id="epoch-1",
        symbol="RANDOM_KR_SYMBOL",
        cycle_id="cycle-2",
        side="SELL_PARTIAL",
        reason="TAKE_PROFIT",
        stage="TP1",
    )

    assert original.action_key != other_account.action_key
    assert original.action_key != other_cycle.action_key
