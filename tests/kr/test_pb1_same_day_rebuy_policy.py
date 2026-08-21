from trader.kr.pb1_stability import evaluate_same_day_reentry

BASE = dict(sell_exists=True, no_sellable_sticky=False, prior_reason_family="TRAIL_STOP_HIT",
            cooldown_elapsed_min=61, market_state="KR_RISK_ON", entry_score_strong=True,
            fresh_entry_signal=True, reentry_count=0)


def decide(**changes):
    return evaluate_same_day_reentry(**{**BASE, "sell_confirmed": True, "pending_sell": False, **changes})


def test_ack_without_confirmed_fill_blocks(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "1")
    assert not decide(sell_confirmed=False).allowed


def test_confirmed_but_cooldown_incomplete_blocks(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "1")
    assert not decide(cooldown_elapsed_min=59).allowed


def test_recovery_and_fresh_signal_allow_one_reentry(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "1")
    assert decide().reason == "BUYABLE_TODAY_REBUY_ALLOWED_BY_RECOVERY"


def test_hard_stop_and_pending_sell_always_block(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "1")
    assert not decide(prior_reason_family="HARD_STOP").allowed
    assert not decide(pending_sell=True).allowed
