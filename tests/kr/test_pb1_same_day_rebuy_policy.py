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


class _GateHarness:
    _display_code = staticmethod(lambda code: code)


def _integrated_gate(monkeypatch, **changes):
    from trader.pb1_engine import PB1Engine
    monkeypatch.setenv("PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1")
    monkeypatch.setenv("KR_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "1")
    context = {
        "kis_holding_qty": 0, "open_order_exists": False,
        "today_submit_exists": True, "today_fill_exists": True,
        "today_buy_exists": False, "today_sell_exists": True,
        "sell_confirmed": True, "pending_sell": False,
        "no_sellable_sticky": False, "prior_sell_reason_family": "TRAIL_STOP_HIT",
        "sell_cooldown_elapsed_min": 61, "market_state": "KR_RISK_ON",
        "entry_score_strong": True, "fresh_entry_signal": True,
        "same_day_reentry_count": 0, "cooldown_active": False,
        "blocking_duplicate_exists": False, "duplicate_intent_exists": False,
    }
    context.update(changes)
    return PB1Engine._evaluate_unified_buyable_gate(
        _GateHarness(), code="028050", gate_context=context, allow_add_to_existing=False)


def test_integrated_gate_allows_confirmed_recovery_despite_submit_and_fill(monkeypatch):
    decision = _integrated_gate(monkeypatch)
    assert decision.ok
    assert "BUYABLE_TODAY_FILL" not in decision.reason_codes
    assert "BUYABLE_TODAY_SUBMIT" not in decision.reason_codes


def test_integrated_gate_still_blocks_unsafe_reentry_evidence(monkeypatch):
    assert not _integrated_gate(monkeypatch, pending_sell=True).ok
    assert not _integrated_gate(monkeypatch, prior_sell_reason_family="HARD_STOP").ok
    assert not _integrated_gate(monkeypatch, sell_cooldown_elapsed_min=59).ok
    assert not _integrated_gate(monkeypatch, fresh_entry_signal=False).ok
