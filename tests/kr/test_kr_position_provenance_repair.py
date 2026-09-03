from datetime import date

from trader.kr.position_provenance import Confidence, audit_position


def position(symbol="005930", qty=15, avg=100):
    return {"symbol": symbol, "qty": qty, "average_price": avg, "account": "a", "env": "real", "strategy_owner": "PB1"}


def fill(side, qty, price, day, **extra):
    return {"symbol": "005930", "account": "a", "env": "real", "side": side,
            "qty": qty, "price": price, "trade_date": day, **extra}


def test_exact_last_flat_chain_recovers_canonical_policy_and_age():
    rows = [fill("BUY", 10, 90, "2026-08-01"), fill("SELL", 10, 90, "2026-08-02"),
            fill("BUY", 5, 100, "2026-08-20", id=7, entry_reason="ENTRY_PULLBACK",
                 entry_style_selected="ENTRY_PULLBACK", initial_stop=92),
            fill("BUY", 10, 100, "2026-08-21")]
    result = audit_position(position(), rows, trade_date=date(2026, 9, 2))
    assert result.confidence is Confidence.CONFIRMED
    assert result.original_buy_id == 7
    assert result.updates["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert result.updates["holding_days"] > 0
    assert result.updates["initial_stop"] == 92


def test_mismatch_missing_metadata_and_owner_exclusion_fail_closed():
    rows = [fill("BUY", 10, 100, "2026-08-20")]
    assert audit_position(position(qty=9), rows).confidence is Confidence.AMBIGUOUS
    assert audit_position(position(qty=10, avg=120), rows).confidence is Confidence.AMBIGUOUS
    assert audit_position(position(qty=10), rows).confidence is Confidence.PARTIAL
    assert audit_position(position(symbol="122630"), []).confidence is Confidence.UNRECOVERABLE
