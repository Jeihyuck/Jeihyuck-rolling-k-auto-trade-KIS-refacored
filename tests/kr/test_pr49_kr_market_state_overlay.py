from pathlib import Path
from trader.kr.market_state_overlay import build_index_context, evaluate_kr_market_state, generate_kr_defense_trim_intents, generate_kr_profit_capture_intents


def test_pr50_preserves_pr49_base_metadata_and_proxy_guard():
    ctx = build_index_context(lambda symbol, lookback: 0.02 if symbol == "229200" else None)
    overlay = evaluate_kr_market_state(ctx)
    assert overlay["overlay_base"] == "PR49"
    assert overlay["overlay_version"] == "PR50"
    assert overlay["market_state"] == "KR_NORMAL"


def test_exit_intent_generators_create_sell_only_paths():
    positions = [{"code": "005930", "qty": 9, "pnl_pct": 0.10}]
    profit = generate_kr_profit_capture_intents(positions, {"profit_capture_enabled": True})
    defense = generate_kr_defense_trim_intents(positions, {"trim_required": True})
    assert profit[0]["side"] == "SELL"
    assert defense[0]["side"] == "SELL"


def test_pb1_engine_contains_overlay_integration_hooks():
    text = Path("trader/pb1_engine.py").read_text(encoding="utf-8")
    for token in [
        "evaluate_kr_market_state",
        "apply_kr_market_state_to_budget",
        "self._kr_market_state_overlay",
        "self._kr_overlay_positions",
        "filter_kr_entry_candidates",
        "pre_api_kr_buy_block",
        "generate_kr_profit_capture_intents",
        "generate_kr_defense_trim_intents",
    ]:
        assert token in text
