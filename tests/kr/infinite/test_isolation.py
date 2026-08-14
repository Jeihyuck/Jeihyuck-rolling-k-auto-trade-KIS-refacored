from pathlib import Path
def test_no_forbidden_pb1_dependencies():
 text="\n".join(p.read_text() for p in Path("trader/kr/infinite").glob("*.py"))
 for forbidden in ("generate_kr_profit_capture_intents","generate_kr_defense_trim_intents","trader.us.infinite"):assert forbidden not in text
 assert not Path("trader/kr/infinite/regime_source.py").exists()
 assert "calculate_market_state" not in text
