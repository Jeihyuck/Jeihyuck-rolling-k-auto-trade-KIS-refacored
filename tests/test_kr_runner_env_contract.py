from pathlib import Path


def test_kr_runner_exports_single_environment_contract():
    text = Path("run_pb1_kr.sh").read_text()
    for token in ("ENV=\"$STRATEGY_ENV\"", "STRATEGY=best_k_meta", "MARKET=KRX", "TRADE_MARKET=KR", "PB1_BUYABLE_BACKFILL_ENABLED=1"):
        assert token in text
    for phase in ("prep", "am", "pm", "close"):
        assert phase in text
