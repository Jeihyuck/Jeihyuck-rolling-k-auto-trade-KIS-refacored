from pathlib import Path


def test_kr_runner_exports_single_environment_contract():
    text = Path("run_pb1_kr.sh").read_text()
    for token in ("ENV=\"$STRATEGY_ENV\"", "STRATEGY=best_k_meta", "MARKET=KRX", "TRADE_MARKET=KR", "PB1_BUYABLE_BACKFILL_ENABLED=1", "PB1_JOB=BUILD_WATCHLIST", "PB1_JOB=TRADE_INTRADAY", "python -m trader.pb1_runner"):
        assert token in text
    for phase in ("prep", "am", "pm", "close"):
        assert phase in text
