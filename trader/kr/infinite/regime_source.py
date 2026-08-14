"""Standalone, current KOSPI regime production using existing KIS data APIs."""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

from trader.kis_wrapper import KisAPI
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.kr.regime import calculate_market_state
from trader.time_utils import is_krx_trading_day

SYMBOLS = ("069500", "226490", "091160", "005930", "000660")


def _sma(values: list[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("KR_INF_REGIME_INSUFFICIENT_ROWS")
    return mean(values[-period:])


def _ret(values: list[float], period: int) -> float:
    return values[-1] / values[-period - 1] - 1 if len(values) > period and values[-period - 1] > 0 else 0.0


def _latest_expected(day: date) -> date:
    cursor = day
    while not is_krx_trading_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def build_current_kospi_regime(kis, *, trade_date: date | None = None) -> dict:
    """Build the canonical KR regime input; missing or stale series raise."""
    day = trade_date or date.today()
    series: dict[str, list[dict]] = {}
    latest_dates: list[date] = []
    provider = ChainOHLCVProvider([KISOHLCVProvider(kis), KRXOHLCVProvider()],
                                  env=os.getenv("KIS_ENV", "practice"))
    for symbol in SYMBOLS:
        try:
            rows = list(kis.get_daily_candles(symbol, count=260))
        except Exception:
            rows = []
        if len(rows) < 201:
            result = provider.get_ohlcv(symbol, 260, purpose="regime", usage_context="prep", allow_long_fetch=True)
            rows = result.df.to_dict("records")
        normalized = sorted((row for row in rows if row.get("date") and row.get("close")), key=lambda row: str(row["date"]))
        if len(normalized) < 201:
            raise RuntimeError(f"KR_INF_REGIME_INSUFFICIENT_ROWS:{symbol}")
        series[symbol] = normalized
        latest = normalized[-1]["date"]
        if hasattr(latest, "date"):
            latest = latest.date()
        if not isinstance(latest, date):
            latest = datetime.strptime(str(latest).replace("-", "")[:8], "%Y%m%d").date()
        latest_dates.append(latest)
    expected = _latest_expected(day)
    if any(value != expected for value in latest_dates):
        raise RuntimeError("KR_INF_REGIME_DATA_STALE")

    benchmark = [float(row["close"]) for row in series["069500"]]
    breadth20 = []
    breadth50 = []
    positive = []
    for symbol in ("091160", "005930", "000660"):
        values = [float(row["close"]) for row in series[symbol]]
        breadth20.append(values[-1] >= _sma(values, 20))
        breadth50.append(values[-1] >= _sma(values, 50))
        positive.append(_ret(values, 1) > 0)
    ma20_now = _sma(benchmark, 20)
    ma20_then = mean(benchmark[-25:-5])
    observations = {
        "market": "KOSPI", "close": benchmark[-1], "ma20": ma20_now,
        "ma50": _sma(benchmark, 50), "ma200": _sma(benchmark, 200),
        "ma20_slope_5d": ma20_now / ma20_then - 1,
        "breadth_ma20": sum(breadth20) / len(breadth20),
        "breadth_ma50": sum(breadth50) / len(breadth50),
        "advance_ratio": sum(positive) / len(positive),
        "median_return_5d": sorted(_ret([float(row["close"]) for row in series[s]], 5) for s in ("091160", "005930", "000660"))[1],
        "return_5d": _ret(benchmark, 5), "return_20d": _ret(benchmark, 20),
        "drawdown_20d": benchmark[-1] / max(benchmark[-20:]) - 1,
        "input_data_quality": "OK", "stale": False,
    }
    market = calculate_market_state("KOSPI", observations)
    return {
        "as_of": datetime.combine(expected, datetime.min.time(), timezone.utc).isoformat(),
        "source": "KR_INFINITE_STANDALONE_KIS", "data_quality": market.data_quality,
        "global_state": market.state, "market_states": {"KOSPI": {
            "market": market.market, "score": market.score, "state": market.state,
            "data_quality": market.data_quality, "components": market.components,
            "reasons": list(market.reasons),
        }},
    }


def produce_current_regime(kis, *, path: str | Path | None = None, trade_date: date | None = None) -> Path:
    payload = build_current_kospi_regime(kis, trade_date=trade_date)
    target = Path(path or os.getenv("KR_INFINITE_REGIME_PATH", "artifacts/kr_regime_snapshot.json"))
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(target)
    return target


def main() -> int:
    produce_current_regime(KisAPI(kis_env=os.getenv("KIS_ENV", "practice")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
