from __future__ import annotations

import csv
import io
import math
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests


YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"


class MarketDataError(RuntimeError):
    pass


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/143.0 Safari/537.36 nullim-macro-monitor/1.0"
            )
        }
    )
    return s


def _get_json(url: str, params: dict[str, Any] | None = None, attempts: int = 3) -> dict[str, Any]:
    last_exc: Exception | None = None
    with _session() as s:
        for attempt in range(attempts):
            try:
                r = s.get(url, params=params, timeout=20)
                r.raise_for_status()
                return r.json()
            except Exception as exc:  # network/provider errors are retried then surfaced
                last_exc = exc
                if attempt + 1 < attempts:
                    time.sleep(1.5 * (attempt + 1))
    raise MarketDataError(f"request failed: {url}: {last_exc}")


def yahoo_series(symbol: str, range_: str, interval: str) -> dict[str, Any]:
    payload = _get_json(
        YAHOO_CHART.format(symbol=quote(symbol, safe="")),
        params={"range": range_, "interval": interval, "includePrePost": "true", "events": "div,splits"},
    )
    chart = payload.get("chart") or {}
    if chart.get("error"):
        raise MarketDataError(f"Yahoo error for {symbol}: {chart['error']}")
    results = chart.get("result") or []
    if not results:
        raise MarketDataError(f"Yahoo returned no result for {symbol}")
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_block = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote_block.get("close") or []
    highs = quote_block.get("high") or []
    rows: list[dict[str, float]] = []
    for idx, ts in enumerate(timestamps):
        close = closes[idx] if idx < len(closes) else None
        high = highs[idx] if idx < len(highs) else None
        if close is None or not math.isfinite(float(close)):
            continue
        rows.append(
            {
                "timestamp": float(ts),
                "close": float(close),
                "high": float(high) if high is not None and math.isfinite(float(high)) else float(close),
            }
        )
    if not rows:
        raise MarketDataError(f"Yahoo returned no usable rows for {symbol}")
    return {"rows": rows, "meta": result.get("meta") or {}}


def yahoo_last(symbol: str, range_: str = "5d", interval: str = "1d") -> float:
    return yahoo_series(symbol, range_, interval)["rows"][-1]["close"]


def fred_latest(series: str) -> dict[str, Any]:
    url = FRED_CSV.format(series=quote(series, safe=""))
    with _session() as s:
        r = s.get(url, timeout=20)
        r.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(r.text)))
    for row in reversed(rows):
        raw = (row.get(series) or "").strip()
        if raw and raw != ".":
            return {"date": row.get("DATE"), "value": float(raw)}
    raise MarketDataError(f"FRED returned no value for {series}")


def _latest_and_previous_tnx() -> tuple[float, float | None]:
    # ^TNX is quoted as 10x the 10-year Treasury yield (e.g. 49.50 == 4.950%).
    intraday = yahoo_series("^TNX", "1d", "5m")["rows"]
    current = intraday[-1]["close"] / 10.0
    daily = yahoo_series("^TNX", "5d", "1d")["rows"]
    previous = daily[-2]["close"] / 10.0 if len(daily) >= 2 else None
    return current, previous


def collect_snapshot() -> dict[str, Any]:
    errors: list[str] = []

    def guarded(name: str, fn, default=None):
        try:
            return fn()
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            return default

    tnx_pair = guarded("us10y_live", _latest_and_previous_tnx, (None, None))
    us10y_live, us10y_prev = tnx_pair
    brent = guarded("brent", lambda: yahoo_last("BZ=F", "1d", "5m"))
    vix = guarded("vix", lambda: yahoo_last("^VIX", "1d", "5m"))
    spx = guarded("sp500", lambda: yahoo_last("^GSPC", "1d", "5m"))

    spx_1y = guarded("sp500_1y", lambda: yahoo_series("^GSPC", "1y", "1d"), None)
    spx_high = None
    drawdown = None
    if spx_1y and spx is not None:
        spx_high = max(row["high"] for row in spx_1y["rows"])
        if spx_high > 0:
            drawdown = (spx / spx_high - 1.0) * 100.0

    fred_dgs10 = guarded("fred_dgs10", lambda: fred_latest("DGS10"), None)
    fred_real10 = guarded("fred_dfii10", lambda: fred_latest("DFII10"), None)
    fred_be10 = guarded("fred_t10yie", lambda: fred_latest("T10YIE"), None)

    yield_change_bp = None
    if us10y_live is not None and us10y_prev is not None:
        yield_change_bp = (us10y_live - us10y_prev) * 100.0

    return {
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "us10y_live": us10y_live,
        "us10y_prev_close": us10y_prev,
        "us10y_change_bp": yield_change_bp,
        "brent": brent,
        "vix": vix,
        "sp500": spx,
        "sp500_1y_high": spx_high,
        "sp500_drawdown_pct": drawdown,
        "fred": {
            "dgs10": fred_dgs10,
            "real10": fred_real10,
            "breakeven10": fred_be10,
        },
        "errors": errors,
    }
