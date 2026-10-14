from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import quote_plus, quote
import xml.etree.ElementTree as ET

import requests


YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
USER_AGENT = "Mozilla/5.0 macro-market-monitor/1.0"


@dataclass(frozen=True)
class Quote:
    symbol: str
    value: float
    previous_close: float | None
    change_pct: float | None
    observed_at: str


@dataclass(frozen=True)
class Headline:
    title: str
    link: str
    published: str


def _request_json(url: str, *, params: dict | None = None, timeout: int = 15) -> dict:
    response = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.json()


def _normalise_tnx(value: float) -> float:
    # Yahoo normally reports ^TNX as the percentage yield (e.g. 4.95).
    # Some providers historically used 10x units; guard against that so a
    # provider-format change cannot create a false RED alert.
    if value > 15:
        value /= 10.0
    if not 0.1 <= value <= 15:
        raise ValueError(f"implausible US10Y value: {value}")
    return value


def yahoo_quote(symbol: str) -> Quote:
    data = _request_json(
        YAHOO_CHART.format(symbol=quote(symbol, safe="")),
        params={"range": "5d", "interval": "5m", "includePrePost": "true"},
    )
    result = data["chart"]["result"][0]
    meta = result.get("meta") or {}
    values = ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    timestamps = result.get("timestamp") or []
    valid = [(ts, val) for ts, val in zip(timestamps, values) if val is not None]
    if not valid:
        raise RuntimeError(f"no quote data for {symbol}")
    ts, raw = valid[-1]
    value = float(raw)
    previous = meta.get("chartPreviousClose")
    if previous is None:
        previous = meta.get("previousClose")
    previous_f = float(previous) if previous is not None else None

    if symbol == "^TNX":
        value = _normalise_tnx(value)
        previous_f = _normalise_tnx(previous_f) if previous_f is not None else None

    change_pct = None
    if previous_f not in (None, 0):
        change_pct = (value / previous_f - 1.0) * 100.0

    return Quote(
        symbol=symbol,
        value=value,
        previous_close=previous_f,
        change_pct=change_pct,
        observed_at=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
    )


def yahoo_daily_closes(symbol: str, *, range_: str = "1y") -> list[float]:
    data = _request_json(
        YAHOO_CHART.format(symbol=quote(symbol, safe="")),
        params={"range": range_, "interval": "1d"},
    )
    result = data["chart"]["result"][0]
    closes = ((result.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    clean = [float(x) for x in closes if x is not None]
    if symbol == "^TNX":
        clean = [_normalise_tnx(x) for x in clean]
    if not clean:
        raise RuntimeError(f"no daily close data for {symbol}")
    return clean


def consecutive_at_or_below(values: Iterable[float], threshold: float) -> int:
    count = 0
    for value in reversed(list(values)):
        if value <= threshold:
            count += 1
        else:
            break
    return count


def collect_headlines(limit: int = 8) -> list[Headline]:
    queries = (
        "Iran war Hormuz oil Reuters",
        "Federal Reserve 10-year Treasury yield Reuters",
        "US inflation CPI PPI FOMC Reuters",
    )
    seen: set[str] = set()
    headlines: list[Headline] = []
    for query_text in queries:
        try:
            response = requests.get(
                GOOGLE_NEWS_RSS,
                params={
                    "q": query_text,
                    "hl": "en-US",
                    "gl": "US",
                    "ceid": "US:en",
                },
                timeout=12,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            root = ET.fromstring(response.content)
            for item in root.findall("./channel/item"):
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                published = (item.findtext("pubDate") or "").strip()
                key = f"{title}|{link}"
                if not title or key in seen:
                    continue
                seen.add(key)
                headlines.append(Headline(title=title, link=link, published=published))
                if len(headlines) >= limit:
                    return headlines
        except Exception:
            # News is context only. Numeric monitoring must continue if RSS fails.
            continue
    return headlines


def collect_market_bundle() -> dict:
    quotes = {
        "us10y": yahoo_quote("^TNX"),
        "brent": yahoo_quote("BZ=F"),
        "sp500": yahoo_quote("^GSPC"),
        "nasdaq": yahoo_quote("^IXIC"),
        "vix": yahoo_quote("^VIX"),
    }
    sp_closes = yahoo_daily_closes("^GSPC", range_="1y")
    ten_year_closes = yahoo_daily_closes("^TNX", range_="1mo")
    sp_high = max(sp_closes)
    sp_drawdown = (quotes["sp500"].value / sp_high - 1.0) * 100.0
    below_470_days = consecutive_at_or_below(ten_year_closes, 4.70)
    return {
        "quotes": quotes,
        "sp500_high_1y": sp_high,
        "sp500_drawdown_pct": sp_drawdown,
        "us10y_below_470_days": below_470_days,
        "headlines": collect_headlines(),
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
