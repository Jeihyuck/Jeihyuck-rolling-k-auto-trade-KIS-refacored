"""Audited 122630 loader: price_daily, then existing KIS provider, then CSV."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from trader.data.ohlcv_provider import KISOHLCVProvider
from trader.db.engine import make_engine
from trader.db.repos import load_price_daily, upsert_price_daily
from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)
FIELDS = ("date", "open", "high", "low", "close", "volume")


def validate(rows: list[dict]) -> tuple[list[dict], dict]:
    by_date = {}; duplicates = invalid = 0
    for raw in rows:
        try:
            d = str(raw["date"]).replace("-", "")
            o, h, lo, c = (float(raw[k]) for k in ("open", "high", "low", "close"))
            volume = float(raw.get("volume") or 0)
            if min(o, h, lo, c) <= 0 or lo > min(o, c) or h < max(o, c) or volume < 0: raise ValueError
            row = {"date": datetime.strptime(d, "%Y%m%d").date().isoformat(), "open": o,
                   "high": h, "low": lo, "close": c, "volume": volume}
        except (KeyError, TypeError, ValueError):
            invalid += 1; continue
        if row["date"] in by_date: duplicates += 1
        by_date[row["date"]] = row
    clean = [by_date[k] for k in sorted(by_date)]
    weekdays = set()
    if clean:
        cursor, end = date.fromisoformat(clean[0]["date"]), date.fromisoformat(clean[-1]["date"])
        while cursor <= end:
            if cursor.weekday() < 5: weekdays.add(cursor.isoformat())
            cursor += timedelta(days=1)
    missing = len(weekdays - set(by_date))  # audit only; includes KRX holidays
    return clean, {"duplicate_count": duplicates, "invalid_ohlc_count": invalid,
                   "missing_date_count": missing}


def load(*, symbol: str, source: str, start: date, end: date, csv_path: str | None = None):
    errors = []; rows = []; used = None; adjusted = "UNKNOWN"
    if source in {"auto", "db"}:
        try:
            rows = load_price_daily(make_engine(), symbol, start, end); used = "price_daily"
        except Exception as exc: errors.append(f"DB:{type(exc).__name__}:{exc}")
    minimum = max(252, int((end - start).days * .55))
    if source in {"auto", "kis"} and len(rows) < minimum:
        try:
            result = KISOHLCVProvider(KisAPI()).get_ohlcv(symbol, max(260, (end-start).days), purpose="replay")
            fetched = result.df.reset_index().rename(columns={result.df.index.name or "index": "date"}).to_dict("records")
            if fetched:
                upsert_price_daily(make_engine(), fetched, "KOSPI", symbol)
                rows = load_price_daily(make_engine(), symbol, start, end); used = "kis+price_daily"
                adjusted = "KIS_PROVIDER_UNCONFIRMED"
        except Exception as exc: errors.append(f"KIS:{type(exc).__name__}:{exc}")
    if not rows and csv_path and source in {"auto", "csv"}:
        with open(csv_path, newline="", encoding="utf-8") as handle: rows = list(csv.DictReader(handle))
        used, adjusted = "user_csv", "USER_DECLARED_UNCONFIRMED"
    clean, audit = validate(rows)
    payload = json.dumps(clean, sort_keys=True, separators=(",", ":")).encode()
    meta = {"source_used": used, "data_start": clean[0]["date"] if clean else None,
            "data_end": clean[-1]["date"] if clean else None, "row_count": len(clean), **audit,
            "adjusted_price": adjusted, "data_quality": "OK" if len(clean) >= minimum else "BLOCKED_INSUFFICIENT_HISTORY",
            "checksum": hashlib.sha256(payload).hexdigest(), "errors": errors,
            "corporate_actions": "No adjustment factor is synthesized."}
    logger.info("[KR_INF][DATA] %s", json.dumps(meta, ensure_ascii=False))
    return clean, meta


def main(argv=None) -> int:
    p = argparse.ArgumentParser(); p.add_argument("--symbol", default="122630"); p.add_argument("--source", choices=("auto","db","kis","csv"), default="auto")
    p.add_argument("--from", dest="start", default="2010-01-01"); p.add_argument("--to", dest="end", default=date.today().isoformat())
    p.add_argument("--csv"); p.add_argument("--output", default="data/kr_infinite/122630_adjusted_daily.csv")
    a = p.parse_args(argv); rows, meta = load(symbol=a.symbol, source=a.source, start=date.fromisoformat(a.start), end=date.fromisoformat(a.end), csv_path=a.csv)
    out = Path(a.output); out.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with out.open("w", newline="", encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)
    out.with_suffix(out.suffix+".metadata.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return 0 if meta["data_quality"] == "OK" else 2


if __name__ == "__main__": raise SystemExit(main())
