from __future__ import annotations

import argparse
import json
import logging

import pandas as pd

from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.kis_wrapper import KisAPI

logger = logging.getLogger(__name__)


def _volume_stats(df: pd.DataFrame) -> dict:
    if df is None or df.empty or "volume" not in df.columns:
        return {"rows": len(df) if df is not None else 0, "volume_missing": True}
    vols = df["volume"].dropna()
    zeros = int((df["volume"] == 0).sum())
    return {
        "rows": len(df),
        "volume_missing": vols.empty,
        "non_null": len(vols),
        "zero_count": zeros,
        "min": float(vols.min()) if not vols.empty else None,
        "max": float(vols.max()) if not vols.empty else None,
        "mean": float(vols.mean()) if not vols.empty else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OHLCV data probe")
    parser.add_argument("--env", required=True, help="Environment (practice/real)")
    parser.add_argument("--symbol", required=True, help="Symbol code (e.g., 005930)")
    parser.add_argument("--days", type=int, default=120, help="Lookback days to fetch")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    env = args.env.lower()
    symbol = str(args.symbol).strip().lstrip("A").zfill(6)
    days = max(1, int(args.days))

    try:
        kis = KisAPI(env=env)
    except Exception as exc:
        logger.warning("[DATA-PROBE][KIS_INIT_FAIL] env=%s err=%s", env, exc)
        kis = None

    providers = [KISOHLCVProvider(kis)] if kis else []
    providers.append(KRXOHLCVProvider())
    chain = ChainOHLCVProvider(providers, env=env)

    result = chain.get_ohlcv(symbol, days)
    df = result.df
    meta = result.meta or {}

    logger.info(
        "[DATA-PROBE][SUMMARY] env=%s symbol=%s provider=%s rows=%s volume_missing=%s cache=%s",
        env,
        symbol,
        meta.get("provider") or meta.get("source"),
        len(df) if df is not None else 0,
        meta.get("volume_missing"),
        meta.get("cache_path") if meta.get("provider") == "cache" else None,
    )

    payload = {
        "env": env,
        "symbol": symbol,
        "days": days,
        "meta": meta,
        "volume_stats": _volume_stats(df if isinstance(df, pd.DataFrame) else pd.DataFrame()),
        "ohlcv_head": df.head(3).to_dict(orient="records") if isinstance(df, pd.DataFrame) and not df.empty else [],
        "ohlcv_tail": df.tail(3).to_dict(orient="records") if isinstance(df, pd.DataFrame) and not df.empty else [],
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
