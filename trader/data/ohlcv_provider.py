from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Iterable, List, Protocol

import pandas as pd

from trader.botstate_paths import get_ohlcv_cache_dir
from trader.time_utils import now_kst
from trader.universe.krx_safe import patch_pykrx_logging
from trader.utils.ohlcv import normalize_ohlcv

logger = logging.getLogger(__name__)


@dataclass
class OHLCVResult:
    df: pd.DataFrame
    meta: dict


class OHLCVProvider(Protocol):
    name: str

    def get_ohlcv(self, symbol: str, days: int) -> OHLCVResult: ...


class KISOHLCVProvider:
    name = "kis"

    def __init__(self, kis: object) -> None:
        self.kis = kis
        self._warned_keys: set[str] = set()

    def _warn_once(self, key: str, message: str, *args: object) -> None:
        if key in self._warned_keys:
            return
        self._warned_keys.add(key)
        logger.warning(message, *args)

    def get_ohlcv(self, symbol: str, days: int) -> OHLCVResult:
        try:
            candles = self.kis.get_daily_candles(symbol, count=max(days, 120))  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - network dependent
            self._warn_once(f"fail:{symbol}", "[OHLCV][KIS][FAIL] symbol=%s err=%s", symbol, exc)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": self.name, "error": str(exc), "volume_missing": True})

        if not candles:
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": self.name, "error": "empty", "volume_missing": True})

        df = pd.DataFrame(candles)
        raw_keys = list(df.columns)
        df_norm, meta = normalize_ohlcv(df)
        df_norm = df_norm.tail(days) if days else df_norm
        meta.update(
            {
                "provider": self.name,
                "source": self.name,
                "raw_keys": raw_keys,
                "rows": len(df_norm),
            }
        )
        return OHLCVResult(df_norm, meta)


class KRXOHLCVProvider:
    name = "krx"

    def __init__(self, *, lookback_pad_days: int = 40) -> None:
        self.lookback_pad_days = lookback_pad_days
        self._warned_keys: set[str] = set()

    def _warn_once(self, key: str, message: str, *args: object) -> None:
        if key in self._warned_keys:
            return
        self._warned_keys.add(key)
        logger.warning(message, *args)

    def _calc_window(self, days: int) -> tuple[str, str]:
        end = now_kst().date()
        back = max(120, days * 3) + self.lookback_pad_days
        start = end - timedelta(days=back)
        return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

    def get_ohlcv(self, symbol: str, days: int) -> OHLCVResult:
        try:
            from pykrx.stock import get_market_ohlcv_by_date
        except Exception as exc:  # pragma: no cover - import guard
            self._warn_once("import_fail", "[OHLCV][KRX][IMPORT_FAIL] symbol=%s err=%s", symbol, exc)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": self.name, "error": str(exc), "volume_missing": True})

        patch_pykrx_logging()
        start, end = self._calc_window(days)
        try:
            df_raw = get_market_ohlcv_by_date(start, end, symbol)
        except Exception as exc:  # pragma: no cover - network dependent
            self._warn_once(f"fail:{symbol}", "[OHLCV][KRX][FAIL] symbol=%s start=%s end=%s err=%s", symbol, start, end, exc)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": self.name, "error": str(exc), "volume_missing": True})

        if df_raw is None or df_raw.empty:
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": self.name, "error": "empty", "volume_missing": True})

        df = df_raw.reset_index()
        raw_keys = list(df.columns)
        df_norm, meta = normalize_ohlcv(df)
        df_norm = df_norm.tail(days) if days else df_norm
        meta.update(
            {
                "provider": self.name,
                "source": self.name,
                "raw_keys": raw_keys,
                "rows": len(df_norm),
            }
        )
        return OHLCVResult(df_norm, meta)


class ChainOHLCVProvider:
    def __init__(self, providers: Iterable[OHLCVProvider], *, env: str | None = None, cache_dir: Path | None = None) -> None:
        self.providers: List[OHLCVProvider] = list(providers)
        base_cache = cache_dir or get_ohlcv_cache_dir()
        self.cache_dir = base_cache / env if env else base_cache
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache: dict[tuple[str, int], OHLCVResult] = {}

    def _cache_path(self, symbol: str) -> Path:
        return self.cache_dir / f"{symbol}.parquet"

    def _load_cache(self, symbol: str, days: int) -> OHLCVResult | None:
        path = self._cache_path(symbol)
        if not path.exists():
            return None
        try:
            df = pd.read_parquet(path)
        except Exception as exc:  # pragma: no cover - corrupted cache
            logger.warning("[OHLCV][CACHE][READ_FAIL] path=%s err=%s", path, exc)
            return None
        df = df.sort_values("date")
        df = df.tail(days) if days else df
        volume_missing = "volume" not in df.columns or df["volume"].isna().all()
        meta = {"provider": "cache", "source": "cache", "cache_path": str(path), "rows": len(df), "volume_missing": volume_missing, "cache_hit": True}
        return OHLCVResult(df, meta)

    def _persist_cache(self, symbol: str, df: pd.DataFrame) -> None:
        path = self._cache_path(symbol)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            df.sort_values("date").to_parquet(path, index=False)
            logger.debug("[OHLCV][CACHE][WRITE] path=%s rows=%s", path, len(df))
        except Exception as exc:  # pragma: no cover - filesystem issues
            logger.warning("[OHLCV][CACHE][WRITE_FAIL] path=%s err=%s", path, exc)

    @staticmethod
    def _annotate_result(result: OHLCVResult, *, days: int) -> OHLCVResult:
        result.meta.setdefault("required_days", days)
        result.meta.setdefault("rows", len(result.df))
        result.meta["insufficient_candles"] = len(result.df) < days if days else False
        return result

    def get_ohlcv(self, symbol: str, days: int) -> OHLCVResult:
        errors: list[str] = []
        best: OHLCVResult | None = None
        memory_key = (symbol, days)
        cached = self._memory_cache.get(memory_key)
        if cached:
            return self._annotate_result(cached, days=days)
        cache_result = self._load_cache(symbol, days)
        if cache_result:
            best = self._annotate_result(cache_result, days=days)
            if not cache_result.meta.get("volume_missing") and cache_result.meta.get("rows", 0) >= days:
                self._memory_cache[memory_key] = cache_result
                return cache_result

        for provider in self.providers:
            try:
                result = provider.get_ohlcv(symbol, days)
            except Exception as exc:  # pragma: no cover - provider resilience
                errors.append(f"{provider.name}:exception:{exc}")
                logger.warning("[OHLCV][PROVIDER][FAIL] provider=%s symbol=%s err=%s", provider.name, symbol, exc)
                continue

            if result is None:
                continue

            result.meta.setdefault("provider", getattr(provider, "name", "unknown"))
            result.meta.setdefault("source", getattr(provider, "name", "unknown"))
            result = self._annotate_result(result, days=days)

            if result.df.empty:
                best = best or result
                continue

            volume_missing = bool(result.meta.get("volume_missing"))
            enough = len(result.df) >= days

            if not volume_missing and enough:
                result.meta["errors"] = errors
                if result.meta.get("provider") != "cache":
                    self._persist_cache(symbol, result.df)
                self._memory_cache[memory_key] = result
                return result

            if best is None:
                best = result
                continue

            best_volume_missing = bool(best.meta.get("volume_missing"))
            replace = False
            if best_volume_missing and not volume_missing:
                replace = True
            elif len(result.df) > len(best.df):
                replace = True
            if replace:
                best = result

        if best:
            best.meta["errors"] = errors
            if best.meta.get("provider") not in {"cache", None} and not best.df.empty:
                self._persist_cache(symbol, best.df)
            self._memory_cache[memory_key] = best
            return best

        result = OHLCVResult(pd.DataFrame(), {"provider": "none", "source": "none", "errors": errors, "volume_missing": True})
        result = self._annotate_result(result, days=days)
        self._memory_cache[memory_key] = result
        return result
