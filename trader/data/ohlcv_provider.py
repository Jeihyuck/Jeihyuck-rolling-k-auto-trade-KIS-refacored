from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Protocol

import pandas as pd

from trader.runtime_paths import get_ohlcv_cache_dir
from trader.time_utils import now_kst, prev_business_day
from trader.universe.krx_safe import patch_pykrx_logging
from trader.utils.ohlcv import normalize_ohlcv
from trader.db.engine import make_engine
from trader.db.repos import load_price_daily, upsert_price_daily
from trader.cache_ttl import daily_cache, DAILY_BAR_TTL_SEC
from trader.rate_limit import get_kis_gate
from trader.config import ALLOW_KIS_DAILY_FALLBACK, MARKET_MAP, is_diag_mode

logger = logging.getLogger(__name__)


@dataclass
class OHLCVResult:
    df: pd.DataFrame
    meta: dict


class OHLCVProvider(Protocol):
    name: str

    def get_ohlcv(self, symbol: str, days: int, *, purpose: str | None = None) -> OHLCVResult: ...


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

    def get_ohlcv(self, symbol: str, days: int, *, purpose: str | None = None) -> OHLCVResult:
        cache_key = ("daily", symbol, days)
        cached = daily_cache.get(cache_key)
        if cached:
            logger.debug("[OHLCV][CACHE][HIT] symbol=%s days=%d", symbol, days)
            return cached

        # DB 우선 조회
        try:
            engine = make_engine()
            end_date = now_kst().date()
            start_date = end_date - timedelta(days=max(days, 120))
            candles = load_price_daily(engine, symbol, start_date, end_date)
            if len(candles) >= days:
                df = pd.DataFrame(candles)
                raw_keys = list(df.columns)
                df_norm, meta = normalize_ohlcv(df)
                df_norm = df_norm.tail(days) if days else df_norm
                meta.update({
                    "provider": self.name,
                    "source": "db",
                    "raw_keys": raw_keys,
                    "rows": len(df_norm),
                })
                result = OHLCVResult(df_norm, meta)
                daily_cache.set(cache_key, result, DAILY_BAR_TTL_SEC)
                logger.info("[OHLCV][DB][HIT] symbol=%s days=%d rows=%d", symbol, days, len(df_norm))
                return result
            else:
                logger.debug("[OHLCV][DB][MISS] symbol=%s days=%d db_rows=%d", symbol, days, len(candles))
        except Exception as exc:
            logger.debug("[OHLCV][DB][ERROR] symbol=%s err=%s", symbol, exc)

        # ====================================================================
        # [PREFETCH_ONLY] 장중 외부 OHLCV 호출 차단
        # ====================================================================
        if os.getenv("OHLCV_PREFETCH_ONLY", "0") == "1":
            logger.debug(
                "[OHLCV][PREFETCH_ONLY] symbol=%s days=%d - external fetch blocked (OHLCV_PREFETCH_ONLY=1)",
                symbol, days
            )
            return OHLCVResult(
                pd.DataFrame(),
                {
                    "provider": self.name,
                    "source": "db_insufficient_prefetch_only",
                    "error": "ohlcv_prefetch_only_enabled",
                    "volume_missing": True,
                }
            )
        
        # ====================================================================
        # [DIAG 방화벽] DIAG 모드일 때는 KIS fallback 절대 금지
        # ====================================================================
        if is_diag_mode():
            logger.debug(
                "[OHLCV][DIAG][KIS_BLOCKED] symbol=%s days=%d - KIS fallback disabled in DIAG mode",
                symbol, days
            )
            return OHLCVResult(
                pd.DataFrame(),
                {
                    "provider": self.name,
                    "source": "db_failed_diag_no_kis",
                    "error": "diag_mode_kis_blocked",
                    "volume_missing": True,
                }
            )

        # KIS fallback (LIVE 모드에서만 실행됨)
        # [FIX] D. day window에서 days <= 120이면 fallback 허용 (watchlist 생성/엔트리에 필수)
        fallback_allowed = ALLOW_KIS_DAILY_FALLBACK or (days <= 120)
        if not fallback_allowed:
            logger.warning("[OHLCV][DB][NO_FALLBACK] symbol=%s days=%d", symbol, days)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": "db", "error": "no_fallback", "volume_missing": True})

        gate = get_kis_gate()
        if not gate.allow("inquire-daily"):
            logger.warning("[OHLCV][KIS][GATE_BLOCKED] symbol=%s", symbol)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": "kis", "error": "gate_blocked", "volume_missing": True})

        try:
            candles = self.kis.get_daily_candles(symbol, count=max(days, 120))  # type: ignore[attr-defined]
            logger.info("[OHLCV][KIS][FALLBACK] symbol=%s days=%d rows=%d", symbol, days, len(candles))
            # DB upsert for self-healing
            if candles:
                market = MARKET_MAP.get(symbol, "KOSPI")
                try:
                    upsert_price_daily(engine, candles, market, symbol)
                    logger.debug("[OHLCV][DB][UPSERT] symbol=%s rows=%d", symbol, len(candles))
                except Exception as exc:
                    logger.debug("[OHLCV][DB][UPSERT_FAIL] symbol=%s err=%s", symbol, exc)
        except Exception as exc:
            self._warn_once(f"fail:{symbol}", "[OHLCV][KIS][FAIL] symbol=%s err=%s", symbol, exc)
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": "kis", "error": str(exc), "volume_missing": True})

        if not candles:
            return OHLCVResult(pd.DataFrame(), {"provider": self.name, "source": "kis", "error": "empty", "volume_missing": True})

        df = pd.DataFrame(candles)
        raw_keys = list(df.columns)
        df_norm, meta = normalize_ohlcv(df)
        df_norm = df_norm.tail(days) if days else df_norm
        meta.update(
            {
                "provider": self.name,
                "source": "kis",
                "raw_keys": raw_keys,
                "rows": len(df_norm),
            }
        )
        result = OHLCVResult(df_norm, meta)
        daily_cache.set(cache_key, result, DAILY_BAR_TTL_SEC)
        return result


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

    def get_ohlcv(self, symbol: str, days: int, *, purpose: str | None = None) -> OHLCVResult:
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

    def fetch(self, code: str, start=None, end=None, *, as_of=None, days: int | None = None, **kwargs):
        """
        Backward-compatible adapter for older callers expecting provider.fetch(...).

        Candidate pool builder currently calls `provider.fetch` but this provider was refactored
        to expose `get_ohlcv` / `load` / `read_daily` etc. This adapter routes the call to the
        canonical implementation so build does not crash.
        """
        # Calculate days from start/end if not provided
        if days is None and start is not None and end is not None:
            from datetime import datetime
            if isinstance(start, str):
                start = datetime.strptime(start, "%Y%m%d").date()
            if isinstance(end, str):
                end = datetime.strptime(end, "%Y%m%d").date()
            days = (end - start).days
        
        # Default to 180 days if still not determined
        if days is None:
            days = 180
        
        # Route to get_ohlcv with symbol parameter
        result = self.get_ohlcv(symbol=code, days=days)
        
        # Return DataFrame (unwrap OHLCVResult for backward compatibility)
        return result.df if hasattr(result, 'df') else result


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

    def get_ohlcv(self, symbol: str, days: int, *, purpose: str | None = None) -> OHLCVResult:
        """
        OHLCV 데이터 조회 (DB-first + guarded remote fetch).
        
        Args:
            symbol: 종목코드
            days: 요청 일수
            purpose: 조회 목적 ("regime", "benchmark", "universe", None)
                     - regime/benchmark는 trade-tick에서도 긴 조회 허용 (DB 우선)
        """
        errors: list[str] = []
        best: OHLCVResult | None = None
        memory_key = (symbol, days)
        
        # 메모리 캐시 확인
        cached = self._memory_cache.get(memory_key)
        if cached:
            return self._annotate_result(cached, days=days)
        
        # 파일 캐시 확인 (DB-first)
        cache_result = self._load_cache(symbol, days)
        if cache_result:
            best = self._annotate_result(cache_result, days=days)
            if not cache_result.meta.get("volume_missing") and cache_result.meta.get("rows", 0) >= days:
                self._memory_cache[memory_key] = cache_result
                logger.info(
                    "[OHLCV][DB][HIT] symbol=%s days=%d rows=%d purpose=%s",
                    symbol, days, cache_result.meta.get("rows", 0), purpose or "universe"
                )
                return cache_result
        
        # ====================================================================
        # [TRADE-TICK GUARD] 긴 조회는 remote fetch 금지 (DB hit는 허용됨)
        # ====================================================================
        if os.getenv("MODE") == "trade" and days >= 260:
            # DB에 충분한 데이터가 있으면 이미 위에서 반환됨
            # 여기까지 왔다는 것은 DB 부족 → remote fetch 필요
            is_benchmark = purpose in {"regime", "benchmark"}
            
            # 레짐/벤치마크는 제한적 허용 (경고 로그)
            if is_benchmark:
                logger.warning(
                    "[OHLCV][TRADE][LONG_FETCH_ALLOWED] symbol=%s days=%d purpose=%s (regime/benchmark exception)",
                    symbol, days, purpose
                )
                # remote fetch 허용하지만 아래 provider 루프에서 시도
            else:
                # 일반 종목은 금지
                logger.error(
                    "[OHLCV][TRADE][LONG_FETCH_BLOCKED] symbol=%s days=%d purpose=%s",
                    symbol, days, purpose or "universe"
                )
                raise RuntimeError("TRADE_TICK_FORBIDS_LONG_OHLCV_FETCH")

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


def _recent_trading_dates(as_of: date, days: int) -> list[date]:
    dates: list[date] = []
    cursor = as_of
    while len(dates) < days:
        if cursor.weekday() >= 5:
            cursor = prev_business_day(cursor)
            continue
        dates.append(cursor)
        cursor = prev_business_day(cursor)
    return sorted(dates)


def upsert_ohlcv_delta(*, symbols: list[str], as_of: date, days: int = 1) -> dict:
    """Fetch recent trading days only and upsert to DB."""
    if os.getenv("MODE") == "trade" and days >= 260:
        raise RuntimeError("TRADE_TICK_FORBIDS_LONG_OHLCV_FETCH")
    if days < 1:
        return {"symbols": 0, "inserted": 0, "updated": 0, "dates": []}

    try:
        import FinanceDataReader as fdr
    except Exception as exc:
        raise RuntimeError(f"FinanceDataReader not available: {exc}")

    engine = make_engine()
    clean_symbols = [str(s).zfill(6) for s in symbols if s]
    dates = _recent_trading_dates(as_of, days)
    if not dates:
        return {"symbols": len(clean_symbols), "inserted": 0, "updated": 0, "dates": []}

    date_min = min(dates)
    date_max = max(dates)
    inserted = 0
    updated = 0

    for symbol in clean_symbols:
        df = fdr.DataReader(symbol, start=date_min, end=date_max)
        if df is None or df.empty:
            continue
        df = df.reset_index()
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].dt.date.isin(dates)]
        if df.empty:
            continue

        existing = load_price_daily(engine, symbol, date_min, date_max)
        existing_dates = {str(row.get("date"))[:10] for row in existing}

        candles = []
        for _, row in df.iterrows():
            dt_val = row.get("Date")
            if hasattr(dt_val, "date"):
                date_str = dt_val.date().isoformat()
            else:
                date_str = str(dt_val)[:10]
            candles.append(
                {
                    "date": date_str,
                    "open": float(row.get("Open", 0)),
                    "high": float(row.get("High", 0)),
                    "low": float(row.get("Low", 0)),
                    "close": float(row.get("Close", 0)),
                    "volume": float(row.get("Volume", 0)),
                }
            )

        if not candles:
            continue

        new_dates = {c["date"] for c in candles}
        inserted += len(new_dates - existing_dates)
        updated += len(new_dates & existing_dates)

        market = MARKET_MAP.get(symbol, "KOSPI")
        upsert_price_daily(engine, candles, market, symbol)

    logger.info(
        "[OHLCV][DELTA_UPSERT] symbols=%s days=%s dates=%s inserted=%s updated=%s",
        len(clean_symbols),
        days,
        [d.isoformat() for d in dates],
        inserted,
        updated,
    )
    return {"symbols": len(clean_symbols), "inserted": inserted, "updated": updated, "dates": dates}


def ensure_ohlcv_history(*, symbols: list[str], lookback_days: int = 520) -> None:
    """Ensure long OHLCV history exists. Weekend/base builds only."""
    mode = (os.getenv("MODE") or "").strip().lower()
    if mode in {"trade", "prep"}:
        raise RuntimeError("PREP_FORBIDS_OHLCV_HISTORY")
    from trader.ohlcv_prefetch import prefetch_ohlcv_to_db

    engine = make_engine()
    members = [{"code": str(s).zfill(6)} for s in symbols if s]
    if not members:
        return
    prefetch_ohlcv_to_db(
        engine=engine,
        members=members,
        days=lookback_days,
        force_rebuild=False,
        env=os.getenv("STRATEGY_ENV", "practice"),
        strategy=os.getenv("CANDIDATE_POOL_STRATEGY_KEY", "pb1_candidate_pool"),
        as_of=now_kst().date(),
    )
