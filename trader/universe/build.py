from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Iterable


from trader.config import (
    PB1_MIN_CANDLES,
)
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.time_utils import now_kst
from trader.universe.capabilities import providers_for_env
from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider
from trader.universe.validation import validate_listed_and_tradeable
from trader.runtime_paths import ensure_not_repo_tracked_path, runtime_path, runtime_root
from trader.universe.mode import is_db_only_mode, resolve_strategy_mode

from datetime import date


def _parse_as_of_date(val: str) -> date | None:
    """Best-effort parse for YYYY-MM-DD or YYYYMMDD."""
    from datetime import datetime as _datetime

    if not val:
        return None
    s = str(val).strip()
    try:
        if "-" in s:
            return _datetime.fromisoformat(s).date()
        if len(s) == 8 and s.isdigit():
            return _datetime.strptime(s, "%Y%m%d").date()
        return _datetime.fromisoformat(s).date()
    except Exception:
        return None


def _prefer_provider_order(chain: list[str]) -> list[str]:
    """Prefer live providers first, then static, then emergency seed."""
    preferred = [
        "kis_marketcap_top",
        "seed_static",
        "emergency_seed",
    ]
    ordered = [name for name in preferred if name in chain]
    tail = [name for name in chain if name not in ordered]
    return ordered + tail

logger = logging.getLogger(__name__)


def _resolve_seed_dir() -> Path:
    default_dir = runtime_root() / "diagnostics" / "universe_seed"
    seed_dir = Path(os.getenv("UNIVERSE_SEED_DIR", str(default_dir)))
    seed_dir.mkdir(parents=True, exist_ok=True)
    return seed_dir


SEED_DIR = _resolve_seed_dir()
SEED_PATH_KOSPI = SEED_DIR / "kospi_mcap_100.csv"
SEED_PATH_KOSDAQ = SEED_DIR / "kosdaq_mcap_100.csv"
TARGETS = {"KOSPI": 150, "KOSDAQ": 150}
DEFAULT_PROVIDER = "kis_marketcap_top"
TICKER_PATTERN = re.compile(r"^\d{6}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and persist universe members")
    parser.add_argument("--env", required=True, help="Environment (practice/real)")
    parser.add_argument("--strategy", required=True, help="Strategy name to store universe for")
    parser.add_argument("--date", help="As-of date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Universe provider name")
    return parser.parse_args()


def _normalize_payload(payload: dict | None) -> dict:
    payload = payload or {}
    selected = payload.get("selected") or []
    selected_stocks = payload.get("selected_stocks") or selected
    selected_by_market = payload.get("selected_by_market") or {}
    normalized = {
        "selected": list(selected),
        "selected_stocks": list(selected_stocks),
        "selected_by_market": dict(selected_by_market),
    }
    if "weight_scope" in payload:
        normalized["weight_scope"] = payload.get("weight_scope")
    return normalized


def _build_members_from_payload(selected_by_market: dict | None) -> list[dict]:
    members: list[dict] = []
    for market, rows in (selected_by_market or {}).items():
        for idx, row in enumerate(rows or []):
            code = str(row.get("code") or row.get("pdno") or "").zfill(6)
            if not code:
                continue
            members.append(
                {
                    "code": code,
                    "market": market,
                    "weight": row.get("weight") or row.get("target_weight") or row.get("weight_pct"),
                    "rank": row.get("rank") or idx + 1,
                    "meta_json": row,
                }
            )
    return members


def _dedup(seq: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    uniq: list[str] = []
    for val in seq:
        if val not in seen:
            seen.add(val)
            uniq.append(val)
    return uniq


def _filter_tradeable_members(
    members: list[dict],
    *,
    kis: KisAPI | None,
) -> tuple[list[dict], list[dict]]:
    if not kis:
        logger.warning("[UNIVERSE][TRADEABLE][SKIP] kis_unavailable=1")
        return members, []
    kept: list[dict] = []
    dropped: list[dict] = []
    total = len(members)
    progress_every = max(1, total // 10)
    ts0 = time.monotonic()
    for idx, member in enumerate(members, start=1):
        code = str(member.get("code") or "").zfill(6)
        ok, reason = validate_listed_and_tradeable(kis, code)
        if ok:
            kept.append(member)
        else:
            dropped.append({"code": code, "reason": f"not_tradeable:{reason}"})
        if idx == 1 or idx % progress_every == 0 or idx == total:
            logger.info(
                "[UNIVERSE][TRADEABLE][PROGRESS] processed=%s/%s kept=%s dropped=%s elapsed=%.1fs",
                idx,
                total,
                len(kept),
                len(dropped),
                time.monotonic() - ts0,
            )
    return kept, dropped


def _load_fallback_universe_from_db(
    *,
    repo: UniverseRepo,
    env: str,
    strategy: str,
    as_of_date: str,
) -> list[dict]:
    # Check if fallback is disabled
    if os.getenv("UNIVERSE_NO_FALLBACK", "0") == "1":
        logger.error("[UNIVERSE][NO_FALLBACK] fallback disabled by env")
        return []
    
    snapshot = repo.get_latest_successful_universe_snapshot(env=env, strategy=strategy, as_of_date=as_of_date)
    if not snapshot:
        logger.error(
            "[UNIVERSE][FALLBACK][DB][MISS] env=%s strategy=%s as_of=%s",
            env,
            strategy,
            as_of_date,
        )
        return []
    logger.warning(
        "[UNIVERSE][FALLBACK][DB] using run_id=%s as_of=%s members=%s",
        snapshot.get("run_id"),
        snapshot.get("as_of"),
        snapshot.get("members_count"),
    )
    return snapshot.get("members") or []




def _load_seed_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    try:
        with path.open(encoding="utf-8") as f:
            # CSV with header: code,name
            reader = csv.DictReader(f)
            has_code_header = reader.fieldnames and any(fn.lower() == "code" for fn in reader.fieldnames)
            if has_code_header:
                for row in reader:
                    code = str(row.get("code") or "").strip()
                    if not code:
                        continue
                    rows.append({"code": code.zfill(6), "name": (row.get("name") or "").strip() or None})
            else:
                f.seek(0)
                for line in f:
                    code = line.strip()
                    if code:
                        rows.append({"code": code.zfill(6), "name": None})
    except Exception:
        logger.exception("[UNIVERSE][FALLBACK][STATIC][READ_FAIL] path=%s", path)
        return []
    seen: set[str] = set()
    uniq_rows: list[dict] = []
    for row in rows:
        code = row.get("code")
        if not code or code in seen:
            continue
        seen.add(code)
        uniq_rows.append(row)
    return uniq_rows


def _sanitize_members(
    members: list[dict],
    *,
    ohlcv_provider: ChainOHLCVProvider | None = None,
    min_candles: int = 0,
) -> tuple[list[dict], dict[str, int], dict]:
    stats = {"total_raw": len(members), "invalid_format": 0, "insufficient_history": 0, "final": 0}
    sanitized: list[dict] = []
    invalid_format_codes: list[str] = []
    insufficient_history_codes: list[str] = []
    dropped: list[dict] = []
    total = len(members)
    progress_every = max(1, total // 10)
    ts0 = time.monotonic()
    for idx, member in enumerate(members, start=1):
        raw_code = str(member.get("code") or member.get("pdno") or "").strip()
        code = raw_code.zfill(6) if raw_code.isdigit() else raw_code
        if not TICKER_PATTERN.match(code):
            stats["invalid_format"] += 1
            if raw_code:
                invalid_format_codes.append(raw_code)
                dropped.append({"code": raw_code, "reason": "invalid_format"})
            continue
        member["code"] = code
        if ohlcv_provider and min_candles > 0:
            try:
                result = ohlcv_provider.get_ohlcv(code, min_candles)
            except Exception:
                logger.exception("[UNIVERSE][VALIDATE][FAIL] code=%s", code)
                stats["insufficient_history"] += 1
                insufficient_history_codes.append(code)
                dropped.append({"code": code, "reason": "insufficient_history"})
                continue
            if result.meta.get("insufficient_candles") or result.meta.get("rows", 0) < min_candles:
                stats["insufficient_history"] += 1
                insufficient_history_codes.append(code)
                dropped.append({"code": code, "reason": "insufficient_history"})
                continue
        sanitized.append(member)
        if idx == 1 or idx % progress_every == 0 or idx == total:
            logger.info(
                "[UNIVERSE][SANITIZE][PROGRESS] processed=%s/%s kept=%s invalid_format=%s insufficient_history=%s elapsed=%.1fs",
                idx,
                total,
                len(sanitized),
                stats["invalid_format"],
                stats["insufficient_history"],
                time.monotonic() - ts0,
            )
    stats["final"] = len(sanitized)
    return (
        sanitized,
        stats,
        {
            "invalid_format_codes": invalid_format_codes,
            "insufficient_history_codes": insufficient_history_codes,
            "kept": [m.get("code") for m in sanitized],
            "dropped": dropped,
        },
    )


def _write_universe_sanitize_report(*, as_of_date: str, keep_codes: list[str], dropped: list[dict]) -> Path:
    path = runtime_path("runtime", f"universe_sanitize_{as_of_date}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"as_of": as_of_date, "kept": keep_codes, "dropped": dropped}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[UNIVERSE][SANITIZE][SAVE] path=%s kept=%s dropped=%s", path, len(keep_codes), len(dropped))
    return path


def _write_universe_drop_diagnostics(
    *,
    as_of_date: str,
    source: str,
    invalid_format_codes: list[str],
    insufficient_history_codes: list[str],
) -> Path:
    payload = {
        "invalid_format": list(invalid_format_codes or []),
        "insufficient_history": list(insufficient_history_codes or []),
        "source": source,
        "as_of": as_of_date,
    }
    path = runtime_path("runtime", "diagnostics", f"universe_drop_{as_of_date}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "[UNIVERSE][SANITIZE][DIAGNOSTICS] path=%s invalid_format=%s insufficient_history=%s",
        path,
        len(payload["invalid_format"]),
        len(payload["insufficient_history"]),
    )
    return path


def _write_seed_rows(path: Path, rows: Iterable[dict]) -> None:
    try:
        ensure_not_repo_tracked_path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["code", "name"])
            writer.writeheader()
            for row in rows:
                code = str(row.get("code") or "").zfill(6)
                if not code:
                    continue
                writer.writerow({"code": code, "name": (row.get("name") or "").strip()})
    except Exception:
        logger.exception("[UNIVERSE][SEED][WRITE_FAIL] path=%s", path)
        raise


def _load_static_seed() -> dict | None:
    kospi_rows = _load_seed_rows(SEED_PATH_KOSPI)
    kosdaq_rows = _load_seed_rows(SEED_PATH_KOSDAQ)

    members: list[dict] = []
    markets: dict[str, list[dict]] = {}

    seen: set[str] = set()

    for row in kospi_rows[: TARGETS["KOSPI"]]:
        code = row.get("code")
        if not code or code in seen:
            continue
        seen.add(code)
        rank = len(markets.get("KOSPI", [])) + 1
        member = {"code": code, "market": "KOSPI", "weight": None, "rank": rank, "meta_json": {"name": row.get("name")}}
        members.append(member)
        markets.setdefault("KOSPI", []).append({"code": code, "rank": rank, "name": row.get("name")})

    for row in kosdaq_rows[: TARGETS["KOSDAQ"]]:
        code = row.get("code")
        if not code or code in seen:
            continue
        seen.add(code)
        rank = len(markets.get("KOSDAQ", [])) + 1
        member = {"code": code, "market": "KOSDAQ", "weight": None, "rank": rank, "meta_json": {"name": row.get("name")}}
        members.append(member)
        markets.setdefault("KOSDAQ", []).append({"code": code, "rank": rank, "name": row.get("name")})

    if not members:
        return None

    payload = _normalize_payload(
        {
            "selected": [m["code"] for m in members],
            "selected_by_market": markets,
        }
    )
    return {
        "payload": payload,
        "members": members,
        "source": "fallback:static",
        "params": {
            "seed_path_kospi": str(SEED_PATH_KOSPI),
            "seed_path_kosdaq": str(SEED_PATH_KOSDAQ),
        },
    }


def _load_emergency_seed() -> dict | None:
    emergency_path = Path("data") / "universe_seed.csv"
    rows = _load_seed_rows(emergency_path)
    if not rows:
        return None

    selected_codes = [str(row.get("code") or "").zfill(6) for row in rows if row.get("code")]
    selected_codes = _dedup([code for code in selected_codes if code])
    target_total = TARGETS["KOSPI"] + TARGETS["KOSDAQ"]
    selected_codes = selected_codes[:target_total]

    members: list[dict] = []
    selected_by_market: dict[str, list[dict]] = {"KOSPI": [], "KOSDAQ": []}
    split = TARGETS["KOSPI"]
    for idx, code in enumerate(selected_codes, start=1):
        market = "KOSPI" if idx <= split else "KOSDAQ"
        rank = len(selected_by_market[market]) + 1
        member = {
            "code": code,
            "market": market,
            "weight": None,
            "rank": rank,
            "meta_json": {"name": None, "source": "emergency_seed"},
        }
        members.append(member)
        selected_by_market[market].append({"code": code, "rank": rank, "name": None})

    if not members:
        return None

    payload = _normalize_payload(
        {
            "selected": [m["code"] for m in members],
            "selected_by_market": selected_by_market,
        }
    )
    return {
        "payload": payload,
        "members": members,
        "source": "fallback:emergency_seed",
        "params": {
            "path": str(emergency_path),
            "target_total": target_total,
        },
    }


def _build_from_kis(provider: KISMarketcapTopProvider, as_of_date: str) -> tuple[dict | None, str]:
    try:
        kospi_rows = provider.get_marketcap_top_with_meta("KOSPI", TARGETS["KOSPI"])
        kosdaq_rows = provider.get_marketcap_top_with_meta("KOSDAQ", TARGETS["KOSDAQ"])
    except Exception as exc:  # pragma: no cover - network/remote failure
        logger.warning("[UNIVERSE][KIS][FAIL] %s", exc)
        return None, str(exc)

    if len(kospi_rows) < TARGETS["KOSPI"] or len(kosdaq_rows) < TARGETS["KOSDAQ"]:
        reason = f"insufficient kospi={len(kospi_rows)} kosdaq={len(kosdaq_rows)}"
        logger.warning("[UNIVERSE][KIS][INSUFFICIENT] %s target=%s", reason, TARGETS)
        return None, reason

    kospi_rows = kospi_rows[: TARGETS["KOSPI"]]
    kosdaq_rows = kosdaq_rows[: TARGETS["KOSDAQ"]]
    _write_seed_rows(SEED_PATH_KOSPI, kospi_rows)
    _write_seed_rows(SEED_PATH_KOSDAQ, kosdaq_rows)

    selected_by_market = {
        "KOSPI": [{"code": row["code"], "rank": idx + 1, "name": row.get("name")} for idx, row in enumerate(kospi_rows)],
        "KOSDAQ": [{"code": row["code"], "rank": idx + 1, "name": row.get("name")} for idx, row in enumerate(kosdaq_rows)],
    }
    payload = _normalize_payload(
        {
            "selected": [row["code"] for row in kospi_rows + kosdaq_rows],
            "selected_by_market": selected_by_market,
        }
    )
    members = _build_members_from_payload(selected_by_market)
    return (
        {
            "payload": payload,
            "members": members,
            "source": "kis_marketcap_top",
            "params": {"targets": TARGETS, "as_of": as_of_date},
        },
        "kis_marketcap_top",
    )


def _build_from_fdr(as_of_date: str, targets: dict[str, int]) -> tuple[dict | None, str]:
    _ = as_of_date, targets
    return None, "fdr_disabled"


def _build_from_fdr_kospi100(as_of_date: str, target: int = 100) -> tuple[dict | None, str]:
    _ = as_of_date, target
    return None, "fdr_disabled"


def build_universe(as_of_date: str, env: str, strategy: str, provider_override: str | None = None) -> list[dict]:
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    payload: dict | None = None
    members: list[dict] = []
    source = provider_override or DEFAULT_PROVIDER
    params: dict = {"as_of": as_of_date}
    last_reason: str | None = None

    logger.info(
        "[UNIVERSE][BUILD][START] env=%s strategy=%s as_of=%s provider=%s",
        env,
        strategy,
        as_of_date,
        provider_override or DEFAULT_PROVIDER,
    )

    # Start run in RUNNING status
    run_id = None
    try:
        run_id = repo.start_universe_run(env=env, strategy=strategy, as_of_date=as_of_date, provider=source)
        logger.info("[UNIVERSE][RUN][STARTED] run_id=%s", run_id)
    except Exception as exc:
        logger.warning("[UNIVERSE][RUN][START_FAIL] err=%s", exc)
        # Continue anyway - operations below will fail gracefully if run_id is None

    allowed_chain = providers_for_env(env)
    if provider_override:
        if provider_override not in allowed_chain:
            logger.info("[UNIVERSE][CHAIN][OVERRIDE] env=%s provider=%s", env, provider_override)
        allowed_chain = [provider_override]
    preferred_chain = _prefer_provider_order(allowed_chain)
    if preferred_chain != list(allowed_chain):
        logger.info(
            "[UNIVERSE][CHAIN] env=%s strategy=%s providers=%s preferred=%s",
            env,
            strategy,
            allowed_chain,
            preferred_chain,
        )
    else:
        logger.info("[UNIVERSE][CHAIN] env=%s strategy=%s providers=%s", env, strategy, allowed_chain)

    kis_provider: KISMarketcapTopProvider | None = None
    if "kis_marketcap_top" in allowed_chain:
        try:
            kis_provider = KISMarketcapTopProvider(kis=KisAPI(), env=env)
        except Exception as exc:
            last_reason = f"kis_init_fail:{exc}"
            logger.warning("[UNIVERSE][KIS][INIT_FAIL] env=%s err=%s", env, exc)

    today = now_kst().date()
    as_of_day = _parse_as_of_date(as_of_date) or today
    provider_total = len(preferred_chain)
    for provider_idx, provider_name in enumerate(preferred_chain, start=1):
        result: dict | None = None
        reason: str | None = None
        logger.info(
            "[UNIVERSE][PROVIDER][PROGRESS] step=%s/%s provider=%s",
            provider_idx,
            provider_total,
            provider_name,
        )

        if provider_name == "kis_marketcap_top":
            if not kis_provider:
                reason = last_reason or "kis_provider_unavailable"
            else:
                result, reason = _build_from_kis(kis_provider, as_of_date)
        elif provider_name == "seed_static":
            result = _load_static_seed()
            reason = "seed_static" if result else "seed_missing"
        elif provider_name == "emergency_seed":
            result = _load_emergency_seed()
            reason = "emergency_seed" if result else "emergency_seed_missing"
        else:
            logger.warning("[UNIVERSE][PROVIDER][UNKNOWN] env=%s provider=%s", env, provider_name)
            continue

        if result:
            payload = result["payload"]
            members = result["members"]
            source = result.get("source", provider_name)
            params.update(result.get("params") or {})
            last_reason = reason or provider_name
            logger.info("[UNIVERSE][PROVIDER][SUCCESS] env=%s provider=%s reason=%s members=%s", env, provider_name, last_reason, len(members))
            break

        last_reason = reason or provider_name
        logger.warning("[UNIVERSE][PROVIDER][FAIL] env=%s provider=%s reason=%s", env, provider_name, last_reason)

    validate_history = os.getenv("UNIVERSE_VALIDATE_OHLCV", "1").lower() in {"1", "true", "yes", "on"}
    min_candles = int(os.getenv("UNIVERSE_MIN_CANDLES", str(max(PB1_MIN_CANDLES, 50))))
    
    # [FIX] OHLCV provider를 KIS 기반으로 변경 (KIS → KRX fallback)
    ohlcv_provider = None
    if validate_history:
        try:
            kis_for_ohlcv = kis_provider.kis if kis_provider else KisAPI()
            ohlcv_provider = ChainOHLCVProvider([KISOHLCVProvider(kis_for_ohlcv), KRXOHLCVProvider()], env=env)
        except Exception as exc:
            logger.warning("[UNIVERSE][OHLCV][INIT_FAIL] falling back to KRX-only: %s", exc)
            ohlcv_provider = ChainOHLCVProvider([KRXOHLCVProvider()], env=env)
    
    logger.info(
        "[UNIVERSE][SANITIZE][START] members=%s validate_history=%s min_candles=%s",
        len(members),
        int(validate_history),
        min_candles,
    )
    members, stats, sanitize_detail = _sanitize_members(members, ohlcv_provider=ohlcv_provider, min_candles=min_candles)
    logger.info(
        "[UNIVERSE][SANITIZE] total_raw=%s invalid_format=%s insufficient_history=%s final=%s",
        stats["total_raw"],
        stats["invalid_format"],
        stats["insufficient_history"],
        stats["final"],
    )
    invalid_codes = sanitize_detail.get("invalid_format_codes") or []
    insufficient_codes = sanitize_detail.get("insufficient_history_codes") or []
    if invalid_codes:
        logger.info(
            "[UNIVERSE][DROP] reason=invalid_format count=%s sample=%s",
            len(invalid_codes),
            invalid_codes[: min(5, len(invalid_codes))],
        )
    if insufficient_codes:
        logger.info(
            "[UNIVERSE][DROP] reason=insufficient_history count=%s sample=%s",
            len(insufficient_codes),
            insufficient_codes[: min(5, len(insufficient_codes))],
        )

    validate_kis = os.getenv("UNIVERSE_VALIDATE_WITH_KIS", "0").lower() in {"1", "true", "yes", "on"}
    kis_validator: KisAPI | None = None
    if validate_kis:
        try:
            kis_validator = kis_provider.kis if kis_provider else KisAPI(kis_env=env)
        except Exception as exc:
            logger.warning("[UNIVERSE][TRADEABLE][INIT_FAIL] env=%s err=%s", env, exc)
            kis_validator = None
    if validate_kis:
        logger.info("[UNIVERSE][TRADEABLE][START] members=%s", len(members))
        members, dropped_tradeable = _filter_tradeable_members(members, kis=kis_validator)
        if dropped_tradeable:
            stats["not_tradeable"] = len(dropped_tradeable)
            sanitize_detail["dropped"].extend(dropped_tradeable)
            sanitize_detail["kept"] = [m.get("code") for m in members]
            logger.info(
                "[UNIVERSE][DROP] reason=not_tradeable count=%s sample=%s",
                len(dropped_tradeable),
                dropped_tradeable[: min(5, len(dropped_tradeable))],
            )

    if not members:
        logger.error("[UNIVERSE][EMPTY] as_of=%s env=%s strategy=%s source=%s", as_of_date, env, strategy, source)
        error_reason = f"provider_empty:{last_reason or source}"
        try:
            if run_id and hasattr(repo, "record_universe_run_failure"):
                repo.record_universe_run_failure(
                    run_id=run_id,
                    env=env,
                    strategy=strategy,
                    as_of_date=as_of_date,
                    provider=source,
                    error_reason=error_reason,
                    members_count=0,
                )
            else:
                logger.warning("[UNIVERSE][RUN][FAIL_LOG][SKIP] run_id=%s not available or method missing", run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][FAIL_LOG] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
        allow_fallback = is_db_only_mode() or resolve_strategy_mode() == "LIVE"
        if allow_fallback:
            fallback_members = _load_fallback_universe_from_db(
                repo=repo,
                env=env,
                strategy=strategy,
                as_of_date=as_of_date,
            )
            if fallback_members:
                return fallback_members
        raise RuntimeError("universe_members_empty")

    try:
        if not run_id:
            logger.warning("[UNIVERSE][STORE][SKIP] run_id not available, skipping snapshot")
            raise RuntimeError("run_id_not_available")
        repo.store_universe_snapshot(
            run_id=run_id,
            env=env,
            strategy=strategy,
            as_of_date=as_of_date,
            provider=source,
            members=members,
            reason=last_reason,
        )
    except Exception as exc:
        error_reason = f"store_fail:{exc}"
        try:
            if run_id and hasattr(repo, "record_universe_run_failure"):
                repo.record_universe_run_failure(
                    run_id=run_id,
                    env=env,
                    strategy=strategy,
                    as_of_date=as_of_date,
                    provider=source,
                    error_reason=error_reason,
                    members_count=len(members),
                )
            else:
                logger.warning("[UNIVERSE][RUN][FAIL_LOG][SKIP] run_id=%s not available or method missing", run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][FAIL_LOG] env=%s strategy=%s as_of=%s", env, strategy, as_of_date)
        raise
    repo.cleanup_old_runs(retain_days=int(os.getenv("UNIVERSE_RETENTION_DAYS", "30")))
    logger.info(
        "[UNIVERSE][BUILT] env=%s strategy=%s as_of=%s members=%s source=%s reason=%s",
        env,
        strategy,
        as_of_date,
        len(members),
        source,
        last_reason or "n/a",
    )
    _write_universe_sanitize_report(
        as_of_date=as_of_date,
        keep_codes=sanitize_detail.get("kept") or [],
        dropped=sanitize_detail.get("dropped") or [],
    )
    _write_universe_drop_diagnostics(
        as_of_date=as_of_date,
        source=source,
        invalid_format_codes=sanitize_detail.get("invalid_format_codes") or [],
        insufficient_history_codes=sanitize_detail.get("insufficient_history_codes") or [],
    )
    return members


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    as_of = args.date or now_kst().date().isoformat()
    build_universe(as_of_date=as_of, env=args.env, strategy=args.strategy, provider_override=args.provider)


if __name__ == "__main__":
    main()
