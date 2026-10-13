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
from trader.universe.providers.krx_marketcap_asof import KRXAsOfMarketcapProvider
from trader.universe.validation import validate_listed_and_tradeable
from trader.runtime_paths import ensure_not_repo_tracked_path, runtime_path, runtime_root
from trader.universe.mode import is_db_only_mode, resolve_strategy_mode
from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance

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
    parser.add_argument("--provider", default=None, help="Universe provider name")
    parser.add_argument("--prep-recovery", action="store_true", help="Require exact-date KRX universe and PREP safety gates")
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
    snapshot: dict | None = None,
    require_exact_as_of: bool = False,
    enforce_fallback_switch: bool = True,
) -> list[dict]:
    """Use only a committed, venue-verified DB snapshot; never guess KRX venues.

    PREP recovery requires the *exact* last completed trading session and >=100
    distinct symbols.  Historical snapshots from a different session are not
    silently relabeled as today's inputs.
    """
    if enforce_fallback_switch and os.getenv("UNIVERSE_NO_FALLBACK", "0") == "1":
        logger.error("[UNIVERSE][NO_FALLBACK] fallback disabled by env")
        return []

    snapshot = snapshot if snapshot is not None else repo.get_latest_successful_universe_snapshot(
        env=env, strategy=strategy, as_of_date=as_of_date,
    )
    if not snapshot or str(snapshot.get("status") or "").upper() != "SUCCESS":
        logger.error("[UNIVERSE][FALLBACK][DB][NO_SUCCESS] env=%s as_of=%s", env, as_of_date)
        return []
    snapshot_as_of = str(snapshot.get("as_of") or "")
    if require_exact_as_of and snapshot_as_of != as_of_date:
        logger.error(
            "[UNIVERSE][FALLBACK][DB][STALE] expected=%s stored=%s action=FAIL_CLOSED",
            as_of_date, snapshot_as_of,
        )
        return []

    if require_exact_as_of and str(snapshot.get("provider") or "") != "krx_marketcap_asof":
        logger.error(
            "[UNIVERSE][FALLBACK][DB][UNVERIFIED_PROVIDER] provider=%s action=FAIL_CLOSED",
            snapshot.get("provider"),
        )
        return []

    if require_exact_as_of and str(snapshot.get("actual_as_of") or "") != as_of_date:
        logger.error(
            "[UNIVERSE][FALLBACK][DB][UNVERIFIED_DATE] requested=%s actual=%s action=FAIL_CLOSED",
            as_of_date, snapshot.get("actual_as_of"),
        )
        return []

    members = list(snapshot.get("members") or [])
    min_members = (
        max(30, int(os.getenv("UNIVERSE_PREP_VERIFIED_DB_MIN_MEMBERS", "100")))
        if require_exact_as_of else 1
    )
    codes: set[str] = set()
    for member in members:
        code = str(member.get("code") or "")
        market = str(member.get("market") or "").upper()
        if not TICKER_PATTERN.fullmatch(code) or market not in TARGETS or code in codes:
            logger.error(
                "[UNIVERSE][FALLBACK][DB][INVALID_MEMBER] code=%s market=%s action=FAIL_CLOSED",
                code, market,
            )
            return []
        codes.add(code)
    if len(members) < min_members:
        logger.error(
            "[UNIVERSE][FALLBACK][DB][INSUFFICIENT] members=%s min=%s action=FAIL_CLOSED",
            len(members), min_members,
        )
        return []
    logger.warning(
        "[UNIVERSE][FALLBACK][DB][VERIFIED] run_id=%s as_of=%s members=%s provider=%s",
        snapshot.get("run_id"), snapshot_as_of, len(members), snapshot.get("provider"),
    )
    return members


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
                    rows.append({"code": code.zfill(6), "name": (row.get("name") or "").strip() or None, "market": str(row.get("market") or "").strip().upper() or None})
            else:
                f.seek(0)
                for line in f:
                    code = line.strip()
                    if code:
                        rows.append({"code": code.zfill(6), "name": None, "market": None})
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


def _load_krx_listing_market_map() -> dict[str, str]:
    """Use FDR's current KRX listing as an authoritative exchange reference.

    For emergency PREP only, never intraday. Reject ambiguous records instead
    of assuming a ticker's market from the order of a static seed list.
    """
    import FinanceDataReader as fdr
    from trader.universe.providers.fdr_marketcap_top import (
        CODE_COLUMNS, MARKET_COLUMNS, _find_column, _normalize_code,
    )

    listing = fdr.StockListing("KRX")
    if listing is None or len(listing) < 1500:
        raise RuntimeError("krx_listing_incomplete")
    code_col = _find_column(listing.columns, CODE_COLUMNS, "code")
    market_col = _find_column(listing.columns, MARKET_COLUMNS, "market")
    mapping: dict[str, str] = {}
    ambiguous: set[str] = set()
    for code_raw, market_raw in zip(listing[code_col], listing[market_col]):
        code = _normalize_code(code_raw)
        venue = str(market_raw).strip().upper()
        if not TICKER_PATTERN.fullmatch(code) or venue not in TARGETS:
            continue
        old = mapping.get(code)
        if old is not None and old != venue:
            ambiguous.add(code)
        else:
            mapping[code] = venue
    for code in ambiguous:
        mapping.pop(code, None)
    if len(mapping) < 1500:
        raise RuntimeError("krx_listing_venue_coverage_incomplete")
    logger.info(
        "[UNIVERSE][EMERGENCY_SEED][KRX_VERIFIED] listing_rows=%d "
        "known_venues=%d ambiguous=%d",
        len(listing), len(mapping), len(ambiguous),
    )
    return mapping


def _load_emergency_seed() -> dict | None:
    """Fail closed on unknown market; optionally verify legacy seed with KRX.

    The historical seed is code-only, unsorted by listing exchange, and cannot
    ever be partitioned safely by array position. An explicit market CSV is
    accepted as-is; otherwise allow *only* opt-in current KRX listing truth.
    """
    emergency_path = Path("data") / "universe_seed.csv"
    rows = _load_seed_rows(emergency_path)
    if not rows:
        return None

    unverified = [
        str(row.get("code") or "")
        for row in rows
        if str(row.get("market") or "").strip().upper() not in TARGETS
    ]
    verification_source = "explicit_seed_csv"
    if unverified:
        if os.getenv("UNIVERSE_EMERGENCY_VERIFY_KRX", "0") != "1":
            logger.error(
                "[UNIVERSE][EMERGENCY_SEED][UNVERIFIED_MARKET] path=%s invalid=%d "
                "sample=%s action=FAIL_CLOSED set_UNIVERSE_EMERGENCY_VERIFY_KRX_1_after_testing",
                emergency_path, len(unverified), unverified[:10],
            )
            return None
        try:
            venue_map = _load_krx_listing_market_map()
        except Exception as exc:
            logger.error(
                "[UNIVERSE][EMERGENCY_SEED][KRX_UNAVAILABLE] reason=%s "
                "action=FAIL_CLOSED",
                exc,
            )
            return None
        verified_rows: list[dict] = []
        unknown: list[str] = []
        for row in rows:
            code = str(row.get("code") or "").zfill(6)
            venue = venue_map.get(code)
            if venue is None:
                unknown.append(code)
                continue
            declared = str(row.get("market") or "").strip().upper()
            if declared in TARGETS and declared != venue:
                logger.error(
                    "[UNIVERSE][EMERGENCY_SEED][VENUE_CONFLICT] code=%s "
                    "declared=%s verified=%s action=FAIL_CLOSED",
                    code, declared, venue,
                )
                return None
            verified_rows.append({**row, "market": venue})
        if len(verified_rows) / len(rows) < 0.95:
            logger.error(
                "[UNIVERSE][EMERGENCY_SEED][LOW_KRX_COVERAGE] "
                "seed=%d verified=%d dropped=%d action=FAIL_CLOSED",
                len(rows), len(verified_rows), len(unknown),
            )
            return None
        if unknown:
            logger.warning(
                "[UNIVERSE][EMERGENCY_SEED][DROPPED_UNKNOWN] "
                "count=%d examples=%s",
                len(unknown), unknown[:10],
            )
        rows = verified_rows
        verification_source = "krx_listing_verified"

    members: list[dict] = []
    selected_by_market: dict[str, list[dict]] = {"KOSPI": [], "KOSDAQ": []}
    seen: set[str] = set()
    for row in rows:
        code = str(row.get("code") or "").zfill(6)
        market = str(row["market"]).strip().upper()
        if not TICKER_PATTERN.fullmatch(code) or code in seen:
            continue
        seen.add(code)
        if len(selected_by_market[market]) >= TARGETS[market]:
            continue
        rank = len(selected_by_market[market]) + 1
        members.append({
            "code": code,
            "market": market,
            "weight": None,
            "rank": rank,
            "meta_json": {
                "name": row.get("name"), "source": "emergency_seed",
                "market_verification": verification_source,
            },
        })
        selected_by_market[market].append({
            "code": code, "rank": rank, "name": row.get("name"),
        })

    if not members:
        return None

    payload = _normalize_payload({
        "selected": [m["code"] for m in members],
        "selected_by_market": selected_by_market,
    })
    return {
        "payload": payload,
        "members": members,
        "source": "fallback:emergency_seed",
        "params": {
            "path": str(emergency_path),
            "target_total": sum(TARGETS.values()),
            "market_source": verification_source,
        },
    }



def _build_from_krx_asof(
    provider: KRXAsOfMarketcapProvider, as_of_date: str,
) -> tuple[dict | None, str]:
    """Build exact-asof 150+150 from dated KRX data, never from current KIS top30."""
    try:
        rows_by_market = {
            market: provider.get_marketcap_top_with_meta(market, as_of_date, TARGETS[market])
            for market in ("KOSPI", "KOSDAQ")
        }
    except Exception as exc:
        logger.warning("[UNIVERSE][KRX_ASOF][FAIL] requested=%s reason=%s", as_of_date, exc)
        return None, f"krx_asof_failed:{exc}"

    combined = rows_by_market["KOSPI"] + rows_by_market["KOSDAQ"]
    if len(combined) != sum(TARGETS.values()):
        return None, f"krx_asof_insufficient total={len(combined)}"
    codes = [item["code"] for item in combined]
    if len(set(codes)) != len(codes):
        return None, "krx_asof_cross_market_duplicate"
    for market, rows in rows_by_market.items():
        if any(row.get("market") != market or row.get("actual_as_of") != as_of_date for row in rows):
            return None, f"krx_asof_metadata_mismatch market={market}"

    selected_by_market = {
        market: [
            {
                "code": item["code"],
                "rank": rank,
                "name": item.get("name"),
                "market_cap": item["market_cap"],
                "actual_as_of": item["actual_as_of"],
            }
            for rank, item in enumerate(rows, 1)
        ]
        for market, rows in rows_by_market.items()
    }
    members = _build_members_from_payload(selected_by_market)
    payload = _normalize_payload({
        "selected": codes, "selected_by_market": selected_by_market,
    })
    logger.info("[UNIVERSE][KRX_ASOF][COMPLETE] as_of=%s kospi=%d kosdaq=%d",
                as_of_date, len(rows_by_market["KOSPI"]), len(rows_by_market["KOSDAQ"]))
    return {
        "payload": payload, "members": members, "source": "krx_marketcap_asof",
        "params": {
            "actual_as_of": as_of_date,
            "requested_as_of": as_of_date,
            "provider_contract": "dated_krx_no_rollback",
            "targets": dict(TARGETS),
        },
    }, "krx_marketcap_asof"


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


def build_universe(as_of_date: str, env: str, strategy: str, provider_override: str | None = None, *, prep_recovery: bool = False) -> list[dict]:
    # Reject undated providers before opening DB connections or causing side effects.
    if prep_recovery and provider_override not in (None, "krx_marketcap_asof"):
        raise ValueError("PREP requires exact-date KRX provider; undated override rejected")
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    # Cache before start_universe_run: a retry with the same as-of may reuse a
    # previously successful run_id and temporarily change its status to RUNNING.
    prep_recovery_enabled = (
        prep_recovery
        and os.getenv("UNIVERSE_PREP_VERIFIED_DB_FALLBACK", "1") == "1"
        and os.getenv("UNIVERSE_NO_FALLBACK", "0") != "1"
    )
    preexisting_snapshot: dict | None = None
    if prep_recovery:
        try:
            preexisting_snapshot = repo.get_latest_successful_universe_snapshot(
                env=env, strategy=strategy, as_of_date=as_of_date,
            )
        except Exception as exc:
            logger.warning("[UNIVERSE][FALLBACK][DB][PREFETCH_FAIL] err=%s", exc)

    # Previous successful dated snapshots are immutable. In particular, force
    # rebuild or provider failure must never downgrade SUCCESS to RUNNING/FAILED.
    if prep_recovery and preexisting_snapshot:
        verified_existing = _load_fallback_universe_from_db(
            repo=repo, env=env, strategy=strategy, as_of_date=as_of_date,
            snapshot=preexisting_snapshot, require_exact_as_of=True,
            enforce_fallback_switch=False,
        )
        if verified_existing:
            logger.info("[UNIVERSE][PREP][REUSE_IMMUTABLE] as_of=%s members=%d run_id=%s",
                        as_of_date, len(verified_existing), preexisting_snapshot.get("run_id"))
            return verified_existing

    payload: dict | None = None
    members: list[dict] = []
    source = "krx_marketcap_asof" if prep_recovery else (provider_override or DEFAULT_PROVIDER)
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

    # PREP must not backdate KIS's undated 30-row current ranking or unversioned
    # static seeds. Preserve the existing KIS chain for non-PREP consumers.
    allowed_chain = ["krx_marketcap_asof"] if prep_recovery else providers_for_env(env)
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

        if provider_name == "krx_marketcap_asof":
            result, reason = _build_from_krx_asof(KRXAsOfMarketcapProvider(), as_of_date)
        elif provider_name == "kis_marketcap_top":
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

    if prep_recovery and members and len(members) < max(100, int(os.getenv("UNIVERSE_PREP_VERIFIED_DB_MIN_MEMBERS", "100"))):
        logger.error(
            "[UNIVERSE][KRX_ASOF][QUALITY_FAIL] after_sanitize=%d min=100 action=FAIL_CLOSED",
            len(members),
        )
        last_reason = "krx_asof_insufficient_after_sanitize"
        members = []

    if not members:
        logger.error("[UNIVERSE][EMPTY] as_of=%s env=%s strategy=%s source=%s", as_of_date, env, strategy, source)
        error_reason = f"provider_empty:{last_reason or source}"
        allow_fallback = is_db_only_mode() or resolve_strategy_mode() == "LIVE" or prep_recovery_enabled
        fallback_members: list[dict] = []
        if allow_fallback:
            fallback_members = _load_fallback_universe_from_db(
                repo=repo, env=env, strategy=strategy, as_of_date=as_of_date,
                snapshot=preexisting_snapshot if prep_recovery_enabled else None,
                require_exact_as_of=prep_recovery_enabled,
            )
        if fallback_members:
            if prep_recovery_enabled:
                # Restore SUCCESS when a reused run_id was temporarily RUNNING;
                # the verified member set is from the exact completed session.
                if not run_id:
                    raise RuntimeError("universe_recovery_run_id_missing")
                repo.store_universe_snapshot(
                    run_id=run_id, env=env, strategy=strategy,
                    as_of_date=as_of_date, provider="verified_db_snapshot",
                    members=fallback_members, reason="exact_asof_verified_db_recovery",
                    requested_as_of=as_of_date, actual_as_of=as_of_date,
                    build_reason="exact_asof_verified_db_recovery",
                    universe_name=strategy,
                )
            logger.warning("[UNIVERSE][RECOVERY][OK] as_of=%s members=%s", as_of_date, len(fallback_members))
            return fallback_members

        logger.error("[UNIVERSE][EMPTY] as_of=%s env=%s strategy=%s source=%s", as_of_date, env, strategy, source)
        try:
            if run_id and hasattr(repo, "record_universe_run_failure"):
                repo.record_universe_run_failure(
                    run_id=run_id, env=env, strategy=strategy,
                    as_of_date=as_of_date, provider=source,
                    error_reason=error_reason, members_count=0,
                )
            else:
                logger.warning("[UNIVERSE][RUN][FAIL_LOG][SKIP] run_id=%s not available", run_id)
        except Exception:
            logger.exception("[UNIVERSE][RUN][FAIL_LOG] as_of=%s", as_of_date)
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
            requested_as_of=as_of_date,
            actual_as_of=str(params.get("actual_as_of") or as_of_date),
            build_reason=last_reason,
            universe_name=strategy,
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
    build_universe(as_of_date=as_of, env=args.env, strategy=args.strategy,
                   provider_override=args.provider, prep_recovery=args.prep_recovery)


if __name__ == "__main__":
    main()
