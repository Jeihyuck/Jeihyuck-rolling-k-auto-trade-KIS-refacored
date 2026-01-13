from __future__ import annotations

import argparse
import csv
import logging
import os
from pathlib import Path
from typing import Iterable

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.time_utils import now_kst
from trader.universe.capabilities import providers_for_env
from trader.universe.providers.fdr_marketcap_top import fetch_marketcap_top
from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider
from trader.universe.providers.lkg_provider import LKGProvider
from trader.universe.providers.sqlite_cache_provider import SQLiteCacheProvider

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
    """Prefer caches first, then live providers, then static."""
    order = {
        "sqlite_cache": 10,
        "lkg": 20,
        # live providers
        "fdr_marketcap_top": 30,
        "kis_marketcap_top": 30,
        # ultimate fallback
        "seed_static": 90,
    }
    return sorted(list(chain), key=lambda p: (order.get(p, 50), p))

logger = logging.getLogger(__name__)

FALLBACK_MAX_AGE_DAYS = int(os.getenv("UNIVERSE_FALLBACK_MAX_AGE_DAYS", "10"))
SEED_DIR = Path(__file__).resolve().parent / "seeds"
SEED_PATH_KOSPI = SEED_DIR / "kospi_mcap_100.csv"
SEED_PATH_KOSDAQ = SEED_DIR / "kosdaq_mcap_100.csv"
TARGETS = {"KOSPI": 100, "KOSDAQ": 100}
DEFAULT_PROVIDER = "fdr_marketcap_top"
ENABLE_LKG = os.getenv("UNIVERSE_ENABLE_LKG", "1").lower() not in {"0", "false", "off"}
ENABLE_SQLITE_CACHE = os.getenv("UNIVERSE_ENABLE_SQLITE_CACHE", "1").lower() not in {"0", "false", "off"}


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


def _write_seed_rows(path: Path, rows: Iterable[dict]) -> None:
    try:
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


def _fallback_from_lkg(provider: LKGProvider, env: str, strategy: str, *, reference_as_of: str, max_age_days: int) -> dict | None:
    payload = provider.load(env, strategy)
    if not payload:
        return None

    cached_as_of = payload.get("params", {}).get("as_of") or payload.get("as_of")
    ref_d = _parse_as_of_date(reference_as_of)
    got_d = _parse_as_of_date(str(cached_as_of or ""))
    if ref_d and got_d:
        age = (ref_d - got_d).days
        if age > int(max_age_days):
            logger.info(
                "[UNIVERSE][LKG][STALE] env=%s strategy=%s cached_as_of=%s reference_as_of=%s age_days=%s max_age_days=%s",
                env,
                strategy,
                cached_as_of,
                reference_as_of,
                age,
                max_age_days,
            )
            return None

    payload_norm = _normalize_payload(payload.get("payload"))
    members = payload.get("members") or _build_members_from_payload(payload_norm.get("selected_by_market"))
    return {
        "payload": payload_norm,
        "members": members,
        "source": "fallback:lkg",
        "params": payload.get("params") or {"as_of": payload.get("as_of")},
    }


def _fallback_from_sqlite(
    provider: SQLiteCacheProvider,
    env: str,
    strategy: str,
    *,
    reference_as_of: str,
    max_age_days: int,
) -> dict | None:
    payload = provider.load_latest_universe_cache(env, strategy, max_age_days=max_age_days, reference_as_of=reference_as_of)
    if not payload:
        return None
    payload_norm = _normalize_payload(payload.get("payload"))
    members = payload.get("members") or _build_members_from_payload(payload_norm.get("selected_by_market"))
    return {
        "payload": payload_norm,
        "members": members,
        "source": "fallback:sqlite_cache",
        "params": payload.get("params") or {"as_of": payload.get("as_of")},
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
    try:
        rows_by_market = fetch_marketcap_top(targets)
    except Exception as exc:  # pragma: no cover - network/remote failure
        logger.warning("[UNIVERSE][FDR][FAIL] as_of=%s err=%s", as_of_date, exc)
        return None, "fdr_fetch_fail"

    if not rows_by_market.get("KOSPI") or not rows_by_market.get("KOSDAQ"):
        return None, "fdr_rows_missing"

    kospi_rows = rows_by_market["KOSPI"][: targets.get("KOSPI", 0)]
    kosdaq_rows = rows_by_market["KOSDAQ"][: targets.get("KOSDAQ", 0)]
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
    params = {"as_of": as_of_date, "targets": targets}
    members = _build_members_from_payload(selected_by_market)
    return {"payload": payload, "members": members, "source": "fdr_marketcap_top", "params": params}, "fdr_marketcap_top"


def build_universe(as_of_date: str, env: str, strategy: str, provider_override: str | None = None) -> str | None:
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

    lkg_provider = LKGProvider(enabled=ENABLE_LKG)
    sqlite_provider = SQLiteCacheProvider() if ENABLE_SQLITE_CACHE else None

    for provider_name in preferred_chain:
        result: dict | None = None
        reason: str | None = None

        if provider_name == "fdr_marketcap_top":
            result, reason = _build_from_fdr(as_of_date, TARGETS)
        elif provider_name == "kis_marketcap_top":
            if not kis_provider:
                reason = last_reason or "kis_provider_unavailable"
            else:
                result, reason = _build_from_kis(kis_provider, as_of_date)
        elif provider_name == "lkg":
            result = _fallback_from_lkg(lkg_provider, env=env, strategy=strategy, reference_as_of=as_of_date, max_age_days=FALLBACK_MAX_AGE_DAYS)
            reason = "lkg_hit" if result else "lkg_miss"
        elif provider_name == "sqlite_cache":
            if sqlite_provider:
                result = _fallback_from_sqlite(
                    sqlite_provider,
                    env=env,
                    strategy=strategy,
                    reference_as_of=as_of_date,
                    max_age_days=FALLBACK_MAX_AGE_DAYS,
                )
            reason = "sqlite_cache_hit" if result else "sqlite_cache_miss"
        elif provider_name == "seed_static":
            result = _load_static_seed()
            reason = "seed_static" if result else "seed_missing"
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
            if source in {"fdr_marketcap_top", "kis_marketcap_top"}:
                if ENABLE_LKG:
                    try:
                        lkg_provider.save(env, strategy, result)
                    except Exception:
                        logger.exception("[UNIVERSE][LKG][SAVE_FAIL] env=%s strategy=%s", env, strategy)
                if ENABLE_SQLITE_CACHE and sqlite_provider:
                    sqlite_provider.save_universe_cache(env, strategy, as_of_date, result)
            break

        last_reason = reason or provider_name
        logger.warning("[UNIVERSE][PROVIDER][FAIL] env=%s provider=%s reason=%s", env, provider_name, last_reason)

    universe_id = repo.store_universe(
        env=env,
        strategy=strategy,
        as_of_date=as_of_date,
        source=source,
        params_json=params,
        payload_json=payload or {},
        members=members,
    )
    if universe_id:
        logger.info(
            "[UNIVERSE][BUILT] env=%s strategy=%s as_of=%s universe_id=%s members=%s source=%s reason=%s",
            env,
            strategy,
            as_of_date,
            universe_id,
            len(members),
            source,
            last_reason or "n/a",
        )
    else:
        logger.warning(
            "[UNIVERSE][SKIPPED_DB] env=%s strategy=%s as_of=%s members=%s", env, strategy, as_of_date, len(members)
        )
    return universe_id


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    as_of = args.date or now_kst().date().isoformat()
    build_universe(as_of_date=as_of, env=args.env, strategy=args.strategy, provider_override=args.provider)


if __name__ == "__main__":
    main()
