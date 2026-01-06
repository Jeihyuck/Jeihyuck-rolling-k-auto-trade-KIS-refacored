from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Iterable

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.time_utils import now_kst
from trader.universe.providers.kis_marketcap_top import KISMarketcapTopProvider

logger = logging.getLogger(__name__)

FALLBACK_MAX_AGE_DAYS = int(os.getenv("UNIVERSE_FALLBACK_MAX_AGE_DAYS", "10"))
SEED_PATH_KOSPI = Path(__file__).resolve().parents[2] / "config" / "universe_seed_kospi100.csv"
SEED_PATH_KOSDAQ = Path(__file__).resolve().parents[2] / "config" / "universe_seed_kosdaq100.csv"
TARGETS = {"KOSPI": 100, "KOSDAQ": 100}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and persist universe members")
    parser.add_argument("--env", required=True, help="Environment (practice/real)")
    parser.add_argument("--strategy", required=True, help="Strategy name to store universe for")
    parser.add_argument("--date", help="As-of date (YYYY-MM-DD). Defaults to today.")
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


def _load_seed_codes(path: Path) -> list[str]:
    if not path.exists():
        return []
    codes: list[str] = []
    try:
        with path.open(encoding="utf-8") as f:
            for line in f:
                code = line.strip()
                if code:
                    codes.append(code.zfill(6))
    except Exception:
        logger.exception("[UNIVERSE][FALLBACK][STATIC][READ_FAIL] path=%s", path)
        return []
    return _dedup(codes)


def _load_static_seed() -> dict | None:
    kospi_codes = _load_seed_codes(SEED_PATH_KOSPI)
    kosdaq_codes = _load_seed_codes(SEED_PATH_KOSDAQ)

    members: list[dict] = []
    markets: dict[str, list[dict]] = {}

    seen: set[str] = set()

    for code in kospi_codes:
        if code in seen:
            continue
        seen.add(code)
        rank = len(markets.get("KOSPI", [])) + 1
        member = {"code": code, "market": "KOSPI", "weight": None, "rank": rank, "meta_json": {}}
        members.append(member)
        markets.setdefault("KOSPI", []).append({"code": code, "rank": rank})

    for code in kosdaq_codes:
        if code in seen:
            continue
        seen.add(code)
        rank = len(markets.get("KOSDAQ", [])) + 1
        member = {"code": code, "market": "KOSDAQ", "weight": None, "rank": rank, "meta_json": {}}
        members.append(member)
        markets.setdefault("KOSDAQ", []).append({"code": code, "rank": rank})

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


def _fallback_from_db(repo: UniverseRepo, env: str, strategy: str) -> dict | None:
    latest = repo.get_latest_universe(env=env, strategy=strategy, max_age_days=FALLBACK_MAX_AGE_DAYS)
    if not latest:
        return None
    universe_row, member_rows = latest
    payload = _normalize_payload(universe_row.get("payload_json"))
    return {
        "payload": payload,
        "members": member_rows,
        "source": "fallback:db",
        "params": {
            "fallback_from": universe_row.get("as_of_date"),
            "source_universe_id": str(universe_row.get("universe_id")),
        },
    }


def _select_fallback(repo: UniverseRepo, env: str, strategy: str) -> dict | None:
    db_fallback = _fallback_from_db(repo, env, strategy)
    if db_fallback:
        logger.info(
            "[UNIVERSE][FALLBACK][DB] env=%s strategy=%s reuse_universe_id=%s members=%s",
            env,
            strategy,
            db_fallback["params"].get("source_universe_id"),
            len(db_fallback["members"]),
        )
        return db_fallback
    static_fallback = _load_static_seed()
    if static_fallback:
        logger.warning(
            "[UNIVERSE][FALLBACK][STATIC] env=%s strategy=%s members=%s kospi_seed=%s kosdaq_seed=%s",
            env,
            strategy,
            len(static_fallback["members"]),
            SEED_PATH_KOSPI,
            SEED_PATH_KOSDAQ,
        )
        return static_fallback
    return None


def _build_from_kis(provider: KISMarketcapTopProvider) -> dict | None:
    try:
        kospi = provider.get_marketcap_top("KOSPI", TARGETS["KOSPI"])
        kosdaq = provider.get_marketcap_top("KOSDAQ", TARGETS["KOSDAQ"])
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[UNIVERSE][KIS][FAIL] %s", exc)
        return None

    if len(kospi) < TARGETS["KOSPI"] or len(kosdaq) < TARGETS["KOSDAQ"]:
        logger.warning(
            "[UNIVERSE][KIS][INSUFFICIENT] kospi=%s kosdaq=%s target=%s",
            len(kospi),
            len(kosdaq),
            TARGETS,
        )
        return None

    selected_by_market = {
        "KOSPI": [{"code": code, "rank": idx + 1} for idx, code in enumerate(kospi[: TARGETS["KOSPI"]])],
        "KOSDAQ": [{"code": code, "rank": idx + 1} for idx, code in enumerate(kosdaq[: TARGETS["KOSDAQ"]])],
    }
    payload = _normalize_payload(
        {
            "selected": kospi[: TARGETS["KOSPI"]] + kosdaq[: TARGETS["KOSDAQ"]],
            "selected_by_market": selected_by_market,
        }
    )
    members = _build_members_from_payload(selected_by_market)
    return {"payload": payload, "members": members, "source": "kis_marketcap_top", "params": {"targets": TARGETS}}


def build_universe(as_of_date: str, env: str, strategy: str) -> str | None:
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    payload: dict | None = None
    members: list[dict] = []
    source = "kis_marketcap_top"
    params: dict = {"as_of": as_of_date}

    kis_provider: KISMarketcapTopProvider | None = None
    try:
        kis_provider = KISMarketcapTopProvider(kis=KisAPI(), env=env)
    except Exception as exc:
        logger.warning("[UNIVERSE][KIS][INIT_FAIL] env=%s err=%s", env, exc)

    if kis_provider:
        kis_result = _build_from_kis(kis_provider)
        if kis_result:
            payload = kis_result["payload"]
            members = kis_result["members"]
            params.update(kis_result.get("params") or {})
            source = kis_result["source"]

    if len(members) == 0:
        fallback = _select_fallback(repo, env=env, strategy=strategy)
        if fallback:
            payload = fallback["payload"]
            members = fallback["members"]
            source = fallback["source"]
            params.update(fallback.get("params") or {})

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
            "[UNIVERSE][BUILT] env=%s strategy=%s as_of=%s universe_id=%s members=%s",
            env,
            strategy,
            as_of_date,
            universe_id,
            len(members),
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
    build_universe(as_of_date=as_of, env=args.env, strategy=args.strategy)


if __name__ == "__main__":
    main()
