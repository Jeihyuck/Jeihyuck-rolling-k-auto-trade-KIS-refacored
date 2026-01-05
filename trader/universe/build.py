from __future__ import annotations

import argparse
import logging
import os
import csv
from pathlib import Path

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.time_utils import now_kst
from trader.universe.krx_safe import patch_pykrx_logging

logger = logging.getLogger(__name__)
FALLBACK_MAX_AGE_DAYS = int(os.getenv("UNIVERSE_FALLBACK_MAX_AGE_DAYS", "10"))
STATIC_SEED_PATH = Path(__file__).resolve().parents[2] / "config" / "universe_seed_kosdaq50.csv"


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


def _load_static_seed() -> dict | None:
    if not STATIC_SEED_PATH.exists():
        return None
    try:
        with STATIC_SEED_PATH.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [row for row in reader]
    except Exception:
        logger.exception("[UNIVERSE][FALLBACK][STATIC][READ_FAIL] path=%s", STATIC_SEED_PATH)
        return None
    members: list[dict] = []
    markets: dict[str, list[dict]] = {}
    for idx, row in enumerate(rows, start=1):
        code = str(row.get("code") or row.get("Code") or "").zfill(6)
        if not code.strip():
            continue
        market = (row.get("market") or row.get("Market") or "KOSDAQ").strip()
        name = (row.get("name") or row.get("Name") or "").strip() or None
        meta_json = {}
        if name:
            meta_json["name"] = name
        member = {"code": code, "market": market, "weight": None, "rank": idx, "meta_json": meta_json}
        members.append(member)
        markets.setdefault(market, []).append({"code": code, "weight": None, "rank": idx, "name": name})
    if not members:
        return None
    payload = _normalize_payload(
        {
            "selected": [m["code"] for m in members],
            "selected_by_market": markets,
        }
    )
    return {"payload": payload, "members": members, "source": "fallback:static", "params": {"seed_path": str(STATIC_SEED_PATH)}}


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
        logger.info(
            "[UNIVERSE][FALLBACK][STATIC] env=%s strategy=%s members=%s path=%s",
            env,
            strategy,
            len(static_fallback["members"]),
            STATIC_SEED_PATH,
        )
        return static_fallback
    return None


def build_universe(as_of_date: str, env: str, strategy: str) -> str | None:
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    patch_pykrx_logging()

    raw_payload: dict | None = None
    try:
        raw_payload = run_rebalance(as_of_date, return_by_market=True)
    except Exception as exc:
        logger.warning("[UNIVERSE][KRX_FAIL] %s", repr(exc))
    payload = _normalize_payload(raw_payload)
    members = _build_members_from_payload(payload.get("selected_by_market"))

    fallback = None
    if len(members) == 0:
        fallback = _select_fallback(repo, env=env, strategy=strategy)
        if fallback:
            payload = fallback["payload"]
            members = fallback["members"]

    universe_id = repo.store_universe(
        env=env,
        strategy=strategy,
        as_of_date=as_of_date,
        source=fallback["source"] if fallback else "best_k_meta_strategy",
        params_json={"as_of": as_of_date, **(fallback.get("params") if fallback else {})},
        payload_json=payload,
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
