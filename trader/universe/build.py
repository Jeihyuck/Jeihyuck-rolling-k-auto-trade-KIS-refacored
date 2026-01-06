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
from trader.universe.providers.kis_mcap import KISMcapProvider

logger = logging.getLogger(__name__)

FALLBACK_MAX_AGE_DAYS = int(os.getenv("UNIVERSE_FALLBACK_MAX_AGE_DAYS", "10"))
SEED_DIR = Path(__file__).resolve().parent / "seeds"
SEED_PATH_KOSPI = SEED_DIR / "kospi_mcap_100.csv"
SEED_PATH_KOSDAQ = SEED_DIR / "kosdaq_mcap_100.csv"
TARGETS = {"KOSPI": 100, "KOSDAQ": 100}
DEFAULT_PROVIDER = "kis_mcap_200"


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


def _build_from_kis(provider: KISMcapProvider) -> dict | None:
    try:
        kospi_rows = provider.get_top_with_meta("KOSPI", TARGETS["KOSPI"])
        kosdaq_rows = provider.get_top_with_meta("KOSDAQ", TARGETS["KOSDAQ"])
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("[UNIVERSE][KIS][FAIL] %s", exc)
        return None

    if len(kospi_rows) < TARGETS["KOSPI"] or len(kosdaq_rows) < TARGETS["KOSDAQ"]:
        logger.warning(
            "[UNIVERSE][KIS][INSUFFICIENT] kospi=%s kosdaq=%s target=%s",
            len(kospi_rows),
            len(kosdaq_rows),
            TARGETS,
        )
        return None

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
    return {"payload": payload, "members": members, "source": DEFAULT_PROVIDER, "params": {"targets": TARGETS}}


def build_universe(as_of_date: str, env: str, strategy: str) -> str | None:
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    payload: dict | None = None
    members: list[dict] = []
    source = DEFAULT_PROVIDER
    params: dict = {"as_of": as_of_date}

    kis_provider: KISMcapProvider | None = None
    try:
        kis_provider = KISMcapProvider(kis=KisAPI(), env=env)
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
