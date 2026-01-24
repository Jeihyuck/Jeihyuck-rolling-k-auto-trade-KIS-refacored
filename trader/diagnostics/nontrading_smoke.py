from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.db.repos import LedgerEventsRepo
from trader.kis_wrapper import KisAPI
from trader.runtime_store import RuntimeStore

logger = logging.getLogger(__name__)


def nontrading_smoke_flag_path(runtime_store: RuntimeStore) -> Path:
    return Path(runtime_store.base_dir) / "runtime" / "diagnostics" / "nontrading_smoke_once.flag"


def write_nontrading_smoke_flag(
    runtime_store: RuntimeStore,
    *,
    now: datetime,
    run_id: str,
    sha: str,
) -> Path:
    flag = nontrading_smoke_flag_path(runtime_store)
    flag.parent.mkdir(parents=True, exist_ok=True)
    flag.write_text(f"{now.isoformat()} run_id={run_id} sha={sha}\n", encoding="utf-8")
    return flag


def _select_probe_codes(members: list[dict], limit: int = 3) -> list[str]:
    codes: list[str] = []
    for member in members:
        if isinstance(member, dict):
            code = member.get("code") or member.get("symbol") or member.get("ticker")
        else:
            code = None
        if not code:
            continue
        normalized = str(code).strip().lstrip("A").zfill(6)
        if normalized and normalized not in codes:
            codes.append(normalized)
        if len(codes) >= limit:
            break
    return codes


def run_nontrading_smoke_once(
    *,
    runtime_store: RuntimeStore,
    engine,
    now: datetime,
    env: str,
    strategy: str,
    as_of: str,
    timeout_sec: int,
    db_store: bool,
    force_rebuild: bool,
) -> dict:
    start_ts = time.monotonic()
    result: dict = {"ok": False, "steps": {}}
    logger.info(
        "[NONTRADING_SMOKE][START] as_of=%s timeout_sec=%s db_store=%s force_rebuild=%s",
        as_of,
        timeout_sec,
        int(db_store),
        int(force_rebuild),
    )

    def _check_timeout(stage: str) -> None:
        elapsed = time.monotonic() - start_ts
        if elapsed > timeout_sec:
            raise TimeoutError(f"nontrading_smoke timeout stage={stage} elapsed={elapsed:.1f}s")

    try:
        _check_timeout("universe_load")
        members, meta = runtime_store.load_today_universe(as_of)
        if not members and force_rebuild:
            runtime_store.ensure_universe(as_of=as_of, env=env, strategy=strategy)
            members, meta = runtime_store.load_today_universe(as_of)
        if not members:
            members, meta = runtime_store.load_universe_for_trading(as_of)
        member_count = len(members)
        result["steps"]["universe"] = {
            "members": member_count,
            "today_path": meta.get("today_path"),
            "selected_path": meta.get("selected_path"),
        }
        logger.info("[NONTRADING_SMOKE][UNIVERSE] members=%s", member_count)
        if member_count <= 0:
            raise RuntimeError("nontrading_smoke empty universe")

        _check_timeout("probe_prepare")
        try:
            kis = KisAPI(env=env)
        except Exception as exc:
            logger.warning("[NONTRADING_SMOKE][PROBE] kis_init_fail env=%s err=%s", env, exc)
            kis = None
        providers = [KISOHLCVProvider(kis)] if kis else []
        providers.append(KRXOHLCVProvider())
        chain = ChainOHLCVProvider(providers, env=env)

        codes = _select_probe_codes(members, limit=3)
        if not codes:
            raise RuntimeError("nontrading_smoke missing probe codes")
        probe_rows: dict[str, int] = {}
        probe_ok = True
        for code in codes:
            _check_timeout("probe_fetch")
            probe = chain.get_ohlcv(code, days=5)
            df = probe.df
            rows = len(df) if df is not None else 0
            probe_rows[code] = rows
            if rows <= 0:
                probe_ok = False
        result["steps"]["probe"] = {"codes": codes, "rows": probe_rows, "ok": probe_ok}
        logger.info("[NONTRADING_SMOKE][PROBE] codes=%s ok=%s", codes, int(probe_ok))
        if not probe_ok:
            raise RuntimeError("nontrading_smoke probe empty")

        db_store_ok = False
        event_id = None
        if db_store:
            _check_timeout("db_store")
            try:
                repo = LedgerEventsRepo(engine)
                event_id = repo.append_event(
                    env=env,
                    run_id=os.getenv("GITHUB_RUN_ID", "local"),
                    strategy=strategy,
                    run_window=None,
                    event_type="NONTRADING_SMOKE",
                    ts=now,
                    ok=True,
                    reasons=["nontrading_smoke"],
                    stage="smoke",
                    payload_json={
                        "as_of": as_of,
                        "codes": codes,
                        "rows": probe_rows,
                    },
                )
                db_store_ok = True
            except Exception:
                logger.exception("[NONTRADING_SMOKE][DB][FAIL] env=%s strategy=%s", env, strategy)
        result["steps"]["db_store"] = {"enabled": db_store, "wrote_event": event_id, "ok": db_store_ok}
        logger.info("[NONTRADING_SMOKE][DB] wrote_event=%s", event_id or "none")
        if db_store and not db_store_ok:
            raise RuntimeError("nontrading_smoke db_store failed")

        result["ok"] = True
    except Exception as exc:
        logger.exception("[NONTRADING_SMOKE][FAIL] err=%s", exc)
        result["ok"] = False
        result["error"] = repr(exc)
    finally:
        elapsed = time.monotonic() - start_ts
        result["elapsed_sec"] = round(elapsed, 3)
        try:
            result_path = Path(runtime_store.base_dir) / "runtime" / "diagnostics" / "nontrading_smoke_result.json"
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        except Exception:
            logger.exception("[NONTRADING_SMOKE][RESULT][WRITE_FAIL]")
        logger.info("[NONTRADING_SMOKE][DONE] ok=%s elapsed=%.2fs", int(result.get("ok")), elapsed)
    return result
