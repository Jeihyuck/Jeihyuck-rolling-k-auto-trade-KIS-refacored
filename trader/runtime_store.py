from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from trader.botstate_paths import botstate_path, get_botstate_root
from trader.config import UNIVERSE_POOL_SIZE
from trader.universe.lkg_store import lkg_path

logger = logging.getLogger(__name__)

DEFAULT_UNIVERSE_STRATEGY = "best_k_meta"
MIN_UNIVERSE_MEMBERS = int(os.getenv("MIN_UNIVERSE_MEMBERS", str(UNIVERSE_POOL_SIZE)))

def _resolve_universe_env() -> str:
    return (os.getenv("KIS_ENV") or "practice").lower()


def _resolve_universe_strategy() -> str:
    return os.getenv("PB1_UNIVERSE_STRATEGY") or DEFAULT_UNIVERSE_STRATEGY


def universe_today_path(as_of: str) -> Path:
    return botstate_path("runtime", "universe", f"{as_of}.json")


def universe_lkg_path() -> Path:
    env = _resolve_universe_env()
    strategy = _resolve_universe_strategy()
    return lkg_path(env, strategy)


def balance_snapshot_path(base_dir: Path | None = None) -> Path:
    root = base_dir or get_botstate_root()
    return Path(root) / "runtime" / "balance_snapshot.json"


class RuntimeStore:
    def __init__(self, base_dir: Path | None = None, *, bot_state_dir: str | Path | None = None) -> None:
        if base_dir is None:
            base_dir = bot_state_dir if bot_state_dir is not None else get_botstate_root()
        self.base_dir = Path(base_dir)

    def _normalize_rel(self, path: Path) -> Path:
        raw = Path(path)
        if raw.is_absolute():
            try:
                return raw.relative_to(self.base_dir)
            except ValueError:
                pass
        parts = raw.parts
        if "bot_state" in parts:
            idx = parts.index("bot_state")
            return Path(*parts[idx + 1 :])
        if parts and parts[0] == "bot_state":
            return Path(*parts[1:])
        return raw

    def _resolve_universe_path(self, raw_path: Path) -> tuple[Path, Path]:
        rel = self._normalize_rel(raw_path)
        abs_path = (self.base_dir / rel).resolve()
        exists = abs_path.exists()
        logger.info(
            "[UNIVERSE][LOAD] base_dir=%s rel=%s abs=%s exists=%s",
            self.base_dir,
            rel,
            abs_path,
            int(exists),
        )
        if not exists:
            self._log_dir_snapshot()
        return abs_path, rel

    def save_json(self, rel: str | Path, data: Any) -> Path:
        target = self.base_dir / self._normalize_rel(Path(rel))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return target

    def save_balance_snapshot(
        self,
        *,
        raw_snapshot: dict,
        normalized_snapshot: dict,
        source: str,
        timestamp_kst: str,
    ) -> Path:
        payload = {
            "timestamp_kst": timestamp_kst,
            "source": source,
            "raw": raw_snapshot,
            "normalized": normalized_snapshot,
        }
        path = balance_snapshot_path(self.base_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def load_balance_snapshot(self) -> dict | None:
        path = balance_snapshot_path(self.base_dir)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("[BALANCE][SNAPSHOT][LOAD_FAIL] path=%s", path)
            return None

    def touch_flag(self, rel: str | Path, *, content: str = "done\n") -> Path:
        target = self.base_dir / self._normalize_rel(Path(rel))
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_text(content, encoding="utf-8")
        return target

    def write_status_last_db_write(self, *, run_id: str | None, reason: str, ts: str) -> Path:
        payload = {
            "run_id": run_id,
            "reason": reason,
            "ts": ts,
        }
        target = self.base_dir / "runtime" / "status" / "last_db_write.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return target

    def _log_dir_snapshot(self) -> None:
        base = self.base_dir
        listing: dict[str, list[str]] = {}
        if not base.exists():
            logger.info("[UNIVERSE][LOAD][LIST] base_dir=%s missing=1", base)
            return
        for child in sorted(base.iterdir()):
            if child.is_dir():
                try:
                    listing[str(child)] = [p.name for p in sorted(child.iterdir())]
                except Exception:
                    listing[str(child)] = ["<unreadable>"]
            else:
                listing[str(child)] = []
        logger.info("[UNIVERSE][LOAD][LIST] base_dir=%s listing=%s", base, listing)

    def _today_path(self, as_of: str) -> Path:
        return self.base_dir / "runtime" / "universe" / f"{as_of}.json"

    def _lkg_path(self) -> Path:
        env = _resolve_universe_env()
        strategy = _resolve_universe_strategy()
        return lkg_path(env, strategy)

    def universe_check(self, as_of: str) -> tuple[bool, dict]:
        today_raw = self._today_path(as_of)
        lkg_raw = self._lkg_path()
        today, today_rel = self._resolve_universe_path(today_raw)
        lkg, lkg_rel = self._resolve_universe_path(lkg_raw)
        meta = {
            "as_of": as_of,
            "today_path": str(today),
            "lkg_path": str(lkg),
            "today_rel": str(today_rel),
            "lkg_rel": str(lkg_rel),
            "have_today": today.exists(),
            "have_lkg": lkg.exists(),
        }
        ok = meta["have_today"] or meta["have_lkg"]
        meta["ok"] = ok
        meta["reason"] = "today" if meta["have_today"] else ("lkg" if meta["have_lkg"] else "missing")
        selected_path = ""
        if ok:
            selected_path = str(today if meta["have_today"] else lkg)
        meta["selected_path"] = selected_path
        members_count = 0
        if selected_path:
            try:
                with Path(selected_path).open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                members = data.get("members") if isinstance(data, dict) else data
                if isinstance(members, list):
                    members_count = len(members)
            except Exception:
                logger.exception("[UNIVERSE][CHECK][LOAD_FAIL] path=%s", selected_path)
        meta["members"] = members_count
        logger.info(
            "[UNIVERSE][CHECK] as_of=%s have_today=%s have_lkg=%s ok=%s reason=%s selected_path=%s members=%s",
            meta["as_of"],
            int(meta["have_today"]),
            int(meta["have_lkg"]),
            int(meta["ok"]),
            meta["reason"],
            meta["selected_path"],
            meta["members"],
        )
        if not meta["have_today"] and meta["have_lkg"]:
            logger.info(
                "[UNIVERSE][CHECK] today_missing -> using_lkg path=%s members=%s",
                meta["selected_path"],
                meta["members"],
            )
        return ok, meta

    def load_universe_for_trading(self, as_of: str) -> tuple[list[dict], dict]:
        ok, meta = self.universe_check(as_of)
        if not ok:
            return [], meta
        today = Path(meta["today_path"])
        p = today if today.exists() else Path(meta["lkg_path"])
        meta["selected_path"] = str(p)
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        members = data.get("members") if isinstance(data, dict) else data
        if not isinstance(members, list):
            members = []
        meta["members"] = len(members)
        return members, meta

    def ensure_universe(
        self,
        *,
        as_of: str,
        env: str,
        strategy: str,
        provider_override: str | None = None,
    ) -> dict:
        ok, meta = self.universe_check(as_of)
        if not ok:
            return meta
        today_members = int(meta.get("members") or 0)
        if today_members > 0:
            return meta
        if not meta.get("have_today"):
            return meta

        flag_rel = Path("runtime") / "status" / f"universe_rebuild_{as_of}.flag"
        flag = self.base_dir / flag_rel
        if flag.exists() and today_members > 0:
            logger.info("[UNIVERSE][REBUILD_SKIP] as_of=%s flag=%s", as_of, flag)
            return meta

        logger.info("[UNIVERSE][EMPTY_FATAL] today_universe_members=0 -> rebuild_once")
        try:
            from trader.universe import build as universe_build
        except Exception:
            logger.exception("[UNIVERSE][REBUILD_FAIL] import_error=1")
            return meta

        try:
            universe_build.build_universe(
                as_of_date=as_of,
                env=env,
                strategy=strategy,
                provider_override=provider_override,
            )
        except Exception:
            logger.exception("[UNIVERSE][REBUILD_FAIL] build_error=1")

        members, post_meta = self.load_today_universe(as_of)
        today_path = Path(post_meta.get("today_path") or "")
        source = None
        try:
            if today_path.exists():
                data = json.loads(today_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    source = data.get("source")
        except Exception:
            logger.exception("[UNIVERSE][REBUILD][LOAD_FAIL] as_of=%s", as_of)
        if members:
            logger.info(
                "[UNIVERSE][REBUILD_DONE] members=%s provider=%s",
                len(members),
                source or "unknown",
            )
            if len(members) >= MIN_UNIVERSE_MEMBERS:
                self.touch_flag(flag_rel, content=f"attempted as_of={as_of}\n")
        else:
            logger.warning("[UNIVERSE][REBUILD_FAILED] members=0 -> skip trading")
            if today_path.exists():
                try:
                    today_path.unlink()
                    logger.info("[UNIVERSE][EMPTY_FATAL] removed_empty_today path=%s", today_path)
                except Exception:
                    logger.exception("[UNIVERSE][EMPTY_FATAL] failed_remove_today path=%s", today_path)
            post_ok, post_meta = self.universe_check(as_of)
            if post_ok:
                return post_meta
        post_meta["members"] = len(members)
        return post_meta

    def load_today_universe(self, as_of: str) -> tuple[list[dict], dict]:
        today_raw = self._today_path(as_of)
        today, today_rel = self._resolve_universe_path(today_raw)
        meta = {
            "as_of": as_of,
            "today_path": str(today),
            "today_rel": str(today_rel),
            "have_today": today.exists(),
        }
        members: list[dict] = []
        if meta["have_today"]:
            try:
                with today.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
                loaded = data.get("members") if isinstance(data, dict) else data
                if isinstance(loaded, list):
                    members = loaded
            except Exception:
                logger.exception("[UNIVERSE][LOAD_TODAY][FAIL] path=%s", today)
        meta["members"] = len(members)
        return members, meta


def universe_check(as_of: str) -> tuple[bool, dict]:
    return RuntimeStore(get_botstate_root()).universe_check(as_of)


def load_universe_for_trading(as_of: str) -> tuple[list[dict], dict]:
    """
    Prefer today; else LKG.
    Returns: (members, meta)
    """
    return RuntimeStore(get_botstate_root()).load_universe_for_trading(as_of)


def check_universe_ready(today: date) -> dict[str, bool]:
    ok, meta = universe_check(today.isoformat())
    return {
        "db_ok": meta.get("have_today", False),
        "lkg_ok": meta.get("have_lkg", False),
        "ok": ok,
    }


def universe_today_exists(today: date) -> bool:
    return check_universe_ready(today)["ok"]
