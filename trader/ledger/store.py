from __future__ import annotations

import json
import os
import uuid
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from trader.ledger.event_types import LedgerEvent
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)

KST = now_kst().tzinfo  # reuse timezone


def _today_str() -> str:
    return now_kst().date().isoformat()


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class LedgerStore:
    def __init__(self, base_dir: Path, env: str = "paper", run_id: str | None = None) -> None:
        self.base_dir = Path(base_dir)
        self.env = env
        self.run_id = run_id or os.getenv("TRADER_RUN_ID", str(uuid.uuid4()))
        self.today = _today_str()

    def _run_file(self, kind: str) -> Path:
        return self.base_dir / kind / self.today / f"run_{self.run_id}.jsonl"

    def open_run_files(self) -> Dict[str, Path]:
        files = {}
        for kind in [
            "orders_intent",
            "orders_ack",
            "fills",
            "exits_intent",
            "errors",
        ]:
            path = self._run_file(kind)
            _ensure_dir(path)
            if not path.exists():
                path.touch()
            files[kind] = path
        return files

    def append_event(self, kind: str, event: LedgerEvent) -> Path:
        path = self._run_file(kind)
        _ensure_dir(path)
        line = event.to_jsonl()
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())
        logger.info("[LEDGER][APPEND] kind=%s path=%s", kind, path)
        return path

    def _iter_jsonl(self, paths: Iterable[Path]) -> Iterable[Dict]:
        for path in paths:
            if not path.exists():
                continue
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue

    def _recent_files(self, lookback_days: int) -> List[Path]:
        cutoff = now_kst().date() - timedelta(days=lookback_days)
        paths: List[Path] = []
        for kind in ["fills", "orders_intent", "orders_ack", "exits_intent", "errors"]:
            base = self.base_dir / kind
            if not base.exists():
                continue
            for day_dir in base.iterdir():
                try:
                    day = datetime.strptime(day_dir.name, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if day >= cutoff:
                    paths.extend(day_dir.glob("run_*.jsonl"))
        return sorted(paths)

    def has_client_order_key(self, client_order_key: str) -> bool:
        if not client_order_key:
            return False
        for row in self._iter_jsonl(self._recent_files(lookback_days=7)):
            if row.get("client_order_key") == client_order_key:
                return True
        return False

    def rebuild_positions_average_cost(self, lookback_days: int = 120) -> Dict[Tuple[str, int, int], Dict]:
        positions: Dict[Tuple[str, int, int], Dict[str, float | int | None]] = {}
        fill_paths = [p for p in self._recent_files(lookback_days) if "fills" in str(p)]
        for row in self._iter_jsonl(fill_paths):
            code = str(row.get("code") or "").zfill(6)
            sid = int(row.get("sid") or 0)
            mode = int(row.get("mode") or 0)
            side = (row.get("side") or "").upper()
            qty = int(row.get("qty") or 0)
            price = float(row.get("price") or 0.0)
            key = (code, sid, mode)
            state = positions.setdefault(
                key,
                {
                    "total_qty": 0,
                    "total_cost": 0.0,
                    "avg_buy_price": None,
                    "realized_pnl": 0.0,
                    "realized_cost_basis": 0.0,
                    "first_buy_ts": None,
                    "market": row.get("market"),
                },
            )
            if side == "BUY":
                state["total_cost"] += qty * price
                state["total_qty"] += qty
                state["avg_buy_price"] = state["total_cost"] / state["total_qty"]
                if state["first_buy_ts"] is None:
                    state["first_buy_ts"] = row.get("ts")
            elif side == "SELL":
                current_qty = int(state.get("total_qty") or 0)

                # 🔥 SELL이 BUY보다 먼저 나오거나 avg_buy_price가 없는 경우 방어
                if current_qty <= 0:
                    logger.warning(
                        "[LEDGER][REBUILD][SELL_WITHOUT_BUY] code=%s sid=%s mode=%s qty=%s price=%s action=ignore_for_position",
                        code, sid, mode, qty, price
                    )
                    state["total_qty"] = 0
                    state["total_cost"] = 0.0
                    state["avg_buy_price"] = None
                    continue

                sell_qty = min(qty, current_qty)
                avg = float(state.get("avg_buy_price") or 0.0)
                cost_basis = sell_qty * avg

                state["realized_pnl"] += sell_qty * price - cost_basis
                state["realized_cost_basis"] += cost_basis
                state["total_qty"] = current_qty - sell_qty
                state["total_cost"] = max(0.0, float(state.get("total_cost") or 0.0) - cost_basis)

                if state["total_qty"] <= 0:
                    state["total_qty"] = 0
                    state["total_cost"] = 0.0
                    state["avg_buy_price"] = None
                else:
                    state["avg_buy_price"] = state["total_cost"] / state["total_qty"]
        
        # holding days
        now_date = now_kst().date()
        for state in positions.values():
            if state.get("first_buy_ts"):
                try:
                    first_dt = datetime.fromisoformat(state["first_buy_ts"])
                    state["holding_days"] = (now_date - first_dt.date()).days
                except Exception:
                    state["holding_days"] = None
            else:
                state["holding_days"] = None
        
        # 🔥 net qty 0 이하인 종목은 반환하지 않음
        positions = {
            key: state
            for key, state in positions.items()
            if int(state.get("total_qty") or 0) > 0
        }
        
        return positions

    def compute_returns_pct(self, positions: Dict[Tuple[str, int, int], Dict], marks: Dict[str, float]) -> Dict[Tuple[str, int, int], Dict[str, float | int | None]]:
        result: Dict[Tuple[str, int, int], Dict[str, float | int | None]] = {}
        for key, state in positions.items():
            code, sid, mode = key
            mark_price = marks.get(code)
            avg = state.get("avg_buy_price")
            qty = state.get("total_qty") or 0
            if avg:
                unrealized = ((mark_price - avg) / avg) * 100 if mark_price else None
            else:
                unrealized = None
            realized_basis = state.get("realized_cost_basis") or 0.0
            realized_return = (
                (state.get("realized_pnl") or 0.0) / realized_basis * 100
                if realized_basis > 0
                else None
            )
            result[key] = {
                "qty": qty,
                "avg_buy_price": avg,
                "mark_price_used": mark_price,
                "unrealized_return_pct": unrealized,
                "realized_pnl": state.get("realized_pnl"),
                "realized_return_pct_to_date": realized_return,
                "holding_days": state.get("holding_days"),
                "market": state.get("market"),
            }
        return result

    def generate_pnl_snapshot(self, positions: Dict[Tuple[str, int, int], Dict], marks: Dict[str, float]) -> Dict:
        returns = self.compute_returns_pct(positions, marks)
        total_unrealized = 0.0
        total_cost = 0.0
        total_realized = 0.0
        snapshot_positions = {}
        for key, state in positions.items():
            data = returns.get(key) or {}
            code, sid, mode = key
            avg = data.get("avg_buy_price") or 0.0
            qty = data.get("qty") or 0
            mark = data.get("mark_price_used") or avg
            total_cost += avg * qty
            total_unrealized += (mark - avg) * qty
            total_realized += state.get("realized_pnl") or 0.0
            snapshot_positions[f"{code}|sid={sid}|mode={mode}"] = data
            snapshot_positions[f"{code}|sid={sid}|mode={mode}"]["last_actions"] = state.get("last_actions")
        total_return_pct = (total_unrealized / total_cost * 100) if total_cost else 0.0
        snapshot = {
            "ts": now_kst().isoformat(),
            "positions": snapshot_positions,
            "totals": {
                "total_cost": total_cost,
                "unrealized": total_unrealized,
                "realized": total_realized,
                "portfolio_return_pct": total_return_pct,
            },
        }
        return snapshot

    def write_snapshot(self, snapshot: Dict, run_id: str) -> Path:
        date_str = self.today
        path = self.base_dir / "reports" / date_str / "pnl_snapshot.json"
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        logger.info("[LEDGER][APPEND] kind=pnl_snapshot path=%s", path)
        return path
