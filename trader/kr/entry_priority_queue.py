"""Read-only collection priority for KR PB1 completed KIS minute-bar proofs.

The manifest is *not* an order authorization.  The score, entry selection,
risk gates and completed-minute ENFORCE remain in PB1.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Iterable
import json
import os
import subprocess
import tempfile

from trader.kr.opening_bar_contract import KST, OpeningProofLockTimeout, _acquire_opening_proof_lock
from trader.runtime_paths import repo_root

VERSION = "kr_pb1_minute_priority_v1"
HOT_TARGET = 5
WARM_TARGET = 5
TTL_SECONDS = 360


def _canonical(value: dict) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _code(value) -> str:
    raw = str(value or "").strip().removeprefix("A")
    return raw.zfill(6) if raw.isdigit() and 0 < len(raw) <= 6 else ""


def _unique(codes: Iterable[str]) -> list[str]:
    result, seen = [], set()
    for value in codes:
        c = _code(value)
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result


def final30_path(date: str) -> Path:
    return repo_root() / "runtime" / "kr" / "watchlist" / date / "final30_scored.json"


def _scope_digest(account_scope: str) -> str:
    return sha256(str(account_scope).encode("utf-8")).hexdigest()[:20]


def manifest_path(date: str, account_scope: str) -> Path:
    return (repo_root() / "runtime" / "kr" / "minute_priority" / date /
            ("pb1-" + _scope_digest(account_scope) + ".json"))


def _prep_snapshot(date: str, filename: str | None = None) -> dict | None:
    """Validate PREP source and its sibling contract before trusting priorities.

    This only validates *collection* scope.  The canonical Final30 and the PB1
    entry engine retain their independent (stronger) trading contracts.
    """
    path = Path(filename) if filename else final30_path(date)
    if not path.is_file():
        return None
    contract_paths = (path.with_name("prep_contract.json"),
                      path.parent / date / "prep_contract.json")
    present = [p for p in contract_paths if p.is_file()]
    if not present:
        return None
    try:
        payload_bytes = path.read_bytes()
        payload = json.loads(payload_bytes)
        contracts = [json.loads(p.read_text(encoding="utf-8")) for p in present]
        for contract in contracts:
            if (not isinstance(contract, dict)
                    or str(contract.get("trade_date") or "")[:10] != date
                    or str(contract.get("env") or "").lower() != "practice"
                    or str(contract.get("market") or "").upper() != "KR"
                    or not bool(contract.get("contract_ok"))
                    or not bool(contract.get("trade_can_proceed"))
                    or int(contract.get("final30_rows") or 0) != 30):
                return None
        if len(contracts) > 1:
            for key in ("trade_date", "market", "env", "contract_hash",
                        "expected_as_of", "actual_as_of"):
                values = {str(c[key]) for c in contracts if c.get(key) is not None}
                if len(values) > 1:
                    return None
        rows = (payload.get("rows") or payload.get("items")
                or payload.get("data") or payload.get("final30") or []
                ) if isinstance(payload, dict) else payload
        if not isinstance(rows, list) or len(rows) != 30:
            return None
        codes = _unique(row.get("code") if isinstance(row, dict) else None for row in rows)
        if len(codes) != 30:
            return None
        asof = str(contracts[0].get("actual_as_of")
                   or contracts[0].get("as_of")
                   or contracts[0].get("expected_as_of") or "")[:10]
        expected = str(contracts[0].get("expected_as_of") or "")[:10]
        if not asof or (expected and expected != asof):
            return None
        if isinstance(payload, dict):
            for key, value in (("trade_date", date), ("market", "KR"),
                               ("env", "practice"), ("as_of", asof)):
                if payload.get(key) is not None and str(payload[key])[:10] != value:
                    return None
        contract_hash = str(contracts[0].get("contract_hash") or "")
        return {"codes": codes, "contract_hash": contract_hash,
                "rows": rows, "final30_digest": sha256(payload_bytes).hexdigest()}
    except (ValueError, TypeError, OSError, KeyError):
        return None


def _current_sha() -> str:
    # The code pin is preferable if supplied by the launcher.  No secret data.
    for key in ("KR_CODE_PIN_SHA", "NULLIM_CODE_SHA", "GIT_SHA"):
        value = str(os.getenv(key) or "").strip()
        if len(value) == 40 and all(c in "0123456789abcdef" for c in value.lower()):
            return value.lower()
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_root(),
                                capture_output=True, text=True, timeout=2, check=True)
        return result.stdout.strip().lower()
    except (OSError, subprocess.SubprocessError):
        return ""


def _snapshot(*, date: str, account_scope: str, prep: dict,
              hot: list[str], warm: list[str], source: str, stage: str,
              now: datetime, code_sha: str) -> dict:
    cold = [c for c in prep["codes"] if c not in set(hot + warm)]
    body = {
        "version": VERSION, "trade_date": date, "env": "practice",
        "owner": "PB1", "stage": stage,
        "account_scope_hash": _scope_digest(account_scope),
        "final30_digest": prep["final30_digest"],
        "prep_contract_hash": prep["contract_hash"],
        "code_sha": code_sha, "source": source,
        "created_at_kst": now.isoformat(),
        "expires_at_kst": (now + timedelta(seconds=TTL_SECONDS)).isoformat(),
        "hot": hot, "warm": warm, "cold": cold,
        "is_order_authorization": False,
    }
    body["sha256"] = sha256(_canonical(body)).hexdigest()
    return body



def eligible_preminute_codes(
    *, candidates: Iterable[object], held_codes: Iterable[str] = (),
    blocking_buy_codes: Iterable[str] = (),
    filled_buy_codes: Iterable[str] = (),
    blocking_client_order_keys: Iterable[str] = (),
) -> list[str]:
    """Read-only HOT/WARM shortlist from *already* computed owner risk results.

    A setup without a positive sized quantity, a known held position, a live
    BUY intent, or an already-filled BUY does not warrant high-frequency
    minute polling. A prior local-only rejected intent need not be excluded:
    its separate final PB1 duplicate guard remains authoritative.

    This is never an order-authorization, cash, risk, or timing decision.
    """
    excluded = set(_unique(held_codes))
    excluded.update(_unique(blocking_buy_codes))
    excluded.update(_unique(filled_buy_codes))
    keys = {str(key).strip() for key in blocking_client_order_keys if str(key).strip()}
    result = []
    for cf in candidates:
        code = _code(getattr(cf, "code", None))
        if not code or code in excluded:
            continue
        if not bool(getattr(cf, "setup_ok", False)):
            continue
        try:
            qty = int(getattr(cf, "planned_qty", 0) or 0)
        except (ValueError, TypeError, OverflowError):
            qty = 0
        if qty <= 0:
            continue
        key = str(getattr(cf, "client_order_key", "") or "").strip()
        if key and key in keys:
            continue
        result.append(code)
    return _unique(result)


def publish_pb1_priority(*, date: str, account_scope: str,
                         eligible_codes: Iterable[str],
                         held_codes: Iterable[str] = (),
                         allow_add_to_existing: bool = False,
                         stage: str = "PB1-AM",
                         now: datetime | None = None) -> dict:
    """Publish the *pre-minute* PB1 ranking; never evaluate/authorize an order.

    Held codes are deprioritized only in the standard no-add PB1 BUY lane.
    Other strategy owners must not call this publisher.
    """
    now = (now or datetime.now(KST)).astimezone(KST)
    prep = _prep_snapshot(date)
    if prep is None or now.date().isoformat() != date:
        return {"state": "PREP_INVALID"}
    allowed = set(prep["codes"])
    held = set(_unique(held_codes)) if not allow_add_to_existing else set()
    ranked = [c for c in _unique(eligible_codes) if c in allowed and c not in held]
    # Invalid or unverified held state does NOT remove a Final30 code globally.
    remaining = [c for c in prep["codes"] if c not in ranked and c not in held]
    hot = ranked[:HOT_TARGET]
    # WARM is a genuine next-in-line BUY candidate, not a filler from Final30.
    warm = ranked[HOT_TARGET:HOT_TARGET + WARM_TARGET]
    body = _snapshot(date=date, account_scope=account_scope, prep=prep,
                     hot=hot, warm=warm, source="PB1_PRE_MINUTE",
                     stage=stage, now=now, code_sha=_current_sha())
    path = manifest_path(date, account_scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    import fcntl
    lock_path = path.with_suffix(".lock")
    with lock_path.open("a+b") as lock:
        try:
            _acquire_opening_proof_lock(lock, timeout_seconds=2.0)
        except OpeningProofLockTimeout:
            return {"state": "PRIORITY_LOCK_TIMEOUT", "hot": [], "warm": []}
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=path.parent,
                prefix="priority-", suffix=".tmp", delete=False,
            ) as out:
                tmp = out.name
                json.dump(body, out, ensure_ascii=False, sort_keys=True)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, path)
        finally:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return {"state": "PUBLISHED", "hot": hot, "warm": warm,
            "count": len(prep["codes"]), "path": str(path)}


def collection_priorities(*, date: str, account_scope: str,
                          now: datetime | None = None,
                          final30_file: str | None = None) -> dict:
    """Return a verified PB1 priority or safe PREP-seeded/fair rotation.

    Expired/tampered/wrong-account/date/SHA manifests never authorize trades.
    A PREP fallback only changes how an independent *read-only* collector works.
    """
    now = (now or datetime.now(KST)).astimezone(KST)
    prep = _prep_snapshot(date, filename=final30_file)
    if prep is None or now.date().isoformat() != date:
        return {"state": "INVALID_PREP", "hot": [], "warm": [], "cold": []}
    raw = None
    try:
        raw = json.loads(manifest_path(date, account_scope).read_text(encoding="utf-8"))
        digest = raw.pop("sha256", None)
        required = (
            raw.get("version") == VERSION and raw.get("trade_date") == date
            and raw.get("env") == "practice" and raw.get("owner") == "PB1"
            and raw.get("account_scope_hash") == _scope_digest(account_scope)
            and raw.get("final30_digest") == prep["final30_digest"]
            and raw.get("prep_contract_hash") == prep["contract_hash"]
            and raw.get("is_order_authorization") is False
            and digest == sha256(_canonical(raw)).hexdigest()
            and datetime.fromisoformat(raw["created_at_kst"]) <= now
            and datetime.fromisoformat(raw["expires_at_kst"]) > now
        )
        observed_sha = str(raw.get("code_sha") or "")
        current_sha = _current_sha()
        if observed_sha and current_sha and observed_sha != current_sha:
            required = False
        groups = _unique((raw.get("hot") or []) + (raw.get("warm") or []) + (raw.get("cold") or []))
        if (not required or len(groups) != 30
                or set(groups) != set(prep["codes"])):
            raise ValueError("STALE_OR_UNTRUSTED_MANIFEST")
        return {"state": "PB1_ACTIVE", "hot": list(raw["hot"]),
                "warm": list(raw["warm"]), "cold": list(raw["cold"]),
                "code_sha": observed_sha}
    except (FileNotFoundError, ValueError, TypeError, KeyError, OSError, AttributeError) as exc:
        # Not an authorization failure: preserve Final30, but the observer
        # must disclose why HOT/WARM is empty rather than idle silently.
        reason = ("MISSING_MANIFEST" if isinstance(exc, FileNotFoundError)
                  else "STALE_OR_UNTRUSTED_MANIFEST"
                  if str(exc) == "STALE_OR_UNTRUSTED_MANIFEST"
                  else "INVALID_MANIFEST_" + type(exc).__name__.upper())
        return {"state": "PREP_FALLBACK", "reason": reason,
                "hot": [], "warm": [], "cold": prep["codes"]}


# Observation-only PREP manifest: intentionally separate from execution shortlist.
PREP_COLLECTION_VERSION = "kr_pb1_prep_collection_v1"


def _collection_scope_ok(scope: str) -> bool:
    parts = str(scope or "").split(":")
    return (len(parts) == 3 and parts[0] == "practice"
            and all(p.isascii() and p.isdigit() for p in parts[1:]))


def prep_collection_path(date: str, account_scope: str) -> Path:
    return manifest_path(date, account_scope).with_suffix(".collection.json")


def _prep_collection_rank(rows: list[dict]) -> list[str]:
    """Observation only; compare existing scores within strategy, never authorize BUY."""
    import math
    families = {}
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        code = _code(row.get("code"))
        style = str(row.get("entry_style_selected") or "").upper().removeprefix("ENTRY_")
        family = next((f for f in ("PULLBACK", "MOMENTUM", "BREAKOUT", "MINERVINI", "VCP")
                       if style == f or style.startswith(f + "_")), "")
        if not code or code == "122630" or not family:
            continue
        try:
            rank = int(row.get("rank_final30") or i + 1)
            score = float(row.get(family.lower() + "_score"))
            if not 1 <= rank <= 30 or not math.isfinite(score) or score <= 0:
                continue
        except (ValueError, TypeError, OverflowError):
            continue
        families.setdefault(family, []).append((rank, -score, code))
    for values in families.values():
        values.sort(key=lambda item: (item[1], item[0], item[2]))
    lanes = sorted(families.values(), key=lambda values: min(x[0] for x in values))
    result = []
    while any(lanes):
        for lane in lanes:
            if lane:
                result.append(lane.pop(0)[2])
    return _unique(result)


def publish_prep_collection(*, date: str, account_scope: str,
                            now: datetime | None = None) -> dict:
    now = (now or datetime.now(KST)).astimezone(KST)
    prep = _prep_snapshot(date)
    sha = _current_sha()
    if (not _collection_scope_ok(account_scope) or prep is None
            or not prep["contract_hash"] or len(sha) != 40
            or now.date().isoformat() != date):
        return {"state": "INVALID_PREP_COLLECTION_SCOPE"}
    ranked = _prep_collection_rank(prep["rows"])
    body = _snapshot(date=date, account_scope=account_scope, prep=prep,
                     hot=ranked[:HOT_TARGET], warm=ranked[HOT_TARGET:HOT_TARGET+WARM_TARGET],
                     source="PREP_COLLECTION_ONLY", stage="PREP", now=now, code_sha=sha)
    body.pop("sha256")
    body["version"] = PREP_COLLECTION_VERSION
    body["expires_at_kst"] = now.replace(hour=15, minute=6, second=0, microsecond=0).isoformat()
    body["selection_reason"] = "FINAL30_WITHIN_STYLE_SCORE_ROUND_ROBIN"
    body["sha256"] = sha256(_canonical(body)).hexdigest()
    path = prep_collection_path(date, account_scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix="prep-collection-", delete=False) as out:
            tmp = out.name
            json.dump(body, out, ensure_ascii=False, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
    return {"state": "PREP_COLLECTION_PUBLISHED", "hot": body["hot"], "warm": body["warm"]}


def load_prep_collection(*, date: str, account_scope: str,
                         now: datetime, final30_file: str | None = None) -> dict | None:
    if not _collection_scope_ok(account_scope):
        return None
    prep = _prep_snapshot(date, filename=final30_file)
    if prep is None or not prep["contract_hash"] or now.astimezone(KST).date().isoformat() != date:
        return None
    try:
        raw = json.loads(prep_collection_path(date, account_scope).read_text(encoding="utf-8"))
        digest = raw.pop("sha256")
        hot, warm, cold = (raw[k] for k in ("hot", "warm", "cold"))
        groups = hot + warm + cold
        sha = _current_sha()
        if (raw.get("version") != PREP_COLLECTION_VERSION
                or raw.get("source") != "PREP_COLLECTION_ONLY"
                or raw.get("stage") != "PREP" or raw.get("owner") != "PB1"
                or raw.get("env") != "practice" or raw.get("trade_date") != date
                or raw.get("account_scope_hash") != _scope_digest(account_scope)
                or raw.get("is_order_authorization") is not False
                or raw.get("final30_digest") != prep["final30_digest"]
                or raw.get("prep_contract_hash") != prep["contract_hash"]
                or not sha or raw.get("code_sha") != sha
                or digest != sha256(_canonical(raw)).hexdigest()
                or not datetime.fromisoformat(raw["created_at_kst"]) <= now < datetime.fromisoformat(raw["expires_at_kst"])
                or len(hot) > HOT_TARGET or len(warm) > WARM_TARGET
                or "122630" in hot + warm or len(groups) != 30
                or len(set(groups)) != 30 or set(groups) != set(prep["codes"])):
            return None
        return {"state": "PREP_COLLECTION_ACTIVE", "hot": hot, "warm": warm, "cold": cold,
                "code_sha": sha, "final30_digest": prep["final30_digest"],
                "is_order_authorization": False}
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None


def observer_collection_priorities(*, date: str, account_scope: str, now: datetime,
                                   final30_file: str | None = None) -> dict:
    execution = collection_priorities(date=date, account_scope=account_scope, now=now,
                                      final30_file=final30_file)
    if (os.getenv("KR_PREP_COLLECTION_PRIORITY_ENABLED", "1") != "1"
            or execution["state"] == "INVALID_PREP"
            or (execution["state"] == "PB1_ACTIVE" and (execution["hot"] or execution["warm"]))):
        return execution
    seed = load_prep_collection(date=date, account_scope=account_scope, now=now,
                                final30_file=final30_file)
    return seed if seed is not None else execution


class CandidateSchedule:
    """Fair read-only poll planner; a poll attempt is NOT a fresh KIS proof.

    HOT is prioritized, but a busy HOT5 must not turn WARM5 into a
    deterministic 225-second polling gap with a 180-second proof TTL.
    This is a scheduling improvement, not a guarantee that REST calls
    succeed or that every Final30 symbol remains fresh.
    """

    def __init__(self, *, max_batch: int = 10,
                 hot_sec: int = 45, warm_sec: int = 180, cold_sec: int = 1800):
        self.last_attempt: dict[str, datetime] = {}
        self.last_tier: dict[str, str] = {}
        self.max_batch = max(3, int(max_batch))
        self.periods = {"hot": max(15, hot_sec),
                        "warm": max(60, warm_sec), "cold": max(120, cold_sec)}

    def diagnostics(self, snapshot: dict, now: datetime,
                    *, interval_sec: float = 45.0) -> dict:
        """Poll-debt/capacity telemetry only; never authorizes an entry.

        The 180-second bound applies to *verified broker data*, not to an
        attempted poll. Real HTTP duration, failures and retries may make
        actual throughput lower than this theoretical slot upper bound.
        """
        now = now.astimezone(KST)
        tiers = {}
        total = 0
        configured_demand = 0.0
        for tier in ("hot", "warm", "cold"):
            codes = _unique(snapshot.get(tier) or [])
            total += len(codes)
            configured_demand += len(codes) / float(self.periods[tier])
            ages = [(now - self.last_attempt[c]).total_seconds()
                    for c in codes if c in self.last_attempt]
            tiers[tier] = {
                "count": len(codes),
                "never_polled": len(codes) - len(ages),
                "overdue_poll": sum(age >= self.periods[tier] for age in ages),
                "max_poll_age_sec": max(ages) if ages else None,
            }
        capacity = self.max_batch / max(1.0, float(interval_sec))
        return {
            "tiers": tiers,
            "theoretical_polls_per_sec": capacity,
            "configured_demand_per_sec": configured_demand,
            "overcommitted": configured_demand > capacity,
            "all_180s_possible_at_slot_capacity": total / 180.0 <= capacity,
            "polls_not_proofs": True,
        }

    def select(self, snapshot: dict, now: datetime) -> list[str]:
        now = now.astimezone(KST)
        due: dict[str, list[tuple[float, str]]] = {}
        for category in ("hot", "warm", "cold"):
            items = []
            for code in _unique(snapshot.get(category) or []):
                last = self.last_attempt.get(code)
                promoted = category == "hot" and self.last_tier.get(code) != "hot"
                age = float("inf") if last is None or promoted else (now - last).total_seconds()
                if age >= self.periods[category]:
                    items.append((age, code))
            items.sort(key=lambda x: (-x[0], x[1]))
            due[category] = items

        # For a six-symbol cycle reserve 3 HOT / 2 WARM / 1 COLD,
        # rather than 4/1/1. This prevents a fixed WARM5 225s gap
        # without reducing the HOT5 maximum planned gap (<=90s in
        # an ideal uninterrupted 45s-cycle simulation). Reserve
        # proportionally for other batch sizes and reallocate idle slots.
        hot_quota = max(1, self.max_batch // 2)
        warm_quota = max(1, self.max_batch - hot_quota - 1)
        quotas = {"hot": hot_quota, "warm": warm_quota, "cold": 1}
        chosen: list[str] = []
        for tier in ("hot", "warm", "cold"):
            chosen.extend(c for _, c in due[tier][:quotas[tier]]
                          if c not in chosen)

        # Fill spare capacity with the most urgent due poll, with
        # the original HOT preference on equal deadline debt.
        remaining = []
        for tier_index, tier in enumerate(("hot", "warm", "cold")):
            for age, code in due[tier]:
                if code in chosen:
                    continue
                deadline = min(self.periods[tier], 180) if tier != "cold" else self.periods[tier]
                urgency = float("inf") if age == float("inf") else age - deadline
                remaining.append((-urgency, tier_index, code))
        remaining.sort()
        chosen.extend(code for _, _, code in remaining[:self.max_batch - len(chosen)])
        for code in chosen:
            self.last_attempt[code] = now
            self.last_tier[code] = next(t for t in ("hot", "warm", "cold") if code in snapshot.get(t, []))
        return chosen
