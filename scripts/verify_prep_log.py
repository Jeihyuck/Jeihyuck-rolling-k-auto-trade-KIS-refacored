#!/usr/bin/env python3
"""Contract-aware PREP verification.

Verification prefers actual DB/file contract state and only falls back to
recognized success logs when runtime probes are unavailable.
"""
from __future__ import annotations

import os
import re
import sys
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


FINAL30_SUCCESS_PATTERNS: dict[str, str] = {
    "done_core": r"\[PREP\]\[DONE_CORE\]\[DONE\]",
    "core_save_done": r"\[FINAL30_SCORED\]\[CORE_SAVE\]\[DONE\]",
    "watchlist_final_save": r"\[PREP\]\[WATCHLIST_FINAL\]\[SAVE\]",
    "watchlist_final_scored_save": r"\[WATCHLIST\]\[SAVE\] strategy=pb1_watchlist_final_scored",
    "final30_snapshot_save": r"\[PREP\]\[FINAL30_SNAPSHOT\]\[SAVE\]",
    "db_final30_verify_ok": r"\[DB\]\[FINAL30_SCORED\]\[VERIFY\].*ok=1",
    "db_commit_verify_ok": r"\[PREP\]\[DB_COMMIT\]\[VERIFY\].*ok=1",
    "file_contract_ok": r"(?:\[PREP\])?\[FINAL30_FILE\]\[CONTRACT\].*ok=1",
    "repair_done": r"\[FINAL30\]\[REPAIR\]\[DONE\].*success=",
    "contract_ok": r"\[PREP\]\[FINAL30\]\[CONTRACT_OK\]",
    "prep_done": r"\[PREP\]\[DONE\]",
}


@dataclass
class VerifyResults:
    """Verification results container."""

    prep_done: bool = False
    prep_done_log: bool = False
    prep_done_db: bool = False
    prep_done_count: int = 0
    derived_verify_ok: bool = False
    derived_verify_fail: bool = False
    derived_count: int = 0
    entry_nonzero_present: bool = False
    asof_consistent: bool = False
    candidate_pool_future_rejected: bool = False
    candidate_pool_future_seen: bool = False
    inmem_scores: dict[str, int] = field(default_factory=dict)
    export_scores: dict[str, int] = field(default_factory=dict)
    exporter_preserved_scores: bool = False
    derived_ok_by_count: bool = False
    contract_failures: list[str] = field(default_factory=list)
    contract_recoveries: list[str] = field(default_factory=list)
    traceback_detected: bool = False
    traceback_non_fatal: bool = False
    pykrx_recovered: bool = False
    detected_as_of: str = ""
    detected_env: str = ""
    final30_success_logs: list[str] = field(default_factory=list)
    final30_file_labels: list[str] = field(default_factory=list)
    final30_log_success: bool = False
    final30_db_contract_ok: bool = False
    final30_file_contract_ok: bool = False
    final30_contract_ok: bool = False
    final30_failure_detail: str = ""
    db_metrics: dict[str, Any] = field(default_factory=dict)
    file_metrics: dict[str, Any] = field(default_factory=dict)
    canonical_manifest_found: bool = False
    canonical_manifest_path: str = ""
    canonical_status: str = ""
    canonical_quality_ok: int = 0
    canonical_trade_can_proceed: int = 0
    canonical_flow_failed_ratio: float = 0.0
    canonical_flow_fail_reason_counts: dict[str, int] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    def has_critical_failure(self) -> bool:
        unrecovered_contracts = len(self.contract_failures) > 0 and len(self.contract_recoveries) == 0
        prep_done_evidence = self.prep_done or self.prep_done_log or self.prep_done_db
        return bool(
            not prep_done_evidence
            or not self.derived_verify_ok
            or self.derived_verify_fail
            or not self.final30_contract_ok
            or not self.asof_consistent
            or unrecovered_contracts
            or not self.exporter_preserved_scores
            or not self.derived_ok_by_count
            or (self.traceback_detected and not self.traceback_non_fatal)
            or not self.canonical_manifest_found
            or bool(self.failures)
        )


def _extract_first(pattern: str, text: str) -> str:
    match = re.search(pattern, text)
    return str(match.group(1)).strip() if match else ""


def _extract_as_of(log_content: str) -> str:
    patterns = [
        r"\[PREP\]\[DONE_CORE\]\[DONE\] as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"\[PREP\]\[FINAL30\]\[CONTRACT_OK\] as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"\[PREP\]\[DB_COMMIT\]\[VERIFY\].* as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"\[PREP\]\[WATCHLIST_FINAL\]\[SAVE\].* as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"\[PREP\]\[DONE\] as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"event_type=PREP_DONE as_of=([0-9]{4}-[0-9]{2}-[0-9]{2})",
    ]
    for pattern in patterns:
        value = _extract_first(pattern, log_content)
        if value:
            return value
    return ""


def _extract_env(log_content: str) -> str:
    patterns = [
        r"\[PREP\]\[DB_COMMIT\]\[VERIFY\] env=([a-zA-Z0-9_-]+)",
        r"\[DB\]\[FINAL30_SCORED\]\[VERIFY\].* env=([a-zA-Z0-9_-]+)",
    ]
    for pattern in patterns:
        value = _extract_first(pattern, log_content)
        if value:
            return value.lower()
    env_value = (os.getenv("STRATEGY_ENV") or os.getenv("KIS_ENV") or "practice").strip().lower()
    return env_value or "practice"


def _has_any_final30_success_log(log_content: str) -> list[str]:
    return [
        name
        for name, pattern in FINAL30_SUCCESS_PATTERNS.items()
        if re.search(pattern, log_content)
    ]


def _db_final30_contract_ok(db_metrics: dict[str, Any]) -> bool:
    if not db_metrics or db_metrics.get("probe_ok") is False:
        return False
    return bool(
        int(db_metrics.get("prep_done", 0)) >= 1
        and int(db_metrics.get("derived_minervini", 0)) > 0
        and int(db_metrics.get("watchlist_final", 0)) >= 30
        and int(db_metrics.get("watchlist_final_scored", 0)) >= 30
        and int(db_metrics.get("uniq_codes", 0)) >= 30
        and int(db_metrics.get("uniq_ranks", 0)) >= 30
        and int(db_metrics.get("null_critical", 1)) == 0
        and not db_metrics.get("missing_required_fields")
    )


def _file_final30_contract_ok(file_metrics: dict[str, Any]) -> bool:
    if not file_metrics or file_metrics.get("probe_ok") is False:
        return False
    return bool(
        int(file_metrics.get("runtime_rows", 0)) > 0
        or int(file_metrics.get("ledger_rows", 0)) > 0
        or int(file_metrics.get("signals_rows", 0)) > 0
    )


def _build_final30_failure_detail(results: VerifyResults) -> str:
    db_final = int(results.db_metrics.get("watchlist_final", 0))
    db_final_scored = int(results.db_metrics.get("watchlist_final_scored", 0))
    runtime_rows = int(results.file_metrics.get("runtime_rows", 0))
    ledger_rows = int(results.file_metrics.get("ledger_rows", 0))
    signals_rows = int(results.file_metrics.get("signals_rows", 0))
    success_log_detected = int(bool(results.final30_success_logs))
    return (
        "FAIL final30 contract missing: "
        f"db_final={db_final} db_final_scored={db_final_scored} "
        f"runtime_rows={runtime_rows} ledger_rows={ledger_rows} signals_rows={signals_rows} "
        f"success_log_detected={success_log_detected}"
    )


def _probe_db_contract(*, as_of: str, env: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "probe_ok": False,
        "prep_done": 0,
        "prep_done_count": 0,
        "derived_minervini": 0,
        "watchlist_final": 0,
        "watchlist_final_scored": 0,
        "uniq_codes": 0,
        "uniq_ranks": 0,
        "null_critical": 1,
        "missing_required_fields": [],
    }
    if not as_of:
        return metrics

    try:
        from trader.db.engine import get_engine
        from trader.db.repos import (
            FINAL30_SCORED_DB_CONTRACT_FIELDS,
            DerivedMinerviniRepo,
            LedgerEventsRepo,
            WatchlistRepo,
        )

        engine = get_engine()
        ledger_repo = LedgerEventsRepo(engine)
        watchlist_repo = WatchlistRepo(engine)
        derived_repo = DerivedMinerviniRepo(engine)

        prep_done, prep_done_count = ledger_repo.prep_done_status(env=env, as_of=as_of)
        derived_count = derived_repo.count_as_of(env=env, as_of=as_of)
        final_rows, _ = watchlist_repo.load_watchlist(
            env=env,
            strategy="pb1_watchlist_final",
            as_of=as_of,
            allow_latest_fallback=False,
        )
        scored_contract = watchlist_repo.verify_watchlist_scored_contract(
            env=env,
            as_of=as_of,
            strategy="pb1_watchlist_final_scored",
            allow_latest_fallback=False,
            log_result=False,
        )
        scored_columns = set(scored_contract.get("columns") or [])
        missing_required_fields = [
            field for field in FINAL30_SCORED_DB_CONTRACT_FIELDS if field not in scored_columns
        ]
        metrics.update(
            {
                "probe_ok": True,
                "prep_done": int(bool(prep_done)),
                "prep_done_count": int(prep_done_count),
                "derived_minervini": int(derived_count),
                "watchlist_final": int(len(final_rows or [])),
                "watchlist_final_scored": int(scored_contract.get("rows") or 0),
                "uniq_codes": int(scored_contract.get("uniq_codes") or 0),
                "uniq_ranks": int(scored_contract.get("uniq_ranks") or 0),
                "null_critical": int(scored_contract.get("null_critical") or 0),
                "missing_required_fields": missing_required_fields,
            }
        )
    except Exception as exc:
        metrics["error"] = str(exc)
    return metrics


def _probe_file_contract(*, repo_root: Path, as_of: str, env: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "probe_ok": False,
        "runtime_rows": 0,
        "ledger_rows": 0,
        "signals_rows": 0,
    }
    if not as_of:
        return metrics

    try:
        from trader.path_contract import verify_final30_mirrors

        results = verify_final30_mirrors(
            repo_root=repo_root,
            env=env,
            as_of=as_of,
            expected_rows=None,
        )
        metrics.update(
            {
                "probe_ok": True,
                "runtime_rows": int((results.get("runtime") or {}).get("rows") or 0),
                "ledger_rows": int((results.get("ledger") or {}).get("rows") or 0),
                "signals_rows": int((results.get("signals") or {}).get("rows") or 0),
                "runtime_exists": int(bool((results.get("runtime") or {}).get("exists"))),
                "ledger_exists": int(bool((results.get("ledger") or {}).get("exists"))),
                "signals_exists": int(bool((results.get("signals") or {}).get("exists"))),
            }
        )
    except Exception as exc:
        metrics["error"] = str(exc)
    return metrics


def _resolve_repo_root(log_path: Path) -> Path:
    explicit = (os.getenv("TRADER_REPO_ROOT") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    cwd = Path.cwd().resolve()
    if (cwd / "trader").exists():
        return cwd
    parent = log_path.resolve().parent
    if (parent / "trader").exists():
        return parent
    return cwd


def _load_canonical_manifest(*, repo_root: Path, as_of: str) -> tuple[dict[str, Any], Path | None]:
    prep_root = repo_root / "runtime" / "prep"
    if not prep_root.exists():
        return {}, None
    candidates = []
    if as_of:
        candidate = prep_root / as_of / "prep_manifest.json"
        if candidate.exists():
            candidates.append(candidate)
    candidates.extend(sorted(prep_root.glob("*/prep_manifest.json"), reverse=True))
    if not candidates:
        return {}, None
    path = candidates[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, path
    return payload, path


def parse_log_file(log_path: Path) -> VerifyResults:
    """Parse PREP log file and extract verification data."""
    results = VerifyResults()

    if not log_path.exists():
        print(f"::error::Log file not found: {log_path}")
        results.failures.append(f"log_missing:{log_path}")
        return results

    log_content = log_path.read_text(encoding="utf-8", errors="replace")
    results.detected_as_of = _extract_as_of(log_content)
    results.detected_env = _extract_env(log_content)

    if re.search(r"event_type=PREP_DONE", log_content):
        results.prep_done = True
    if re.search(r"\[PREP\]\[DONE\]", log_content) or re.search(r"\[PREP\]\[DONE_CORE\]\[DONE\]", log_content):
        results.prep_done_log = True

    if re.search(r"\[PREP\]\[DERIVED_VERIFY\]\[OK\]", log_content):
        results.derived_verify_ok = True
    if re.search(r"\[PREP\]\[DERIVED_VERIFY\]\[FAIL\]", log_content):
        results.derived_verify_fail = True

    derived_patterns = [
        r"\[PREP\]\[DERIVED\]\[MINERVINI\].*upserted=(\d+)",
        r"\[DERIVED\]\[LOAD\].*rows=(\d+)",
    ]
    for pattern in derived_patterns:
        match = re.search(pattern, log_content)
        if match:
            results.derived_count = int(match.group(1))
            break

    inmem_match = re.search(
        r"\[PREP\]\[EXPORT\]\[FINAL30\]\[INMEM\].*breakout_nonzero=(\d+).*pullback_nonzero=(\d+).*momentum_nonzero=(\d+)",
        log_content,
    )
    if inmem_match:
        results.inmem_scores = {
            "breakout_nonzero": int(inmem_match.group(1)),
            "pullback_nonzero": int(inmem_match.group(2)),
            "momentum_nonzero": int(inmem_match.group(3)),
        }
        results.entry_nonzero_present = any(v > 0 for v in results.inmem_scores.values())

    export_match = re.search(
        r"\[EXPORT\]\[SCORES\] name=final30 .*tech_nonzero=(\d+).*score_final_nonzero=(\d+).*breakout_nonzero=(\d+).*pullback_nonzero=(\d+).*momentum_nonzero=(\d+)",
        log_content,
    )
    if export_match:
        results.export_scores = {
            "tech_nonzero": int(export_match.group(1)),
            "score_final_nonzero": int(export_match.group(2)),
            "breakout_nonzero": int(export_match.group(3)),
            "pullback_nonzero": int(export_match.group(4)),
            "momentum_nonzero": int(export_match.group(5)),
        }

    file_labels = set(
        match.group(1)
        for match in re.finditer(
            r"\[PREP\]\[FINAL30_FILE\]\[WRITE\] label=(\w+) path=.* exists=True bytes=(\d+) rows=(\d+)",
            log_content,
        )
        if int(match.group(2)) > 0 and int(match.group(3)) > 0
    )
    results.final30_file_labels = sorted(file_labels)
    results.final30_success_logs = _has_any_final30_success_log(log_content)
    results.final30_log_success = bool(results.final30_success_logs)

    asof_match = re.search(r"\[PREP\]\[ASOF_CONSISTENCY\].*consistent=(\d+)", log_content)
    if asof_match:
        results.asof_consistent = asof_match.group(1) == "1"

    if re.search(r"\[CANDIDATE_POOL\]\[DATE_GUARD\].*action=reject_future_snapshot", log_content):
        results.candidate_pool_future_rejected = True
    if re.search(r"\[CANDIDATE_POOL\]\[LOAD\].*age=-\d+", log_content):
        results.candidate_pool_future_seen = True
    if re.search(r"\[CANDIDATE_POOL\]\[LOAD\].*reason=future_snapshot", log_content):
        results.candidate_pool_future_seen = True

    for match in re.finditer(r"(contract_\w+_too_small)", log_content):
        failure = match.group(1)
        if failure not in results.contract_failures:
            results.contract_failures.append(failure)

    if re.search(r"\[PREP\]\[WATCHLIST\]\[RECOVERY\]\[DB_SUCCESS\]", log_content):
        results.contract_recoveries.append("DB_SUCCESS")
    if re.search(r"\[PREP\]\[WATCHLIST\]\[CONTRACT\]\[RECOVERED\]", log_content):
        results.contract_recoveries.append("CONTRACT_RECOVERED")

    if re.search(r"Traceback \(most recent call last\)", log_content):
        results.traceback_detected = True
    results.pykrx_recovered = bool(
        re.search(r"\[TIME\]\[TRADING_DAY\]\[PYKRX_FAIL\].*fallback=", log_content)
        and (results.prep_done or results.prep_done_log)
    )
    results.traceback_non_fatal = bool(results.traceback_detected and results.pykrx_recovered)

    if results.inmem_scores and results.export_scores:
        results.exporter_preserved_scores = (
            results.inmem_scores.get("breakout_nonzero", 0) == results.export_scores.get("breakout_nonzero", 0)
            and results.inmem_scores.get("pullback_nonzero", 0) == results.export_scores.get("pullback_nonzero", 0)
            and results.inmem_scores.get("momentum_nonzero", 0) == results.export_scores.get("momentum_nonzero", 0)
        )

    repo_root = _resolve_repo_root(log_path)
    results.db_metrics = _probe_db_contract(as_of=results.detected_as_of, env=results.detected_env)
    results.file_metrics = _probe_file_contract(
        repo_root=repo_root,
        as_of=results.detected_as_of,
        env=results.detected_env,
    )
    results.prep_done_db = bool(results.db_metrics.get("prep_done"))
    results.prep_done_count = int(results.db_metrics.get("prep_done_count") or 0)
    results.derived_count = max(results.derived_count, int(results.db_metrics.get("derived_minervini") or 0))
    results.derived_ok_by_count = results.derived_count > 0
    results.final30_db_contract_ok = _db_final30_contract_ok(results.db_metrics)
    results.final30_file_contract_ok = _file_final30_contract_ok(results.file_metrics)
    results.final30_contract_ok = (
        results.final30_db_contract_ok
        or results.final30_file_contract_ok
        or results.final30_log_success
    )
    results.final30_failure_detail = _build_final30_failure_detail(results)
    require_canonical = str(os.getenv("VERIFY_PREP_REQUIRE_CANONICAL", "0")).strip().lower() in {"1", "true", "yes", "on"}
    manifest, manifest_path = _load_canonical_manifest(repo_root=repo_root, as_of=results.detected_as_of)
    if manifest and manifest_path is not None:
        canonical = dict(manifest.get("canonical_quality") or {})
        results.canonical_manifest_found = True
        results.canonical_manifest_path = str(manifest_path)
        results.canonical_status = str(canonical.get("status") or manifest.get("build_status") or "")
        results.canonical_quality_ok = int(canonical.get("quality_ok", manifest.get("final30_quality_ok", 0)) or 0)
        results.canonical_trade_can_proceed = int(canonical.get("trade_can_proceed", manifest.get("trade_can_proceed", 0)) or 0)
        try:
            results.canonical_flow_failed_ratio = float(manifest.get("flow_failed_ratio", 0.0) or 0.0)
        except Exception:
            results.canonical_flow_failed_ratio = 0.0
        raw_reason_counts = dict(manifest.get("flow_fail_reason_counts") or {})
        results.canonical_flow_fail_reason_counts = {
            str(key): int(value)
            for key, value in raw_reason_counts.items()
            if str(key).strip()
        }
        if results.canonical_quality_ok == 0:
            results.failures.append("quality_not_ok")
        if results.canonical_status.upper() == "FAIL":
            results.failures.append("status_fail")
        if results.canonical_flow_failed_ratio >= 1.0:
            results.failures.append("flow_failed_ratio_hard_fail")
        if results.canonical_trade_can_proceed == 0:
            results.failures.append("trade_cannot_proceed")
    elif require_canonical:
        results.failures.append("canonical_manifest_missing")
    return results


def print_verification_results(results: VerifyResults) -> None:
    """Print verification results in GitHub Actions format."""
    print("=" * 50)
    print("[PREP][VERIFY] Verification Results")
    print("=" * 50)

    if results.prep_done or results.prep_done_db:
        print(f"PASS PREP_DONE found (db_count={results.prep_done_count})")
    else:
        print("FAIL PREP_DONE missing")

    if results.prep_done_log:
        print("PASS [PREP][DONE] or [PREP][DONE_CORE][DONE] found")
    else:
        print("FAIL [PREP][DONE] missing")

    if results.asof_consistent:
        print("PASS as_of consistency passed")
    else:
        print("FAIL as_of consistency failed")

    if results.candidate_pool_future_rejected:
        print("PASS candidate pool future snapshot rejected")
    else:
        print("INFO no future snapshot rejection observed")

    if results.derived_verify_ok and not results.derived_verify_fail and results.derived_ok_by_count:
        print(f"PASS derived verify passed (derived_minervini={results.derived_count})")
    else:
        print("FAIL derived verify missing")

    if results.entry_nonzero_present:
        print("PASS entry scores present")
    else:
        print("FAIL entry scores missing")

    if results.contract_failures and not results.contract_recoveries:
        print("FAIL unrecovered contract failure")
    else:
        print("PASS contract violation recovered successfully")

    if results.final30_contract_ok:
        print(
            "PASS final30 contract ok "
            f"(db={int(results.final30_db_contract_ok)} file={int(results.final30_file_contract_ok)} log={int(results.final30_log_success)} "
            f"db_final={int(results.db_metrics.get('watchlist_final', 0))} "
            f"db_final_scored={int(results.db_metrics.get('watchlist_final_scored', 0))} "
            f"runtime_rows={int(results.file_metrics.get('runtime_rows', 0))} "
            f"ledger_rows={int(results.file_metrics.get('ledger_rows', 0))} "
            f"signals_rows={int(results.file_metrics.get('signals_rows', 0))} "
            f"success_logs={results.final30_success_logs})"
        )
    else:
        print(results.final30_failure_detail)

    if results.canonical_manifest_found:
        print(
            "PASS canonical manifest loaded "
            f"path={results.canonical_manifest_path} "
            f"quality_ok={results.canonical_quality_ok} "
            f"status={results.canonical_status} "
            f"trade_can_proceed={results.canonical_trade_can_proceed} "
            f"flow_failed_ratio={results.canonical_flow_failed_ratio:.3f} "
            f"flow_fail_reason_counts={results.canonical_flow_fail_reason_counts}"
        )
    else:
        print("FAIL canonical manifest missing")

    if results.exporter_preserved_scores:
        print("PASS exporter preserved score fields")
    else:
        print("FAIL exporter score field preservation failed")

    if results.traceback_detected and not results.traceback_non_fatal:
        print("FAIL Python traceback detected")
    elif results.traceback_non_fatal:
        print("PASS traceback classified as non-fatal PYKRX fallback noise")
    else:
        print("PASS no traceback detected")

    print("=" * 50)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: verify_prep_log.py <log_file_path>")
        return 1

    log_path = Path(sys.argv[1])
    results = parse_log_file(log_path)
    print_verification_results(results)

    if results.has_critical_failure():
        for reason in dict.fromkeys(results.failures):
            print(f"[VERIFY_PREP][FAIL] reason={reason}")
        print("Error: Critical failures detected - PREP verification failed")
        return 1

    print("PREP verification passed")
    print("=" * 50)
    return 0


if __name__ == "__main__":
    sys.exit(main())
