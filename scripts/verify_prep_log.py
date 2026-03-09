#!/usr/bin/env python3
"""Policy-aware PREP log verification parser.

Checks:
- PREP done markers and derived verify markers
- as_of consistency and candidate pool date guard
- contract recovery policy (degrade_allowed)
- exporter score preservation vs in-memory final30
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class VerifyResults:
    """Verification results container."""
    prep_done: bool = False
    prep_done_log: bool = False
    derived_verify_ok: bool = False
    derived_verify_fail: bool = False
    derived_count: int = 0
    entry_nonzero_present: bool = False
    final30_saved: bool = False
    final30_count: int = 0
    asof_consistent: bool = False
    candidate_pool_future_rejected: bool = False
    candidate_pool_future_seen: bool = False
    inmem_scores: Dict[str, int] = field(default_factory=dict)
    export_scores: Dict[str, int] = field(default_factory=dict)
    exporter_preserved_scores: bool = False
    derived_ok_by_count: bool = False
    contract_failures: List[str] = field(default_factory=list)
    contract_recoveries: List[str] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)
    
    def has_critical_failure(self) -> bool:
        """Check if any critical failure condition exists."""
        unrecovered_contracts = len(self.contract_failures) > 0 and len(self.contract_recoveries) == 0
        return bool(
            not self.prep_done
            or not self.prep_done_log
            or not self.derived_verify_ok
            or self.derived_verify_fail
            or not self.final30_saved
            or not self.asof_consistent
            or self.candidate_pool_future_seen
            or (not self.candidate_pool_future_rejected and self.candidate_pool_future_seen)
            or unrecovered_contracts
            or not self.exporter_preserved_scores
            or not self.derived_ok_by_count
            or bool(self.failures)
        )


def parse_log_file(log_path: Path) -> VerifyResults:
    """Parse PREP log file and extract verification data."""
    results = VerifyResults()
    
    if not log_path.exists():
        print(f"::error::Log file not found: {log_path}")
        return results
    
    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
        log_content = f.read()
    
    # PREP done conditions
    if re.search(r'event_type=PREP_DONE', log_content):
        results.prep_done = True
    if re.search(r'\[PREP\]\[DONE\]', log_content):
        results.prep_done_log = True
    if results.prep_done and results.prep_done_log:
        results.prep_done = True
    
    # Check derived_verify
    if re.search(r'\[PREP\]\[DERIVED_VERIFY\]\[OK\]', log_content):
        results.derived_verify_ok = True
    if re.search(r'\[PREP\]\[DERIVED_VERIFY\]\[FAIL\]', log_content):
        results.derived_verify_fail = True
    
    # Derived count patterns
    match = re.search(r'\[PREP\]\[DERIVED\]\[MINERVINI\].*upserted=(\d+)', log_content)
    if match:
        results.derived_count = int(match.group(1))
    
    # Pattern 2: [DERIVED][LOAD] ... rows=196
    if results.derived_count == 0:
        match = re.search(r'\[DERIVED\]\[LOAD\].*rows=(\d+)', log_content)
        if match:
            results.derived_count = int(match.group(1))
    
    results.derived_ok_by_count = results.derived_count > 0

    # Entry scores from in-memory PREP export checkpoint
    inmem_match = re.search(
        r'\[PREP\]\[EXPORT\]\[FINAL30\]\[INMEM\].*breakout_nonzero=(\d+).*pullback_nonzero=(\d+).*momentum_nonzero=(\d+)',
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
        r'\[EXPORT\]\[SCORES\] name=final30 .*tech_nonzero=(\d+).*score_final_nonzero=(\d+).*breakout_nonzero=(\d+).*pullback_nonzero=(\d+).*momentum_nonzero=(\d+)',
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
    
    # Check final30 saved
    if re.search(r'\[PREP\]\[WATCHLIST_FINAL\]\[SAVE\].*n=\d+', log_content):
        results.final30_saved = True
        match = re.search(r'\[PREP\]\[WATCHLIST_FINAL\]\[SAVE\].*n=(\d+)', log_content)
        if match:
            results.final30_count = int(match.group(1))
    elif re.search(r'\[PREP\]\[WATCHLIST\]\[FINAL\]\[SAVE\].*n=\d+', log_content):
        results.final30_saved = True
        match = re.search(r'\[PREP\]\[WATCHLIST\]\[FINAL\]\[SAVE\].*n=(\d+)', log_content)
        if match:
            results.final30_count = int(match.group(1))
    
    # as_of consistency
    asof_match = re.search(r'\[PREP\]\[ASOF_CONSISTENCY\].*consistent=(\d+)', log_content)
    if asof_match:
        results.asof_consistent = asof_match.group(1) == "1"

    # Candidate pool future snapshot guard
    if re.search(r'\[CANDIDATE_POOL\]\[DATE_GUARD\].*action=reject_future_snapshot', log_content):
        results.candidate_pool_future_rejected = True
    if re.search(r'\[CANDIDATE_POOL\]\[LOAD\].*age=-\d+', log_content):
        results.candidate_pool_future_seen = True
    if re.search(r'\[CANDIDATE_POOL\]\[LOAD\].*reason=future_snapshot', log_content):
        results.candidate_pool_future_seen = True

    # Contract failures
    for match in re.finditer(r'(contract_\w+_too_small)', log_content):
        failure = match.group(1)
        if failure not in results.contract_failures:
            results.contract_failures.append(failure)
    
    # Check for contract recoveries
    if re.search(r'\[PREP\]\[WATCHLIST\]\[RECOVERY\]\[DB_SUCCESS\]', log_content):
        results.contract_recoveries.append('DB_SUCCESS')
    if re.search(r'\[PREP\]\[WATCHLIST\]\[CONTRACT\]\[RECOVERED\]', log_content):
        results.contract_recoveries.append('CONTRACT_RECOVERED')
    
    # Exporter preservation: compare inmem and exporter for key score fields when both exist.
    if results.inmem_scores and results.export_scores:
        results.exporter_preserved_scores = (
            results.inmem_scores.get("breakout_nonzero", 0) == results.export_scores.get("breakout_nonzero", 0)
            and results.inmem_scores.get("pullback_nonzero", 0) == results.export_scores.get("pullback_nonzero", 0)
            and results.inmem_scores.get("momentum_nonzero", 0) == results.export_scores.get("momentum_nonzero", 0)
        )
    else:
        results.exporter_preserved_scores = False
    
    return results


def print_verification_results(results: VerifyResults) -> None:
    """Print verification results in GitHub Actions format."""
    print("=" * 50)
    print("[PREP][VERIFY] Verification Results")
    print("=" * 50)
    
    if results.prep_done:
        print("✅ PREP_DONE found")
    else:
        print("::error::PREP_DONE missing")

    if results.prep_done_log:
        print("✅ [PREP][DONE] found")
    else:
        print("::error::[PREP][DONE] missing")

    if results.asof_consistent:
        print("✅ as_of consistency passed")
    else:
        print("::error::as_of consistency failed")

    if not results.candidate_pool_future_seen:
        print("✅ candidate pool date guard passed")
    elif results.candidate_pool_future_rejected:
        print("✅ candidate pool future snapshot rejected")
    else:
        print("::error::candidate pool future snapshot detected")

    if results.derived_verify_ok and not results.derived_verify_fail and results.derived_ok_by_count:
        print("✅ derived verify passed")
    else:
        print("::error::derived verify failed")

    if results.entry_nonzero_present:
        print("✅ entry scores present")
    else:
        print("::error::entry scores missing")

    if results.contract_failures:
        if results.contract_recoveries:
            print("✅ contract violation recovered successfully")
        else:
            print("::error::Contract violation remained unrecovered")

    if results.final30_saved:
        print(f"✅ final30 save found (count={results.final30_count})")
    else:
        print("::error::final30 save missing")

    if results.exporter_preserved_scores:
        print("✅ exporter preserved score fields")
    else:
        print("::error::watchlist/exporter score mismatch")
    
    print("=" * 50)


def main() -> int:
    """Main verification function."""
    if len(sys.argv) < 2:
        print("Usage: verify_prep_log.py <log_file_path>")
        return 1
    
    log_path = Path(sys.argv[1])
    results = parse_log_file(log_path)
    print_verification_results(results)
    
    if results.has_critical_failure():
        print("::error::Critical failures detected - PREP verification failed")
        return 1

    print("✅ Verification complete")
    print("=" * 50)
    return 0


if __name__ == "__main__":
    sys.exit(main())
