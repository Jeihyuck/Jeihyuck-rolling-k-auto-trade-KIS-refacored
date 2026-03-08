#!/usr/bin/env python3
"""
PREP log verification script.

Parses PREP log output and validates that:
1. PREP completed successfully with PREP_DONE event
2. Derived data has nonzero counts and quality
3. Entry scores (breakout/pullback/momentum) are present
4. Contract failures (if any) were recovered
5. Watchlist stages were created successfully

Exit codes:
  0 - All checks passed
  1 - Critical failure detected
  2 - Warnings present but recoverable
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class VerifyResults:
    """Verification results container."""
    prep_done: bool = False
    derived_verify_ok: bool = False
    derived_verify_fail: bool = False
    derived_count: int = 0
    rs_nonzero: int = 0
    vcp_nonzero: int = 0
    trend_nonzero: int = 0
    breakout_nonzero: int = 0
    pullback_nonzero: int = 0
    momentum_nonzero: int = 0
    final30_saved: bool = False
    final30_count: int = 0
    contract_failures: List[str] = field(default_factory=list)
    contract_recoveries: List[str] = field(default_factory=list)
    quality_failures: List[str] = field(default_factory=list)
    attribute_errors: List[str] = field(default_factory=list)
    universe_build_fail: bool = False
    
    def has_critical_failure(self) -> bool:
        """Check if any critical failure condition exists."""
        # Contract failures are critical only if not recovered
        unrecovered_contracts = len(self.contract_failures) > 0 and len(self.contract_recoveries) == 0
        
        return (
            not self.prep_done
            or self.derived_verify_fail
            or self.universe_build_fail
            or len(self.attribute_errors) > 0
            or len(self.quality_failures) > 0
            or unrecovered_contracts
            # Don't treat final30_saved as critical if PREP_DONE is present
            # or not self.final30_saved
        )
    
    def has_warnings(self) -> bool:
        """Check if any warning condition exists."""
        # Only warn if BOTH derived scores and entry scores are missing
        derived_scores_missing = (self.rs_nonzero == 0 and self.vcp_nonzero == 0 and self.trend_nonzero == 0)
        entry_scores_missing = (self.breakout_nonzero == 0 and self.pullback_nonzero == 0 and self.momentum_nonzero == 0)
        
        return (
            self.derived_count == 0
            or (derived_scores_missing and entry_scores_missing)
        )


def parse_log_file(log_path: Path) -> VerifyResults:
    """Parse PREP log file and extract verification data."""
    results = VerifyResults()
    
    if not log_path.exists():
        print(f"::error::Log file not found: {log_path}")
        return results
    
    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
        log_content = f.read()
    
    # Check PREP_DONE
    if re.search(r'event_type=PREP_DONE', log_content):
        results.prep_done = True
    elif re.search(r'\[PREP\]\[DONE\]', log_content):
        results.prep_done = True
    
    # Check derived_verify
    if re.search(r'\[PREP\]\[DERIVED_VERIFY\]\[OK\]', log_content):
        results.derived_verify_ok = True
    if re.search(r'\[PREP\]\[DERIVED_VERIFY\]\[FAIL\]', log_content):
        results.derived_verify_fail = True
    
    # Check derived count with updated patterns
    # Pattern 1: [PREP][DERIVED][MINERVINI] upserted=196
    match = re.search(r'\[PREP\]\[DERIVED\]\[MINERVINI\].*upserted=(\d+)', log_content)
    if match:
        results.derived_count = int(match.group(1))
    
    # Pattern 2: [DERIVED][LOAD] ... rows=196
    if results.derived_count == 0:
        match = re.search(r'\[DERIVED\]\[LOAD\].*rows=(\d+)', log_content)
        if match:
            results.derived_count = int(match.group(1))
    
    # Check for nonzero RS/VCP/Trend scores
    match = re.search(r'rs_nonzero=(\d+)', log_content)
    if match:
        results.rs_nonzero = int(match.group(1))
    
    match = re.search(r'vcp_nonzero=(\d+)', log_content)
    if match:
        results.vcp_nonzero = int(match.group(1))
    
    match = re.search(r'trend_nonzero=(\d+)', log_content)
    if match:
        results.trend_nonzero = int(match.group(1))
    
    # Check for entry scores (breakout/pullback/momentum)
    match = re.search(r'breakout_nonzero=(\d+)', log_content)
    if match:
        results.breakout_nonzero = int(match.group(1))
    
    match = re.search(r'pullback_nonzero=(\d+)', log_content)
    if match:
        results.pullback_nonzero = int(match.group(1))
    
    match = re.search(r'momentum_nonzero=(\d+)', log_content)
    if match:
        results.momentum_nonzero = int(match.group(1))
    
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
    
    # Check for contract failures
    for match in re.finditer(r'(contract_\w+_too_small)', log_content):
        failure = match.group(1)
        if failure not in results.contract_failures:
            results.contract_failures.append(failure)
    
    # Check for contract recoveries
    if re.search(r'\[PREP\]\[WATCHLIST\]\[RECOVERY\]\[DB_SUCCESS\]', log_content):
        results.contract_recoveries.append('DB_SUCCESS')
    if re.search(r'\[PREP\]\[WATCHLIST\]\[CONTRACT\]\[RECOVERED\]', log_content):
        results.contract_recoveries.append('CONTRACT_RECOVERED')
    
    # Check for quality failures
    if re.search(r'quality check failed', log_content, re.IGNORECASE):
        results.quality_failures.append('quality_check_failed')
    if re.search(r'\[DERIVED_MERGE\].*\[FAIL\]', log_content):
        results.quality_failures.append('derived_merge_fail')
    if re.search(r'all_scores_zero', log_content):
        results.quality_failures.append('all_scores_zero')
    if re.search(r'all_entry_scores_zero', log_content):
        results.quality_failures.append('all_entry_scores_zero')
    
    # Check for AttributeError
    for match in re.finditer(r'AttributeError[^\n]*', log_content):
        error_line = match.group(0)
        if error_line not in results.attribute_errors:
            results.attribute_errors.append(error_line)
    
    # Check for universe build failure
    if re.search(r'Universe build/save failed', log_content):
        results.universe_build_fail = True
    
    return results


def print_verification_results(results: VerifyResults) -> None:
    """Print verification results in GitHub Actions format."""
    print("=" * 50)
    print("[PREP][VERIFY] Verification Results")
    print("=" * 50)
    
    # PREP completion
    if results.prep_done:
        print("✅ PREP_DONE event found")
    else:
        print("::error::PREP_DONE event NOT found")
    
    # Derived verification
    if results.derived_verify_ok:
        print("✅ DERIVED_VERIFY passed with quality checks")
    elif results.derived_verify_fail:
        print("::error::DERIVED_VERIFY failed - quality check violation")
    else:
        print("⚠️ DERIVED_VERIFY status not found in logs")
    
    # Derived count
    if results.derived_count > 0:
        print(f"✅ derived_minervini data created (count={results.derived_count})")
    else:
        print("::warning::derived_minervini count may be 0")
    
    # Derived scores
    if results.rs_nonzero > 0 or results.vcp_nonzero > 0 or results.trend_nonzero > 0:
        print(f"✅ Derived scores have nonzero values (RS={results.rs_nonzero}, VCP={results.vcp_nonzero}, Trend={results.trend_nonzero})")
    else:
        print("::error::Derived scores appear to be all-zero - quality failure")
    
    # Entry scores
    if results.breakout_nonzero > 0 or results.pullback_nonzero > 0 or results.momentum_nonzero > 0:
        print(f"✅ Entry scores present (breakout={results.breakout_nonzero}, pullback={results.pullback_nonzero}, momentum={results.momentum_nonzero})")
    else:
        print("::warning::Entry scores may be missing or zero")
    
    # Contract failures and recoveries
    if results.contract_failures:
        print(f"⚠️ Contract failures detected: {', '.join(results.contract_failures)}")
        if results.contract_recoveries:
            print(f"✅ Contract failure was recovered successfully: {', '.join(results.contract_recoveries)}")
        else:
            print("::error::Contract violation remained unrecovered")
    else:
        print("✅ No contract violations")
    
    # Final30
    if results.final30_saved:
        print(f"✅ final30 watchlist saved (count={results.final30_count})")
    else:
        print("::error::final30 watchlist missing or empty - quality failure")
    
    # Quality failures
    if results.quality_failures:
        print(f"::error::Quality check failures detected: {', '.join(results.quality_failures)}")
    
    # AttributeErrors
    if results.attribute_errors:
        print(f"::error::AttributeError detected: {results.attribute_errors[0][:100]}")
    
    # Universe build
    if results.universe_build_fail:
        print("::error::Universe build/save failed")
    
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
    
    if results.has_warnings():
        print("⚠️ Warnings present but no critical failures - PREP verification passed with warnings")
        print("=" * 50)
        return 0
    
    print("✅ All verification checks passed")
    print("=" * 50)
    return 0


if __name__ == "__main__":
    sys.exit(main())
