#!/usr/bin/env bash
set -euo pipefail

LOG="${1:-prep.log}"
VERIFY_LOG="${2:-/tmp/prep_verify.out}"

critical=0

echo "==================================================" | tee "$VERIFY_LOG"
echo "[PREP][VERIFY] Verification Results" | tee -a "$VERIFY_LOG"
echo "==================================================" | tee -a "$VERIFY_LOG"

if grep -q "\[DB\]\[LEDGER_EVENT\]\[APPEND\].*PREP_DONE" "$LOG" || grep -q "\[LEDGER_EVENT\].*event_type=PREP_DONE" "$LOG"; then
	echo "PASS PREP_DONE found" | tee -a "$VERIFY_LOG"
else
	echo "FAIL PREP_DONE missing" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "\[PREP\]\[DONE\]" "$LOG"; then
	echo "PASS [PREP][DONE] found" | tee -a "$VERIFY_LOG"
else
	echo "FAIL [PREP][DONE] missing" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "\[PREP\]\[ASOF_CONSISTENCY\].*consistent=1" "$LOG"; then
	echo "PASS as_of consistency passed" | tee -a "$VERIFY_LOG"
else
	echo "FAIL as_of consistency failed" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "reject_future_snapshot" "$LOG"; then
	echo "PASS candidate pool future snapshot rejected" | tee -a "$VERIFY_LOG"
else
	echo "INFO no future snapshot rejection observed" | tee -a "$VERIFY_LOG"
fi

if grep -q "\[PREP\]\[DERIVED_VERIFY\]\[OK\]" "$LOG"; then
	echo "PASS derived verify passed" | tee -a "$VERIFY_LOG"
else
	echo "FAIL derived verify missing" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "\[DERIVED\]\[MINERVINI\]\[ENTRY_SCORES\]" "$LOG" || grep -q "\[PREP\]\[EXPORT\]\[FINAL30\]\[INMEM\].*breakout_nonzero=.*pullback_nonzero=.*momentum_nonzero" "$LOG"; then
	echo "PASS entry scores present" | tee -a "$VERIFY_LOG"
else
	echo "FAIL entry scores missing" | tee -a "$VERIFY_LOG"
	critical=1
fi

contract_failed=0
contract_recovered=0

grep -q "\[CONTRACT_VALIDATION\]\[FAIL\]" "$LOG" && contract_failed=1 || true
grep -q "\[PREP\]\[WATCHLIST\]\[CONTRACT_FAIL\]" "$LOG" && contract_failed=1 || true
grep -q "\[PREP\]\[WATCHLIST\]\[CONTRACT\]\[RECOVERED\]" "$LOG" && contract_recovered=1 || true
grep -q "\[PREP\]\[WATCHLIST\]\[RECOVERY\]\[DB_SUCCESS\]" "$LOG" && contract_recovered=1 || true

if [[ "$contract_failed" -eq 1 && "$contract_recovered" -eq 0 ]]; then
	echo "FAIL unrecovered contract failure" | tee -a "$VERIFY_LOG"
	critical=1
else
	echo "PASS contract violation recovered successfully" | tee -a "$VERIFY_LOG"
fi

if grep -q "\[PREP\]\[WATCHLIST_FINAL\]\[SAVE\].*n=30" "$LOG" || grep -q "\[PREP\]\[FINAL30_SNAPSHOT\]\[SAVE\].*count=30" "$LOG"; then
	echo "PASS final30 save found (count=30)" | tee -a "$VERIFY_LOG"
else
	echo "FAIL final30 save missing" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "\[PREP\]\[EXPORT\]\[CONSISTENCY\]" "$LOG" && \
	 grep -q "lhs_has_tech=True.*rhs_has_tech=True.*lhs_has_final=True.*rhs_has_final=True" "$LOG"; then
	echo "PASS exporter preserved score fields" | tee -a "$VERIFY_LOG"
else
	echo "FAIL exporter score field preservation failed" | tee -a "$VERIFY_LOG"
	critical=1
fi

if grep -q "Traceback (most recent call last)" "$LOG"; then
	if grep -q "\[TIME\]\[TRADING_DAY\]\[PYKRX_FAIL\].*fallback=" "$LOG" && \
	   { grep -q "\[PREP\]\[DONE\]" "$LOG" || grep -q "event_type=PREP_DONE" "$LOG"; }; then
		echo "PASS traceback classified as non-fatal PYKRX fallback noise" | tee -a "$VERIFY_LOG"
	else
		echo "FAIL Python traceback detected" | tee -a "$VERIFY_LOG"
		critical=1
	fi
else
	echo "PASS no traceback detected" | tee -a "$VERIFY_LOG"
fi

echo "==================================================" | tee -a "$VERIFY_LOG"

if [[ "$critical" -eq 1 ]]; then
	echo "Error: Critical failures detected - PREP verification failed" | tee -a "$VERIFY_LOG"
	exit 1
fi

echo "PREP verification passed" | tee -a "$VERIFY_LOG"
