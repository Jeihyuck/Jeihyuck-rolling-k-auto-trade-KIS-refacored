#!/usr/bin/env bash
set -euo pipefail
APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MARKET="${1:-}"
case "$MARKET" in kr|us) ;; *) echo "[HEALTH][FAIL] reason=MISSING_OR_INVALID_MARKET" >&2; exit 64;; esac
DAY="${2:-$(TZ=Asia/Seoul date +%F)}"
TRADE_DATE="$DAY"; [[ "$MARKET" == us ]] && TRADE_DATE="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
cd "$APP"
source "$APP/scripts/wsl/resolve-nullim-python.sh"
CALENDAR_PYTHON="$(nullim_resolve_python "$APP")" || { echo "[CALENDAR][FAIL] reason=PROJECT_PYTHON_MISSING" >&2; exit 1; }
POLICY_STATUS="OK"
FORBIDDEN=0
VERIFY_OUTPUT="$(bash scripts/wsl/verify-no-nullim-auto-scheduler.sh 2>&1)" || { POLICY_STATUS="FAIL"; FORBIDDEN="$(printf '%s\n' "$VERIFY_OUTPUT" | sed -n 's/.*forbidden_sources=\([0-9][0-9]*\).*/\1/p' | tail -1)"; FORBIDDEN="${FORBIDDEN:-1}"; }
printf '%s\n' "$VERIFY_OUTPUT"
mkdir -p runtime/health
set +e
TRADING_DAY_JSON="$("$CALENDAR_PYTHON" scripts/wsl/check-nullim-trading-day.py --market "$MARKET" --date "$TRADE_DATE" 2>&1)"
TRADING_DAY_RC=$?; set -e
printf '%s\n' "$TRADING_DAY_JSON"
if [[ "$MARKET" == kr && "$TRADING_DAY_RC" != 0 ]]; then
 echo "[HEALTH][WARN] reason=KR_CALENDAR_ADVISORY rc=$TRADING_DAY_RC action=continue"
 TRADING_DAY_RC=0
fi
if [[ "$TRADING_DAY_RC" == 10 ]]; then
 "$CALENDAR_PYTHON" - "$MARKET" "$DAY" "$TRADE_DATE" "$POLICY_STATUS" "$FORBIDDEN" <<'PY_CLOSED'
import json,subprocess,sys
from pathlib import Path
market,day,trade,policy,forbidden=sys.argv[1:]; forbidden=int(forbidden); root=Path('.')
try:
 installed=json.loads((root/'runtime/health/windows-scheduler-install.json').read_text()); current=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
 owner_ok=installed.get('scheduler_owner')=='WINDOWS_TASK_SCHEDULER'; sha_ok=installed.get('status')=='OK' and installed.get('installed_commit_sha')==current
except Exception:
 installed={}; current='unknown'; owner_ok=False; sha_ok=False
ok=policy=='OK' and forbidden==0 and owner_ok and sha_ok
reason=None if ok else ('SCHEDULER_POLICY_VIOLATION' if forbidden or policy!='OK' or not owner_ok else 'FAILED_SCHEDULER_DRIFT')
result={'market':market.upper(),'date':day,'trade_date':trade,'status':'SKIPPED_NON_TRADING_DAY','mail_ok':True,'mail_delivered':True,'mail_complete':True,'mail_required':False,'automatic_scheduler_owner':'WINDOWS_TASK_SCHEDULER','scheduler_policy_status':policy,'forbidden_wsl_scheduler_sources':forbidden,'scheduler_sha_matches':sha_ok,'scheduler_owner_matches':owner_ok,'installed_commit_sha':installed.get('installed_commit_sha'),'current_commit_sha':current,'ok':ok}
if reason: result['failure_reason']=reason
out=root/'runtime/health'/f'{market}-{day}.json'; summary=root/'runtime/health'/f'{market}-{day}.summary.txt'
out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n'); summary.write_text(f'NULLIM {market.upper()} health {day}\n- status: SKIPPED_NON_TRADING_DAY\n- ok: {ok}\n')
print(json.dumps(result,ensure_ascii=False)); raise SystemExit(0 if ok else 1)
PY_CLOSED
 exit 0
elif [[ "$TRADING_DAY_RC" != 0 ]]; then echo '[HEALTH][FAIL] reason=CALENDAR_ERROR' >&2; exit 1; fi
OUT="runtime/health/${MARKET}-${DAY}.json"
SUMMARY="runtime/health/${MARKET}-${DAY}.summary.txt"
"$CALENDAR_PYTHON" - "$MARKET" "$DAY" "$TRADE_DATE" "$OUT" "$SUMMARY" "$POLICY_STATUS" "$FORBIDDEN" <<'PY'
import json, re, subprocess, sys
from pathlib import Path
market, day, trade_date, out, summary, policy_status, forbidden = sys.argv[1:]
forbidden = int(forbidden)
root = Path('.')
def text(paths):
    buf=[]
    for p in paths:
        if p.exists():
            try: buf.append(p.read_text(encoding='utf-8', errors='ignore')[-200000:])
            except Exception: pass
    return '\n'.join(buf)
def ticks(t): return len(re.findall(r'(?:US_TICK_LOOP\]\[TICK|\[TICK\]|tick=)', t, re.I))
log_partition = trade_date if market == 'us' else day
logs = list((root/'runtime/logs'/market/log_partition).rglob('*.log')) if (root/'runtime/logs'/market/log_partition).exists() else []
blob = text(logs)
# Date-scoped US logs retain every line, including shell errors without timestamps.
mail_marker = root/'runtime/health'/f'{market}-mail-{day}.json'
duplicate_skips=len(re.findall(r'SKIP_DUPLICATE|DUPLICATE_BLOCKED', blob, re.I))
advisory_unavailable=len(re.findall(r'PB1_ADVISORY_LOCK_UNAVAILABLE', blob))
stale_detected=len(re.findall(r'\[LOCK\]\[STALE\]\[DETECTED\]', blob))
stale_removed=len(re.findall(r'\[LOCK\]\[STALE\]\[REMOVED\]|\[LOCK\]\[RELEASED\]', blob))
lock_warnings=len(re.findall(r'\[LOCK\].*\[WARN\]', blob))
active_same_session=len(re.findall(r'\[LOCK\]\[ACTIVE\]\[SAME_SESSION\]\[SKIP\]', blob))
order_idempotency=len(re.findall(r'ORDER_(?:IDEMPOTENCY|SKIP_ALREADY_SUBMITTED)|ALREADY_SUBMITTED_THIS_SESSION', blob, re.I))
kr_inf_stale_pending=len(re.findall(r'\[EXIT_STARVATION\]\[KR_INF_STALE_PENDING\]', blob))
policy_missing_starvation=len(re.findall(r'\[EXIT_STARVATION\]\[POLICY_MISSING\]', blob))
policy_authority_conflicts=len(re.findall(r'\[EXIT\]\[POLICY_AUTHORITY\]\[CONFLICT_BLOCKED\]', blob))
accepted_sell_count=len(re.findall(r'\[TRADE\]\[ORDER\]\[SELL\].*result=ACCEPTED', blob, re.I))
execution_truth_mismatch=0
for log_path in logs:
    try:
        session_blob=log_path.read_text(encoding='utf-8', errors='ignore')[-200000:]
    except Exception:
        continue
    if (re.search(r'\[TRADE\]\[ORDER\]\[SELL\].*result=ACCEPTED', session_blob, re.I)
            and re.search(r'\[RUN_SUMMARY\]\[RESULT\].*orders_ack=0', session_blob, re.I)):
        execution_truth_mismatch += 1
result = {'market': market.upper(), 'date': day, 'trade_date': trade_date, 'automatic_scheduler_owner':'WINDOWS_TASK_SCHEDULER', 'scheduler_policy_status':policy_status, 'forbidden_wsl_scheduler_sources':forbidden, 'duplicate_session_skips':duplicate_skips, 'active_same_session_count':active_same_session, 'stale_lock_detected_count':stale_detected, 'stale_lock_removed_count':stale_removed, 'lock_warning_count':lock_warnings, 'order_idempotency_skips':order_idempotency, 'advisory_lock_unavailable_count':advisory_unavailable, 'advisory_lock_failure_count':advisory_unavailable, 'tick_count': ticks(blob), 'mail_ok': False, 'mail_delivered': False, 'mail_complete': False, 'logs_checked': [str(p) for p in logs],
          'kr_inf_stale_pending_count': kr_inf_stale_pending,
          'policy_missing_exit_starvation_count': policy_missing_starvation,
          'policy_authority_conflict_count': policy_authority_conflicts,
          'accepted_sell_count': accepted_sell_count,
          'execution_truth_mismatch_count': execution_truth_mismatch}
if mail_marker.exists():
    try:
        marker=json.loads(mail_marker.read_text())
        identity_ok = marker.get('market') == market and marker.get('trade_date') == trade_date and bool(marker.get('archive_sha256'))
        delivered = bool(identity_ok and marker.get('status') in {'OK','DEGRADED'} and marker.get('mail_sent') is True)
        complete = bool(delivered and marker.get('status') == 'OK' and int(marker.get('required_missing_count') or 0) == 0)
        result['mail_status'] = marker.get('status')
        result['mail_delivered'] = delivered
        result['mail_complete'] = complete
        result['mail_ok'] = complete
        result['mail_required_missing_count'] = int(marker.get('required_missing_count') or 0)
        result['mail_missing_evidence'] = marker.get('missing_evidence') or []
    except Exception: pass

install_marker=root/'runtime/health/windows-scheduler-install.json'
try:
    installed=json.loads(install_marker.read_text())
    current=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    result['scheduler_install_sha']=installed.get('installed_commit_sha')
    result['current_commit_sha']=current
    result['scheduler_owner_matches']=installed.get('scheduler_owner') == 'WINDOWS_TASK_SCHEDULER'
    result['scheduler_sha_matches']=installed.get('status') == 'OK' and installed.get('installed_commit_sha') == current
except Exception:
    result['scheduler_owner_matches']=False
    result['scheduler_sha_matches']=False

if market == 'us':
    prep = root/'reports/us_prep/latest_us_prep_summary.json'
    if prep.exists():
        try:
            d=json.loads(prep.read_text()); c=d.get('contract') or d
            result.update({k:c.get(k) for k in ['status','trade_can_proceed','entry_can_proceed','exit_can_proceed','close_can_proceed']})
        except Exception as e: result['prep_error']=str(e)
else:
    result['prep_final30_ok'] = 'final30=30' in blob or 'final30_rows=30' in blob
result['ok'] = bool(result.get('tick_count',0) >= 2 or re.search(r'session_end|graceful_shutdown|retryable close failure', blob, re.I))
if not result.get('scheduler_owner_matches'):
    result['ok']=False
    result['failure_reason']='SCHEDULER_POLICY_VIOLATION'
if not result.get('scheduler_sha_matches'):
    result['ok']=False
    result.setdefault('failure_reason','FAILED_SCHEDULER_DRIFT')
if not result['mail_ok']:
    result['ok']=False
    result.setdefault('failure_reason','DEGRADED_MAIL_EVIDENCE' if result.get('mail_delivered') else 'FAILED_MAIL_VALIDATION')
if forbidden or advisory_unavailable:
    result['ok']=False
    result['failure_reason']='SCHEDULER_POLICY_VIOLATION' if forbidden else 'PB1_ADVISORY_LOCK_UNAVAILABLE'
if market == 'kr' and kr_inf_stale_pending:
    result['ok']=False
    result['failure_reason']='KR_INF_EXIT_STARVATION'
elif market == 'kr' and policy_authority_conflicts:
    result['ok']=False
    result['failure_reason']='KR_POLICY_AUTHORITY_CONFLICT'
elif market == 'kr' and policy_missing_starvation:
    result['ok']=False
    result['failure_reason']='KR_POLICY_MISSING_EXIT_STARVATION'
elif market == 'kr' and execution_truth_mismatch:
    result['ok']=False
    result['failure_reason']='KR_EXECUTION_TRUTH_MISMATCH'
Path(out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
lines=[f"NULLIM {market.upper()} health {day}"]+[f"- {k}: {v}" for k,v in result.items() if k!='logs_checked']
Path(summary).write_text('\n'.join(lines)+'\n', encoding='utf-8')
print('\n'.join(lines))
raise SystemExit(0 if result.get('ok') else 1)
PY
