#!/usr/bin/env bash
set -euo pipefail
APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MARKET="${1:-}"
case "$MARKET" in kr|us) ;; *) echo "[HEALTH][FAIL] reason=MISSING_OR_INVALID_MARKET" >&2; exit 64;; esac
DAY="${2:-$(TZ=Asia/Seoul date +%F)}"
TRADE_DATE="$DAY"; [[ "$MARKET" == us ]] && TRADE_DATE="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
cd "$APP"
mkdir -p runtime/health
set +e
TRADING_DAY_JSON="$(python3 scripts/wsl/check-nullim-trading-day.py --market "$MARKET" --date "$TRADE_DATE" 2>&1)"
TRADING_DAY_RC=$?; set -e
printf '%s\n' "$TRADING_DAY_JSON"
if [[ "$TRADING_DAY_RC" == 10 ]]; then
 python3 - "$MARKET" "$DAY" "$TRADE_DATE" <<'PY_CLOSED'
import json,sys
from pathlib import Path
market,day,trade=sys.argv[1:]; result={'market':market.upper(),'date':day,'trade_date':trade,'status':'SKIPPED_NON_TRADING_DAY','ok':True,'mail_ok':True,'mail_required':False,'automatic_scheduler_owner':'WINDOWS_TASK_SCHEDULER'}
out=Path('runtime/health')/f'{market}-{day}.json'; summary=Path('runtime/health')/f'{market}-{day}.summary.txt'
out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n'); summary.write_text(f'NULLIM {market.upper()} health {day}\n- status: SKIPPED_NON_TRADING_DAY\n- ok: True\n')
print(json.dumps(result,ensure_ascii=False))
PY_CLOSED
 exit 0
elif [[ "$TRADING_DAY_RC" != 0 ]]; then echo '[HEALTH][FAIL] reason=CALENDAR_ERROR' >&2; exit 1; fi
POLICY_STATUS="OK"
FORBIDDEN=0
VERIFY_OUTPUT="$(bash scripts/wsl/verify-no-nullim-auto-scheduler.sh 2>&1)" || { POLICY_STATUS="FAIL"; FORBIDDEN="$(printf '%s\n' "$VERIFY_OUTPUT" | sed -n 's/.*forbidden_sources=\([0-9][0-9]*\).*/\1/p' | tail -1)"; FORBIDDEN="${FORBIDDEN:-1}"; }
printf '%s\n' "$VERIFY_OUTPUT"
OUT="runtime/health/${MARKET}-${DAY}.json"
SUMMARY="runtime/health/${MARKET}-${DAY}.summary.txt"
python - "$MARKET" "$DAY" "$TRADE_DATE" "$OUT" "$SUMMARY" "$POLICY_STATUS" "$FORBIDDEN" <<'PY'
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
result = {'market': market.upper(), 'date': day, 'trade_date': trade_date, 'automatic_scheduler_owner':'WINDOWS_TASK_SCHEDULER', 'scheduler_policy_status':policy_status, 'forbidden_wsl_scheduler_sources':forbidden, 'duplicate_session_skips':duplicate_skips, 'advisory_lock_unavailable_count':advisory_unavailable, 'tick_count': ticks(blob), 'mail_ok': False, 'logs_checked': [str(p) for p in logs]}
if mail_marker.exists():
    try:
        marker=json.loads(mail_marker.read_text())
        result['mail_ok'] = (marker.get('status') == 'OK' and marker.get('mail_sent') is True and marker.get('market') == market and marker.get('required_missing_count') == 0 and bool(marker.get('archive_sha256')) and marker.get('trade_date') == trade_date)
    except Exception: pass

install_marker=root/'runtime/health/windows-scheduler-install.json'
try:
    installed=json.loads(install_marker.read_text())
    current=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    result['scheduler_install_sha']=installed.get('installed_commit_sha')
    result['current_commit_sha']=current
    result['scheduler_sha_matches']=installed.get('status') == 'OK' and installed.get('installed_commit_sha') == current
except Exception:
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
if not result.get('scheduler_sha_matches'):
    result['ok']=False
    result['failure_reason']='FAILED_SCHEDULER_DRIFT'
if not result['mail_ok']:
    result['ok']=False
    result.setdefault('failure_reason','FAILED_MAIL_VALIDATION')
if forbidden or duplicate_skips or advisory_unavailable:
    result['ok']=False
    result['failure_reason']='SCHEDULER_POLICY_VIOLATION' if forbidden else 'DUPLICATE_SESSION_START'
Path(out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
lines=[f"NULLIM {market.upper()} health {day}"]+[f"- {k}: {v}" for k,v in result.items() if k!='logs_checked']
Path(summary).write_text('\n'.join(lines)+'\n', encoding='utf-8')
print('\n'.join(lines))
raise SystemExit(0 if result.get('ok') else 1)
PY
