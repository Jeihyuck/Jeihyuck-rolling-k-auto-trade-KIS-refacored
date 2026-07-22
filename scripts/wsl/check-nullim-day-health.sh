#!/usr/bin/env bash
set -euo pipefail
APP="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MARKET="${1:-kr}"
DAY="${2:-$(TZ=Asia/Seoul date +%F)}"
cd "$APP"
mkdir -p runtime/health
POLICY_STATUS="OK"
FORBIDDEN=0
VERIFY_OUTPUT="$(bash scripts/wsl/verify-no-nullim-auto-scheduler.sh 2>&1)" || { POLICY_STATUS="FAIL"; FORBIDDEN="$(printf '%s\n' "$VERIFY_OUTPUT" | sed -n 's/.*forbidden_sources=\([0-9][0-9]*\).*/\1/p' | tail -1)"; FORBIDDEN="${FORBIDDEN:-1}"; }
printf '%s\n' "$VERIFY_OUTPUT"
OUT="runtime/health/${MARKET}-${DAY}.json"
SUMMARY="runtime/health/${MARKET}-${DAY}.summary.txt"
python - "$MARKET" "$DAY" "$OUT" "$SUMMARY" "$POLICY_STATUS" "$FORBIDDEN" <<'PY'
import json, re, sys
from pathlib import Path
market, day, out, summary, policy_status, forbidden = sys.argv[1:]
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
logs = list((root/'runtime/logs'/market/day).glob('*.log')) if (root/'runtime/logs'/market/day).exists() else []
logs += list((root/'runtime').glob(f'wsl-{market}-*.log'))
blob = text(logs)
# Flat US logs are append-only.  US health runs at 07:10 KST: inspect the
# complete overnight execution window from the prior KST day 19:00 through the
# requested KST day 07:30, rather than a fragile single-date substring.
if market == 'us':
    from datetime import datetime, time, timedelta
    from zoneinfo import ZoneInfo
    kst=ZoneInfo('Asia/Seoul')
    end=datetime.combine(datetime.fromisoformat(day).date(), time(7,30), kst)
    start=end-timedelta(hours=12, minutes=30)
    kept=[]
    for line in blob.splitlines():
        match=re.search(r'\[?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:?\d{2}|Z)?)', line)
        if not match:
            continue
        try:
            stamp=datetime.fromisoformat(match.group(1).replace('Z','+00:00')).astimezone(kst)
        except ValueError:
            continue
        if start <= stamp <= end:
            kept.append(line)
    blob='\n'.join(kept)
mail_marker = root/'runtime/health'/f'{market}-mail-{day}.json'
duplicate_skips=len(re.findall(r'SKIP_DUPLICATE|DUPLICATE_BLOCKED', blob, re.I))
advisory_unavailable=len(re.findall(r'PB1_ADVISORY_LOCK_UNAVAILABLE', blob))
result = {'market': market.upper(), 'date': day, 'automatic_scheduler_owner':'WINDOWS_TASK_SCHEDULER', 'scheduler_policy_status':policy_status, 'forbidden_wsl_scheduler_sources':forbidden, 'duplicate_session_skips':duplicate_skips, 'advisory_lock_unavailable_count':advisory_unavailable, 'tick_count': ticks(blob), 'mail_ok': False, 'logs_checked': [str(p) for p in logs]}
if mail_marker.exists():
    try: result['mail_ok'] = json.loads(mail_marker.read_text()).get('status') == 'OK'
    except Exception: pass
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
if forbidden or duplicate_skips or advisory_unavailable:
    result['ok']=False
    result['failure_reason']='SCHEDULER_POLICY_VIOLATION' if forbidden else 'DUPLICATE_SESSION_START'
Path(out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
lines=[f"NULLIM {market.upper()} health {day}"]+[f"- {k}: {v}" for k,v in result.items() if k!='logs_checked']
Path(summary).write_text('\n'.join(lines)+'\n', encoding='utf-8')
print('\n'.join(lines))
PY
