#!/usr/bin/env bash
set -euo pipefail

EX_USAGE=64
MARKET="${1:-}"
case "$MARKET" in
  kr|us) ;;
  *) echo '[LOG_MAIL][FAIL] reason=MISSING_OR_INVALID_MARKET' >&2; exit "$EX_USAGE" ;;
esac

APP="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../.." && pwd -P)"
cd "$APP"
source "$APP/scripts/wsl/resolve-nullim-python.sh"
CALENDAR_PYTHON="$(nullim_resolve_python "$APP")" || {
  echo "[CALENDAR][FAIL] reason=PROJECT_PYTHON_MISSING" >&2
  exit 1
}

KST_RUN_DATE="${NULLIM_KST_RUN_DATE:-$(TZ=Asia/Seoul date +%F)}"
US_REFERENCE_DATE_ET="$(TZ=America/New_York date +%F)"

resolve_latest_completed_us_trade_date() {
  local reference_date="$1" dir base
  local -a candidates=()
  shopt -s nullglob
  candidates=(runtime/logs/us/20??-??-??)
  shopt -u nullglob
  if ((${#candidates[@]})); then
    while IFS= read -r dir; do
      base="$(basename "$dir")"
      [[ "$base" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || continue
      [[ "$base" > "$reference_date" ]] && continue
      [[ -f "$dir/session-manifest.json" ]] || continue
      if [[ -d "$dir/close" ]] && find "$dir/close" -maxdepth 1 -type f -name '*.log' -print -quit | grep -q .; then
        printf '%s\n' "$base"
        return 0
      fi
    done < <(printf '%s\n' "${candidates[@]}" | sort -r)
  fi
  printf '%s\n' "$reference_date"
}

TRADE_DATE="$KST_RUN_DATE"
if [[ "$MARKET" == us ]]; then
  if [[ -n "${US_TRADE_DATE:-}" ]]; then
    TRADE_DATE="$US_TRADE_DATE"
  else
    TRADE_DATE="$(resolve_latest_completed_us_trade_date "$US_REFERENCE_DATE_ET")"
    if [[ "$TRADE_DATE" != "$US_REFERENCE_DATE_ET" ]]; then
      echo "[LOG_MAIL][TRADE_DATE_RECOVERY] reference_et=$US_REFERENCE_DATE_ET resolved_trade_date=$TRADE_DATE source=latest_completed_partition"
    fi
  fi
fi

TS="$(date +%Y%m%d-%H%M%S)"
BRANCH="$(git branch --show-current 2>/dev/null || echo unknown)"
BRANCH="${BRANCH:-unknown}"
BRANCH_SAFE="$(printf %s "$BRANCH" | sed 's/[^A-Za-z0-9._-]/-/g')"
OUT="${NULLIM_LOG_MAIL_OUT:-/tmp/nullim-${BRANCH_SAFE}-${MARKET}-logs-${TS}.tar.gz}"
WARN="${OUT%.tar.gz}.warn"
STAGE="$(mktemp -d "/tmp/nullim-${MARKET}-mail.XXXXXX")"
TRACK="$(mktemp -d "/tmp/nullim-${MARKET}-mail-track.XXXXXX")"
ATTEMPT_GROUP="${NULLIM_MAIL_ATTEMPT_ID:-$(date +%s)-$$}"
PROD_MARKER="runtime/health/${MARKET}-mail-${KST_RUN_DATE}.json"
DRY_MARKER="runtime/health/${MARKET}-mail-dry-run-${KST_RUN_DATE}.json"
READINESS="runtime/health/${MARKET}-mail-readiness-${KST_RUN_DATE}.json"
EXPECTED_LIST="$TRACK/expected.tsv"
INCLUDED_LIST="$TRACK/included.txt"
MISSING_LIST="$TRACK/missing.tsv"
: > "$EXPECTED_LIST"; : > "$INCLUDED_LIST"; : > "$MISSING_LIST"
mkdir -p runtime/health

cleanup() {
  rm -rf "$STAGE" "$TRACK" "$WARN"
  [[ "${NULLIM_MAIL_DRY_RUN:-0}" == 1 || "${NULLIM_KEEP_MAIL_ARCHIVE:-0}" == 1 ]] || rm -f "$OUT"
}
trap cleanup EXIT

fail() {
  local reason="$1" rc="${2:-1}" missing_path="${3:-}" searched_file="${4:-}" marker="$PROD_MARKER"
  [[ "${NULLIM_MAIL_DRY_RUN:-0}" == 1 ]] && marker="$DRY_MARKER"
  python3 - "$marker" "$MARKET" "$TRADE_DATE" "$KST_RUN_DATE" "$reason" "$ATTEMPT_GROUP" "${MAIL_ATTEMPT_COUNT:-0}" "$missing_path" "$searched_file" <<'PY'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,d,k,r,a,count,missing,searched_file=sys.argv[1:]
data={'status':'FAIL','mail_sent':False,'mail_delivered':False,'mail_complete':False,'market':m,'trade_date':d,'run_date_kst':k,'failure_reason':r,'mail_attempt_id':a,'attempt_count':int(count),'sent_at':datetime.now(timezone.utc).isoformat()}
if missing: data['missing_path']=missing
if searched_file and Path(searched_file).is_file(): data['searched_paths']=[x for x in Path(searched_file).read_text().splitlines() if x]
Path(p).write_text(json.dumps(data,indent=2)+'\n')
PY
  local searched=""
  [[ -z "$searched_file" || ! -f "$searched_file" ]] || searched="$(paste -sd, "$searched_file")"
  echo "[LOG_MAIL][FAIL] market=$MARKET reason=$reason${missing_path:+ missing_path=$missing_path}${searched:+ searched_paths=$searched}" >&2
  exit "$rc"
}

record_expected() {
  local code="$1" path="$2"
  printf '%s\t%s\n' "$code" "$path" >> "$EXPECTED_LIST"
}

record_included() {
  local path="$1"
  printf '%s\n' "$path" >> "$INCLUDED_LIST"
}

record_missing() {
  local code="$1" path="$2"
  printf '%s\t%s\n' "$code" "$path" >> "$MISSING_LIST"
  echo "[LOG_MAIL][DEGRADED] market=$MARKET code=$code missing_path=$path" >&2
}

collect_path() {
  local code="$1" path="$2"
  record_expected "$code" "$path"
  if [[ -e "$path" ]]; then record_included "$path"; else record_missing "$code" "$path"; fi
}

collect_session_purpose() {
  local purpose="$1" dir="$LOG_ROOT/$purpose" code="REQUIRED_LOG_MISSING_${purpose}"
  record_expected "$code" "$dir"
  if [[ -d "$dir" ]] && find "$dir" -maxdepth 1 -type f -name '*.log' -print -quit | grep -q .; then
    record_included "$dir"
  else
    record_missing "$code" "$dir"
  fi
}

# KR calendar results remain advisory. US calendar remains authoritative for the
# resolved completed trade date.
set +e
TRADING_DAY_JSON="$("$CALENDAR_PYTHON" "$APP/scripts/wsl/check-nullim-trading-day.py" --market "$MARKET" --date "$TRADE_DATE" 2>&1)"
TRADING_DAY_RC=$?
set -e
printf '%s\n' "$TRADING_DAY_JSON"
if [[ "$TRADING_DAY_RC" == 10 ]]; then
  if [[ "$MARKET" == kr ]]; then
    echo "[LOG_MAIL][WARN] reason=MARKET_CLOSED_BUT_PACKAGE_LOGS market=$MARKET trade_date=$TRADE_DATE"
    TRADING_DAY_RC=0
  else
    python3 - "$PROD_MARKER" "$MARKET" "$TRADE_DATE" "$KST_RUN_DATE" <<'PY_CLOSED'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,d,k=sys.argv[1:]
Path(p).write_text(json.dumps({'status':'SKIPPED_NON_TRADING_DAY','mail_sent':False,'mail_delivered':False,'mail_complete':True,'mail_required':False,'market':m,'trade_date':d,'run_date_kst':k,'reason':'MARKET_HOLIDAY','ok':True,'created_at':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
PY_CLOSED
    echo "[LOG_MAIL][SKIP] reason=SKIPPED_NON_TRADING_DAY market=$MARKET trade_date=$TRADE_DATE"
    exit 0
  fi
elif [[ "$TRADING_DAY_RC" != 0 ]]; then
  if [[ "$MARKET" == kr ]]; then
    echo "[LOG_MAIL][WARN] reason=CALENDAR_ERROR_FAIL_OPEN market=$MARKET trade_date=$TRADE_DATE"
    TRADING_DAY_RC=0
  else
    echo "[LOG_MAIL][FAIL] reason=CALENDAR_ERROR market=$MARKET trade_date=$TRADE_DATE" >&2
    exit 1
  fi
fi

# Do not wait forever on a stale/active snapshot lock. A consistent archive is a
# hard requirement; missing evidence is not.
mkdir -p runtime/locks
exec {SNAPSHOT_FD}>"runtime/locks/${MARKET}-mail-snapshot.lock"
flock -w "${NULLIM_MAIL_SNAPSHOT_LOCK_TIMEOUT_SEC:-30}" -x "$SNAPSHOT_FD" || fail SNAPSHOT_LOCK_TIMEOUT 75

if [[ "${NULLIM_SKIP_SESSION_ACTIVE_CHECK:-0}" != 1 ]]; then
  for lock in runtime/locks/${MARKET}-*.lock; do
    [[ -e "$lock" ]] || continue
    [[ "$lock" == *-mail-snapshot.lock ]] && continue
    exec {fd}>"$lock"
    flock -n "$fd" || fail SESSION_STILL_RUNNING
    eval "exec ${fd}>&-"
  done
fi

resolve_kr_final30_evidence() {
  local env="$1" trade_date="$2" searched_file="$3"
  local trade_ledger="bot_state/trader_ledger/final30/$env/$trade_date"
  local runtime_final30="runtime/kr/watchlist/$trade_date/final30_scored.json"
  local contract="runtime/kr/watchlist/$trade_date/prep_contract.json"
  local prev_krx_trading_day contract_as_of source_final30 candidate
  local candidates=("$trade_ledger" "$runtime_final30")

  if [[ -f "$contract" ]]; then
    readarray -t contract_values < <(python3 - "$contract" <<'PY_CONTRACT'
import json, sys
try:
    data = json.load(open(sys.argv[1], encoding='utf-8'))
    print(str(data.get('as_of') or '')[:10])
    print(str((data.get('source_paths') or {}).get('final30') or ''))
except Exception:
    print('')
    print('')
PY_CONTRACT
)
    contract_as_of="${contract_values[0]:-}"
    source_final30="${contract_values[1]:-}"
    [[ -z "$contract_as_of" ]] || candidates+=("bot_state/trader_ledger/final30/$env/$contract_as_of")
    if [[ -n "$source_final30" ]]; then
      source_final30="$(python3 - "$APP" "$source_final30" <<'PY_SOURCE_PATH'
from pathlib import Path
import sys
app = Path(sys.argv[1]).resolve()
raw = Path(sys.argv[2])
resolved = raw.resolve() if raw.is_absolute() else (app / raw).resolve()
try:
    relative = resolved.relative_to(app)
except ValueError:
    raise SystemExit(2)
print(relative)
PY_SOURCE_PATH
)" || source_final30=""
      [[ -z "$source_final30" ]] || candidates+=("$source_final30")
    fi
  fi

  prev_krx_trading_day="$("$CALENDAR_PYTHON" - "$trade_date" <<'PY_PREV_KRX'
from datetime import date, timedelta
import sys
from pathlib import Path
ROOT = Path.cwd()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from trader.time_utils import resolve_krx_trading_day_strict
trade_date = date.fromisoformat(sys.argv[1])
candidate = trade_date - timedelta(days=1)
skipped = []
for _ in range(14):
    state, source = resolve_krx_trading_day_strict(candidate)
    if state == 'OPEN':
        print(candidate.isoformat())
        raise SystemExit(0)
    skipped.append(f'{candidate.isoformat()}:{state}:{source}')
    candidate -= timedelta(days=1)
raise RuntimeError(f"could not resolve previous KRX trading day before {trade_date.isoformat()}; skipped={','.join(skipped) if skipped else 'none'}")
PY_PREV_KRX
)" || return 2
  candidates+=("bot_state/trader_ledger/final30/$env/$prev_krx_trading_day")
  printf '%s\n' "${candidates[@]}" > "$searched_file"

  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" && "$(basename "$candidate")" == final30_scored.json ]] || [[ -d "$candidate" && -f "$candidate/final30_scored.json" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

LOG_ROOT="runtime/logs/$MARKET/$TRADE_DATE"
collect_path REQUIRED_LOG_MISSING "$LOG_ROOT/session-manifest.json"

if [[ "$MARKET" == kr ]]; then
  purposes=(prep am afternoon close)
  LEDGER_ENV="${NULLIM_LEDGER_ENV:-${STRATEGY_ENV:-practice}}"
  evidence_groups=("reports/kr_prep/$TRADE_DATE" "runtime/kr/watchlist/$TRADE_DATE" "runtime/kr/session/$TRADE_DATE")
  final30_search_file="$STAGE/kr-final30-searched-paths.txt"
  resolver_rc=0
  final30_evidence="$(resolve_kr_final30_evidence "$LEDGER_ENV" "$TRADE_DATE" "$final30_search_file")" || resolver_rc=$?
  expected_final30="bot_state/trader_ledger/final30/$LEDGER_ENV/$TRADE_DATE/final30_scored.json"
  if [[ "$resolver_rc" == 0 && -n "$final30_evidence" ]]; then
    record_expected REQUIRED_EVIDENCE_MISSING "$final30_evidence"
    record_included "$final30_evidence"
  elif [[ "$resolver_rc" == 2 ]]; then
    fail KRX_PREV_TRADING_DAY_RESOLVE_FAILED 1 "$TRADE_DATE" "$final30_search_file"
  else
    fail REQUIRED_EVIDENCE_MISSING 1 "$expected_final30" "$final30_search_file"
  fi
else
  purposes=(prep-prewarm-edt prep-prewarm-est prep prep-recovery am-preflight am afternoon close)
  evidence_groups=("reports/us_daily/$TRADE_DATE" "reports/us_prep/$TRADE_DATE" "runtime/us/session_state/$TRADE_DATE" "runtime/us/watchlist/$TRADE_DATE" "reports/us_schedule_health/$TRADE_DATE.json")
fi

for purpose in "${purposes[@]}"; do collect_session_purpose "$purpose"; done
for item in "${evidence_groups[@]}"; do collect_path REQUIRED_EVIDENCE_MISSING "$item"; done

MISSING_COUNT="$(wc -l < "$MISSING_LIST" | tr -d ' ')"
INCLUDED_COUNT="$(wc -l < "$INCLUDED_LIST" | tr -d ' ')"
EXPECTED_COUNT="$(wc -l < "$EXPECTED_LIST" | tr -d ' ')"
SESSION_EXPECTED_COUNT="${#purposes[@]}"
SESSION_MISSING_COUNT="$(awk -F '\t' '$1 ~ /^REQUIRED_LOG_MISSING_/ {count++} END {print count+0}' "$MISSING_LIST")"
SESSION_PRESENT_COUNT=$((SESSION_EXPECTED_COUNT - SESSION_MISSING_COUNT))
EVIDENCE_STATUS=OK
(( MISSING_COUNT == 0 )) || EVIDENCE_STATUS=DEGRADED

# Readiness is a pre-mail snapshot. It records incompleteness but does not block delivery.
python3 - "$READINESS" "$MARKET" "$KST_RUN_DATE" "$TRADE_DATE" "$EVIDENCE_STATUS" "$EXPECTED_LIST" "$INCLUDED_LIST" "$MISSING_LIST" <<'PY'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,k,d,status,expected_path,included_path,missing_path=sys.argv[1:]
def lines(path):
    q=Path(path)
    return q.read_text().splitlines() if q.is_file() else []
def parse_missing(path):
    out=[]
    for line in lines(path):
        code, _, value=line.partition('\t')
        out.append({'code':code,'path':value})
    return out
expected=lines(expected_path); included=lines(included_path); missing=parse_missing(missing_path)
data={'status':'READY' if status=='OK' else 'DEGRADED','market':m,'run_date_kst':k,'trade_date':d,'expected_evidence_count':len(expected),'included_evidence_count':len(included),'required_missing_count':len(missing),'missing_evidence':missing,'created_at':datetime.now(timezone.utc).isoformat()}
Path(p).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
PY

while IFS= read -r item; do
  [[ -n "$item" ]] || continue
  mkdir -p "$STAGE/$(dirname "$item")"
  cp -a "$item" "$STAGE/$item" || fail STAGING_COPY_FAILED 1 "$item"
done < "$INCLUDED_LIST"
mkdir -p "$STAGE/$(dirname "$READINESS")"
cp -a "$READINESS" "$STAGE/$READINESS" || fail STAGING_COPY_FAILED 1 "$READINESS"

# Deterministic evidence report is part of the evidence digest and archive.
python3 - "$STAGE/mail_evidence_report.json" "$MARKET" "$TRADE_DATE" "$EVIDENCE_STATUS" "$EXPECTED_LIST" "$INCLUDED_LIST" "$MISSING_LIST" "$SESSION_EXPECTED_COUNT" "$SESSION_PRESENT_COUNT" <<'PY'
import json,sys
from pathlib import Path
out,market,trade,status,expected_path,included_path,missing_path,session_expected,session_present=sys.argv[1:]
def lines(path):
    q=Path(path)
    return q.read_text().splitlines() if q.is_file() else []
def parse_expected(path):
    out=[]
    for line in lines(path):
        code, _, value=line.partition('\t')
        out.append({'code':code,'path':value})
    return out
def parse_missing(path): return parse_expected(path)
expected=parse_expected(expected_path); included=lines(included_path); missing=parse_missing(missing_path)
missing_sessions=[item['code'].removeprefix('REQUIRED_LOG_MISSING_') for item in missing if item['code'].startswith('REQUIRED_LOG_MISSING_')]
data={
    'status':status,
    'market':market,
    'trade_date':trade,
    'expected':int(session_expected),
    'present':int(session_present),
    'missing':missing_sessions,
    'expected_session_log_count':int(session_expected),
    'present_session_log_count':int(session_present),
    'missing_session_purposes':missing_sessions,
    'expected_evidence':expected,
    'included_evidence':included,
    'missing_evidence':missing,
    'required_missing_count':len(missing),
    'mail_delivery_policy':'SEND_DEGRADED_WHEN_EVIDENCE_IS_INCOMPLETE',
}
Path(out).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
PY

if [[ -f "$APP/scripts/wsl/redact-log-archive.py" ]]; then
  python3 "$APP/scripts/wsl/redact-log-archive.py" "$STAGE" || fail REDACTION_FAILED
else
  python3 - "$STAGE" <<'PY_REDACT' || fail REDACTION_FAILED
import json,re,sys
from pathlib import Path
root=Path(sys.argv[1]); keys={x.lower() for x in 'CANO ACNT_PRDT_CD APP_KEY APP_SECRET KIS_APP_KEY KIS_APP_SECRET ACCESS_TOKEN SMTP_PASS DATABASE_URL DB_URL authorization token password account recipient message_id'.split()}
email=re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
secret=re.compile(r'(?i)((?:APP_KEY|APP_SECRET|KIS_APP_KEY|KIS_APP_SECRET|ACCESS_TOKEN|SMTP_PASS|DATABASE_URL|DB_URL|CANO|ACNT_PRDT_CD|token|password|account)\s*[=:]\s*)[^\s,"]+')
account=re.compile(r'(?<![\d.])(\d{4})[- ]?\d{4}[- ]?(\d{2,6})(?![\d.])')
def clean(v):
    if isinstance(v,dict): return {k:'[REDACTED]' if str(k).lower() in keys else clean(x) for k,x in v.items()}
    if isinstance(v,list): return [clean(x) for x in v]
    return v
for p in root.rglob('*'):
    if not p.is_file(): continue
    if p.suffix.lower()=='.json': p.write_text(json.dumps(clean(json.loads(p.read_text())),ensure_ascii=False,indent=2)+'\n')
    elif p.suffix.lower() in {'.log','.txt','.md'}:
        s=p.read_text(errors='replace'); p.write_text(account.sub(r'\1-****-\2',email.sub('[REDACTED_EMAIL]',secret.sub(r'\1[REDACTED]',s))))
PY_REDACT
fi

if [[ -f "$APP/scripts/wsl/validate-json-files.py" ]]; then
  python3 "$APP/scripts/wsl/validate-json-files.py" "$STAGE" || fail INVALID_JSON_AFTER_REDACTION
else
  python3 - "$STAGE" <<'PY_JSON' || fail INVALID_JSON_AFTER_REDACTION
import json,sys
from pathlib import Path
for p in Path(sys.argv[1]).rglob('*.json'): json.load(p.open())
PY_JSON
fi

COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
RECIPIENT="${MAIL_TO:-${NAVER_MAIL_TO:-${REPORT_MAIL_TO:-dry-run}}}"
SOURCE_SHA="$(python3 - "$STAGE" "$READINESS" <<'PY'
import hashlib,sys
from pathlib import Path
stage,readiness=sys.argv[1:]
root=Path(stage); h=hashlib.sha256()
for p in sorted(x for x in root.rglob('*') if x.is_file() and str(x.relative_to(root)) != readiness):
    h.update(str(p.relative_to(root)).encode()+b'\0'); h.update(p.read_bytes())
print(h.hexdigest())
PY
)"

if [[ "${NULLIM_MAIL_DRY_RUN:-0}" != 1 && "${NULLIM_FORCE_RESEND:-0}" != 1 ]]; then
  IDEMPOTENT_MATCH="$(python3 - "runtime/health" "$MARKET" "$TRADE_DATE" "$RECIPIENT" "$SOURCE_SHA" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1]); market,trade,recipient,evidence_sha=sys.argv[2:]
for path in sorted(root.glob(f'{market}-mail-*.json')):
    try:
        data=json.loads(path.read_text())
    except Exception:
        continue
    if (
        data.get('status') in {'OK','DEGRADED'}
        and data.get('mail_sent') is True
        and data.get('market') == market
        and data.get('trade_date') == trade
        and data.get('recipient') == recipient
        and data.get('source_evidence_sha256') == evidence_sha
    ):
        print(path)
        break
PY
)"
  if [[ -n "$IDEMPOTENT_MATCH" ]]; then
    echo "[LOG_MAIL][IDEMPOTENT_SKIP] status=$EVIDENCE_STATUS source_evidence_sha256=$SOURCE_SHA prior_marker=$IDEMPOTENT_MATCH"
    exit 0
  fi
fi

python3 - "$STAGE" "$MARKET" "$KST_RUN_DATE" "$TRADE_DATE" "$BRANCH" "$COMMIT" "$ATTEMPT_GROUP" "$SOURCE_SHA" "$EVIDENCE_STATUS" "$EXPECTED_LIST" "$INCLUDED_LIST" "$MISSING_LIST" <<'PY'
import json,os,sys
from datetime import datetime,timezone
from pathlib import Path
stage,market,kst,trade,branch,commit,attempt,source,status,expected_path,included_path,missing_path=sys.argv[1:]
def lines(path):
    p=Path(path); return p.read_text().splitlines() if p.is_file() else []
def parse(path):
    out=[]
    for line in lines(path):
        code,_,value=line.partition('\t'); out.append({'code':code,'path':value})
    return out
expected=parse(expected_path); included=lines(included_path); missing=parse(missing_path)
data={'market':market,'run_date_kst':kst,'trade_date_et':trade,'branch':branch,'commit_sha':commit,'scheduler_owner':'WINDOWS_TASK_SCHEDULER','scheduler_task':os.getenv('NULLIM_SCHEDULER_TASK_NAME','manual'),'archive_created_at':datetime.now(timezone.utc).isoformat(),'evidence_status':status,'required_files':[x['path'] for x in expected],'included_required_files':included,'missing_required_files':[x['path'] for x in missing],'missing_evidence':missing,'required_missing_count':len(missing),'optional_files':[],'source_evidence_sha256':source,'redaction_applied':True,'session_active_at_packaging':False,'mail_attempt_id':attempt}
Path(stage,'NULLIM_LOG_ARCHIVE_MANIFEST.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
Path(stage,'NULLIM_LOG_ARCHIVE_MANIFEST.txt').write_text('\n'.join(f'{k}={v}' for k,v in data.items())+'\n')
PY

if [[ -f "$APP/scripts/wsl/validate-json-files.py" ]]; then
  python3 "$APP/scripts/wsl/validate-json-files.py" "$STAGE"
else
  python3 - "$STAGE" <<'PY_JSON'
import json,sys
from pathlib import Path
for p in Path(sys.argv[1]).rglob('*.json'): json.load(p.open())
PY_JSON
fi || fail INVALID_JSON_BEFORE_TAR

if ! tar -C "$STAGE" -czf "$OUT" . 2>"$WARN"; then fail TAR_FAILED; fi
[[ ! -s "$WARN" ]] || fail TAR_WARNING
tar -tzf "$OUT" >/dev/null || fail TAR_VERIFY_FAILED
if ! tar -tzf "$OUT" | grep -F 'NULLIM_LOG_ARCHIVE_MANIFEST.json' >/dev/null; then fail ARCHIVE_MANIFEST_MISSING; fi
SIZE="$(stat -c%s "$OUT")"
MAX=$(( ${NULLIM_MAIL_MAX_ATTACHMENT_MB:-15} * 1024 * 1024 ))
(( SIZE <= MAX )) || fail ARCHIVE_TOO_LARGE
ARCHIVE_SHA="$(sha256sum "$OUT" | awk '{print $1}')"
echo "[LOG_MAIL][ARCHIVE_OK] out=$OUT size=$SIZE evidence_status=$EVIDENCE_STATUS missing_count=$MISSING_COUNT source_sha256=$SOURCE_SHA archive_sha256=$ARCHIVE_SHA"

if [[ "${NULLIM_MAIL_DRY_RUN:-0}" == 1 ]]; then
  python3 - "$DRY_MARKER" "$MARKET" "$TRADE_DATE" "$KST_RUN_DATE" "$SOURCE_SHA" "$ARCHIVE_SHA" "$ATTEMPT_GROUP" "$OUT" "$EVIDENCE_STATUS" "$MISSING_LIST" <<'PY'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,d,k,source,archive,a,out,status,missing_path=sys.argv[1:]
missing=[]
for line in Path(missing_path).read_text().splitlines():
    code,_,value=line.partition('\t'); missing.append({'code':code,'path':value})
Path(p).write_text(json.dumps({'status':'DRY_RUN','evidence_status':status,'mail_sent':False,'mail_delivered':False,'mail_complete':status=='OK','market':m,'trade_date':d,'run_date_kst':k,'source_evidence_sha256':source,'archive_sha256':archive,'required_missing_count':len(missing),'missing_evidence':missing,'mail_attempt_id':a,'archive':out,'created_at':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
PY
  echo "[LOG_MAIL][DRY_RUN] market=$MARKET evidence_status=$EVIDENCE_STATUS archive=$OUT marker=$DRY_MARKER"
  exit 0
fi

if [[ "$EVIDENCE_STATUS" == DEGRADED ]]; then
  SUBJECT="[NULLIM][${MARKET^^}][DEGRADED] $TRADE_DATE trading logs"
  MISSING_SUMMARY="$(awk -F '\t' '{printf "%s%s:%s", (NR>1?", ":""), $1, $2}' "$MISSING_LIST")"
  BODY="NULLIM ${MARKET^^} redacted log archive (DEGRADED). Missing evidence: ${MISSING_SUMMARY}"
else
  SUBJECT="[NULLIM][${MARKET^^}] $TRADE_DATE trading logs"
  BODY="NULLIM ${MARKET^^} verified redacted log archive"
fi

mail_rc=1; attempt_count=0; message_id=""; unknown_after_send=0
for attempt_no in 1 2 3; do
  attempt_count="$attempt_no"; MAIL_ATTEMPT_COUNT="$attempt_no"
  echo "[LOG_MAIL][SEND_ATTEMPT] group=$ATTEMPT_GROUP attempt=$attempt_no evidence_status=$EVIDENCE_STATUS"
  set +e
  mail_output="$("$APP/scripts/wsl/with-venv.sh" python "$APP/scripts/notify/send_mail_attachment.py" --subject "$SUBJECT" --body "$BODY" --attach "$OUT" --attempt-id "$ATTEMPT_GROUP" 2>&1)"
  mail_rc=$?
  set -e
  printf '%s\n' "$mail_output"
  [[ "$mail_rc" == 75 ]] && unknown_after_send=1
  if [[ "$mail_rc" == 0 ]]; then
    message_id="$(printf '%s\n' "$mail_output" | sed -n 's/^\[MAIL\]\[MESSAGE_ID\] //p' | tail -1)"
    break
  fi
  [[ "$attempt_no" == 1 ]] && sleep "${NULLIM_SMTP_RETRY_SLEEP_1:-30}"
  [[ "$attempt_no" == 2 ]] && sleep "${NULLIM_SMTP_RETRY_SLEEP_2:-120}"
done
if [[ "$mail_rc" != 0 ]]; then
  [[ "$unknown_after_send" == 1 ]] && fail UNKNOWN_AFTER_SEND 75
  fail FAILED_SMTP
fi

python3 - "$PROD_MARKER" "$MARKET" "$TRADE_DATE" "$KST_RUN_DATE" "$SOURCE_SHA" "$ARCHIVE_SHA" "$ATTEMPT_GROUP" "$message_id" "$RECIPIENT" "$OUT" "$attempt_count" "$EVIDENCE_STATUS" "$MISSING_LIST" <<'PY'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,d,k,source,archive,a,msg,recipient,out,count,status,missing_path=sys.argv[1:]
missing=[]
for line in Path(missing_path).read_text().splitlines():
    code,_,value=line.partition('\t'); missing.append({'code':code,'path':value})
data={'status':status,'mail_sent':True,'mail_delivered':True,'mail_complete':status=='OK','market':m,'trade_date':d,'run_date_kst':k,'source_evidence_sha256':source,'archive_sha256':archive,'required_missing_count':len(missing),'missing_evidence':missing,'mail_attempt_id':a,'message_id':msg or a,'recipient':recipient,'archive':out,'attempt_count':int(count),'sent_at':datetime.now(timezone.utc).isoformat()}
Path(p).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
PY

echo "[LOG_MAIL][OK] market=$MARKET status=$EVIDENCE_STATUS archive=$OUT marker=$PROD_MARKER attempt_count=$attempt_count missing_count=$MISSING_COUNT"
