#!/usr/bin/env bash
set -euo pipefail
EX_USAGE=64
MARKET="${1:-}"
case "$MARKET" in
  kr|us) ;;
  all) [[ "${ALLOW_ALL_LOG_MAIL:-0}" == 1 && "${NULLIM_SCHEDULER_OWNER:-}" != WINDOWS_TASK_SCHEDULER ]] || { echo '[LOG_MAIL][FAIL] reason=ALL_NOT_ALLOWED' >&2; exit "$EX_USAGE"; } ;;
  *) echo '[LOG_MAIL][FAIL] reason=MISSING_OR_INVALID_MARKET' >&2; exit "$EX_USAGE" ;;
esac
APP="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/../.." && pwd -P)"; cd "$APP"
KST_RUN_DATE="${NULLIM_KST_RUN_DATE:-$(TZ=Asia/Seoul date +%F)}"
US_TRADE_DATE_ET="${US_TRADE_DATE:-$(TZ=America/New_York date +%F)}"
TRADE_DATE="$KST_RUN_DATE"; [[ "$MARKET" == us ]] && TRADE_DATE="$US_TRADE_DATE_ET"
TS="$(date +%Y%m%d-%H%M%S)"; BRANCH="$(git branch --show-current 2>/dev/null || echo unknown)"; BRANCH="${BRANCH:-unknown}"
BRANCH_SAFE="$(printf %s "$BRANCH"|sed 's/[^A-Za-z0-9._-]/-/g')"; OUT="${NULLIM_LOG_MAIL_OUT:-/tmp/nullim-${BRANCH_SAFE}-${MARKET}-logs-${TS}.tar.gz}"
WARN="${OUT%.tar.gz}.warn"; STAGE="$(mktemp -d "/tmp/nullim-${MARKET}-mail.XXXXXX")"; ATTEMPT="${NULLIM_MAIL_ATTEMPT_ID:-$(date +%s)-$$}"
MARKER="runtime/health/${MARKET}-mail-${KST_RUN_DATE}.json"; mkdir -p runtime/health
cleanup(){ rm -rf "$STAGE" "$WARN"; [[ "${NULLIM_MAIL_DRY_RUN:-0}" == 1 || "${NULLIM_KEEP_MAIL_ARCHIVE:-0}" == 1 ]] || rm -f "$OUT"; }
trap cleanup EXIT
fail(){ local reason="$1" rc="${2:-1}"; python3 - "$MARKER" "$MARKET" "$TRADE_DATE" "$reason" <<'PY'
import json,sys
from datetime import datetime,timezone
from pathlib import Path
p,m,d,r=sys.argv[1:]; Path(p).write_text(json.dumps({'status':'FAIL','market':m,'trade_date':d,'failure_reason':r,'sent_at':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
PY
 echo "[LOG_MAIL][FAIL] market=$MARKET reason=$reason" >&2; exit "$rc"; }
# Never package a live market session. flock tests the same advisory lock files.
if [[ "${NULLIM_SKIP_SESSION_ACTIVE_CHECK:-0}" != 1 ]]; then
  for lock in runtime/locks/${MARKET}-*.lock; do [[ -e "$lock" ]] || continue; exec {fd}>"$lock"; flock -n "$fd" || fail SESSION_STILL_RUNNING; eval "exec ${fd}>&-"; done
fi
LOG_ROOT="runtime/logs/$MARKET/$KST_RUN_DATE"; [[ -f "$LOG_ROOT/session-manifest.json" ]] || fail REQUIRED_LOG_MISSING
required=("$LOG_ROOT/session-manifest.json")
if [[ "$MARKET" == kr ]]; then sessions=(prep am afternoon close); else sessions=(prep am-preflight am afternoon close); fi
for session in "${sessions[@]}"; do
  found="$(find "$LOG_ROOT/$session" -maxdepth 1 -type f -name '*.log' -print -quit 2>/dev/null || true)"
  # Canonical prep may have an explicit purpose supplied by the scheduler.
  [[ -n "$found" ]] || fail "REQUIRED_LOG_MISSING_${session}"
  required+=("$found")
done
# Health and dated trading evidence are mandatory; accept the canonical layouts used by runners.
health="runtime/health/${MARKET}-${KST_RUN_DATE}.json"; [[ "$MARKET" == us && -f "reports/us_schedule_health/${TRADE_DATE}.json" ]] && health="reports/us_schedule_health/${TRADE_DATE}.json"
[[ -f "$health" ]] || fail REQUIRED_HEALTH_MISSING; required+=("$health")
if [[ "$MARKET" == kr ]]; then
  groups=("reports/kr/$TRADE_DATE" "runtime/kr/session/$TRADE_DATE" "runtime/trader_ledger")
else
  groups=("reports/us_daily/$TRADE_DATE" "reports/us_prep/$TRADE_DATE" "runtime/us/session/$TRADE_DATE")
fi
for item in "${groups[@]}"; do [[ -e "$item" ]] || fail "REQUIRED_EVIDENCE_MISSING_$(basename "$item")"; required+=("$item"); done
# Copy to a private staging tree, never archive mutable originals.
for item in "${required[@]}"; do mkdir -p "$STAGE/$(dirname "$item")"; cp -a "$item" "$STAGE/$item" || fail STAGING_COPY_FAILED; done
# Deterministic masking of credentials, bearer tokens, account numbers and email addresses.
find "$STAGE" -type f -print0 | xargs -0 -r sed -E -i \
 -e 's/((APP_KEY|APP_SECRET|KIS_APP_KEY|KIS_APP_SECRET|ACCESS_TOKEN|SMTP_PASS|DATABASE_URL|DB_URL|CANO|ACNT_PRDT_CD)[=:][[:space:]]*)[^[:space:]",]+/\1[REDACTED]/Ig' \
 -e 's/(Authorization:[[:space:]]*Bearer)[[:space:]]+[^[:space:]]+/\1 [REDACTED]/Ig' \
 -e 's/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/[REDACTED_EMAIL]/g' \
 -e 's/([0-9]{4})[- ]?[0-9]{4}[- ]?([0-9]{2,6})/\1-****-\2/g'
COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"; SESSION_ACTIVE=false
python3 - "$STAGE" "$MARKET" "$KST_RUN_DATE" "$US_TRADE_DATE_ET" "$BRANCH" "$COMMIT" "$ATTEMPT" "${required[@]}" <<'PY'
import hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
stage,market,kst,et,branch,commit,attempt,*required=sys.argv[1:]
files=sorted(p for p in Path(stage).rglob('*') if p.is_file())
h=hashlib.sha256()
for p in files: h.update(str(p.relative_to(stage)).encode()); h.update(p.read_bytes())
data={'market':market,'run_date_kst':kst,'trade_date_et':et,'branch':branch,'commit_sha':commit,'scheduler_owner':'WINDOWS_TASK_SCHEDULER','scheduler_task':__import__('os').environ.get('NULLIM_SCHEDULER_TASK_NAME','manual'),'archive_created_at':datetime.now(timezone.utc).isoformat(),'required_files':required,'included_required_files':required,'missing_required_files':[],'required_missing_count':0,'optional_files':[],'archive_sha256':h.hexdigest(),'redaction_applied':True,'session_active_at_packaging':False,'mail_attempt_id':attempt}
Path(stage,'NULLIM_LOG_ARCHIVE_MANIFEST.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
Path(stage,'NULLIM_LOG_ARCHIVE_MANIFEST.txt').write_text('\n'.join(f'{k}={v}' for k,v in data.items())+'\n')
PY
if ! tar -C "$STAGE" -czf "$OUT" . 2>"$WARN"; then fail TAR_FAILED; fi
[[ ! -s "$WARN" ]] || fail TAR_WARNING
if ! tar -tzf "$OUT" >/dev/null; then fail TAR_VERIFY_FAILED; fi
tar -tzf "$OUT" | grep -q 'NULLIM_LOG_ARCHIVE_MANIFEST.json' || fail ARCHIVE_MANIFEST_MISSING
SIZE="$(stat -c%s "$OUT")"; MAX=$(( ${NULLIM_MAIL_MAX_ATTACHMENT_MB:-15} * 1024 * 1024 )); (( SIZE <= MAX )) || fail ARCHIVE_TOO_LARGE
SHA="$(sha256sum "$OUT"|awk '{print $1}')"
if [[ -f "$MARKER" && "${NULLIM_FORCE_RESEND:-0}" != 1 ]] && python3 - "$MARKER" "$MARKET" "$TRADE_DATE" "$SHA" <<'PY'
import json,sys
try:d=json.load(open(sys.argv[1])); raise SystemExit(not(d.get('status')=='OK' and d.get('market')==sys.argv[2] and d.get('trade_date')==sys.argv[3] and d.get('archive_sha256')==sys.argv[4]))
except Exception: raise SystemExit(1)
PY
then echo "[LOG_MAIL][IDEMPOTENT_SKIP] archive_sha256=$SHA"; exit 0; fi
echo "[LOG_MAIL][ARCHIVE_OK] out=$OUT size=$SIZE sha256=$SHA"
MESSAGE_ID="dry-run-$ATTEMPT"
if [[ "${NULLIM_MAIL_DRY_RUN:-0}" != 1 ]]; then
  MESSAGE_ID="$("$APP/scripts/wsl/with-venv.sh" python "$APP/scripts/notify/send_mail_attachment.py" --subject "[NULLIM][$BRANCH][${MARKET^^}][LOG] $TRADE_DATE" --body "NULLIM ${MARKET^^} verified redacted log archive" --attach "$OUT" --attempt-id "$ATTEMPT" | tee /dev/stderr | sed -n 's/^\[MAIL\]\[MESSAGE_ID\] //p' | tail -1)" || fail FAILED_SMTP
  [[ -n "$MESSAGE_ID" ]] || MESSAGE_ID="$ATTEMPT"
fi
python3 - "$MARKER" "$MARKET" "$TRADE_DATE" "$KST_RUN_DATE" "$SHA" "$ATTEMPT" "$MESSAGE_ID" "$OUT" <<'PY'
import json,sys,os
from datetime import datetime,timezone
from pathlib import Path
p,m,d,k,sha,a,msg,out=sys.argv[1:]; recipient=os.getenv('MAIL_TO') or os.getenv('NAVER_MAIL_TO') or os.getenv('REPORT_MAIL_TO','dry-run')
Path(p).write_text(json.dumps({'status':'OK','market':m,'trade_date':d,'run_date_kst':k,'archive':out,'archive_sha256':sha,'required_missing_count':0,'mail_attempt_id':a,'message_id':msg,'recipient':recipient,'sent_at':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
PY
echo "[LOG_MAIL][OK] market=$MARKET archive=$OUT marker=$MARKER"
