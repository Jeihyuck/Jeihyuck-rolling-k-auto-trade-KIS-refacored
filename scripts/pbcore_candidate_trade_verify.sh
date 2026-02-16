#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi

log() {
  echo "[PB-VERIFY] $*"
}

die() {
  echo "[PB-VERIFY][FAIL] $*" >&2
  exit 1
}

ensure_runtime_deps() {
  local missing
  missing="$($PYTHON_BIN - <<'PY'
import importlib.util

required = [
    "numpy",
    "pandas",
    "sqlalchemy",
    "psycopg",
    "dotenv",
]
missing = [m for m in required if importlib.util.find_spec(m) is None]
print(" ".join(missing))
PY
)"

  if [[ -n "$missing" ]]; then
    log "Missing runtime deps: $missing"
    log "Installing requirements.txt"
    "$PYTHON_BIN" -m pip install -q -r "$ROOT_DIR/requirements.txt"
  fi
}

derived_as_of_for_trade() {
  "$PYTHON_BIN" - <<'PY'
from datetime import date, timedelta

today = date.today()
prev = today - timedelta(days=1)
while prev.weekday() >= 5:
    prev -= timedelta(days=1)
print(prev.isoformat())
PY
}

check_prep_done_for_as_of() {
  local as_of="$1"
  AS_OF="$as_of" STRATEGY_ENV="$STRATEGY_ENV" "$PYTHON_BIN" - <<'PY'
import os
from trader.db.engine import get_engine
from trader.db.repos import LedgerEventsRepo

env = (os.getenv("STRATEGY_ENV") or "practice").lower()
as_of = os.getenv("AS_OF")
repo = LedgerEventsRepo(get_engine())
ok = repo.has_event_type_on_date(env=env, event_type="PREP_DONE", as_of=as_of)

print("1" if ok else "0")
PY
}

if [[ -f "$ROOT_DIR/.env.local" ]]; then
  log "Loading .env.local"
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env.local"
  set +a
fi

[[ -n "${PBCORE_DB_URL:-}" ]] || die "PBCORE_DB_URL is not set. Put pooler URL in .env.local"

STRATEGY_ENV="${STRATEGY_ENV:-practice}"
STRATEGY_ENV="$(echo "$STRATEGY_ENV" | tr '[:upper:]' '[:lower:]')"

AS_OF="${AS_OF:-$(date +%F)}"
EXPECTED_POOL="${EXPECTED_POOL:-120}"
EXPECTED_FINAL="${EXPECTED_FINAL:-30}"
TRADE_LOG="${TRADE_LOG:-/tmp/pbcore_trade_verify.log}"
AUTO_PREP_IF_MISSING="${AUTO_PREP_IF_MISSING:-1}"

export STRATEGY_ENV
export KIS_ENV="${KIS_ENV:-$STRATEGY_ENV}"

ensure_runtime_deps

log "Preflight import check (SQLAlchemy + psycopg3)"
"$PYTHON_BIN" - <<'PY'
import sqlalchemy
import psycopg
print("SQLAlchemy:", sqlalchemy.__version__)
print("psycopg:", psycopg.__version__)
PY

log "DB smoke test"
"$PYTHON_BIN" - <<'PY'
import os
from sqlalchemy import create_engine, text

u = os.getenv("PBCORE_DB_URL")
assert u, "PBCORE_DB_URL missing"
u3 = u.replace("postgresql://", "postgresql+psycopg://", 1)
if "sslmode=" not in u3.lower():
    u3 += ("&" if "?" in u3 else "?") + "sslmode=require"

e = create_engine(u3, pool_pre_ping=True)
with e.connect() as c:
    print("DB_OK:", c.execute(text("select 1")).scalar())
PY

log "Run candidate (prefetch-only)"
MODE=candidate \
PREFETCH_DAYS="${PREFETCH_DAYS:-520}" \
OHLCV_DELTA_DAYS="${OHLCV_DELTA_DAYS:-5}" \
FORCE_CANDIDATE=1 \
DRYRUN=1 \
ANALYSIS_ONLY=1 \
FORCE_BLOCK_LIVE=1 \
"$PYTHON_BIN" -m trader.candidate_pool_builder --prefetch-only --env "$STRATEGY_ENV"

log "Run candidate (build pool)"
MODE=candidate \
PREFETCH_DAYS="${PREFETCH_DAYS:-520}" \
OHLCV_DELTA_DAYS="${OHLCV_DELTA_DAYS:-5}" \
FORCE_CANDIDATE=1 \
DRYRUN=1 \
ANALYSIS_ONLY=1 \
FORCE_BLOCK_LIVE=1 \
"$PYTHON_BIN" -m trader.candidate_pool_builder --build-only --build pool --env "$STRATEGY_ENV"

log "Validate watchlist counts (as_of=$AS_OF, env=$STRATEGY_ENV)"
AS_OF="$AS_OF" STRATEGY_ENV="$STRATEGY_ENV" EXPECTED_POOL="$EXPECTED_POOL" EXPECTED_FINAL="$EXPECTED_FINAL" "$PYTHON_BIN" - <<'PY'
import os
import sys
from sqlalchemy import create_engine, text

u = os.getenv("PBCORE_DB_URL")
assert u, "PBCORE_DB_URL missing"
u3 = u.replace("postgresql://", "postgresql+psycopg://", 1)
if "sslmode=" not in u3.lower():
    u3 += ("&" if "?" in u3 else "?") + "sslmode=require"

env = (os.getenv("STRATEGY_ENV") or "practice").lower()
as_of = os.getenv("AS_OF")
expected_pool = int(os.getenv("EXPECTED_POOL", "120"))
expected_final = int(os.getenv("EXPECTED_FINAL", "30"))

e = create_engine(u3, pool_pre_ping=True)
sql = text("""
select count(*) from pb1_watchlist
where env=:env and strategy=:s and as_of=:as_of
""")

with e.connect() as c:
    pool = c.execute(sql, {"env": env, "s": "pb1_candidate_pool", "as_of": as_of}).scalar()
    final = c.execute(sql, {"env": env, "s": "pb1_watchlist_final", "as_of": as_of}).scalar()

print("pb1_candidate_pool", pool)
print("pb1_watchlist_final", final)

if pool != expected_pool or final != expected_final:
    print(
        f"[VERIFY][FAIL] expected pool={expected_pool}, final={expected_final} but got pool={pool}, final={final}",
        file=sys.stderr,
    )
    sys.exit(1)

print("[VERIFY][OK] candidate counts matched")
PY

TRADE_DERIVED_AS_OF="$(derived_as_of_for_trade)"
log "Trade derived_as_of=$TRADE_DERIVED_AS_OF"

PREP_READY="$(check_prep_done_for_as_of "$TRADE_DERIVED_AS_OF" | tail -n1 | tr -d '[:space:]')"
if [[ "$PREP_READY" != "1" ]]; then
  log "PREP_DONE missing for derived_as_of=$TRADE_DERIVED_AS_OF"
  if [[ "$AUTO_PREP_IF_MISSING" == "1" ]]; then
    log "Running prep_runner once to bootstrap PREP_DONE"
    MODE=prep \
    STRATEGY_ENV="$STRATEGY_ENV" \
    KIS_ENV="$STRATEGY_ENV" \
    ALLOW_LIVE_GATE=0 \
    FORCE_BLOCK_LIVE=1 \
    DRYRUN=1 \
    ANALYSIS_ONLY=1 \
    "$PYTHON_BIN" -m trader.prep_runner

    PREP_READY="$(check_prep_done_for_as_of "$TRADE_DERIVED_AS_OF" | tail -n1 | tr -d '[:space:]')"
    [[ "$PREP_READY" == "1" ]] || die "prep_runner executed but PREP_DONE still missing for derived_as_of=$TRADE_DERIVED_AS_OF"
    log "PREP_DONE bootstrap completed"
  else
    die "PREP_DONE missing for derived_as_of=$TRADE_DERIVED_AS_OF (set AUTO_PREP_IF_MISSING=1)"
  fi
fi

log "Run trade verification with live blocks ON (log: $TRADE_LOG)"
MODE=trade \
STRATEGY_ENV="$STRATEGY_ENV" \
KIS_ENV="$STRATEGY_ENV" \
ALLOW_LIVE_GATE=0 \
FORCE_BLOCK_LIVE=1 \
DRYRUN=1 \
ANALYSIS_ONLY=1 \
"$PYTHON_BIN" -m trader.trade_tick 2>&1 | tee "$TRADE_LOG"

log "Check trade log checkpoints"
grep -q "\[TRADE\]\[WATCHLIST_FINAL\]\[LOCK\]" "$TRADE_LOG" || die "missing [TRADE][WATCHLIST_FINAL][LOCK]"
grep -q "members=30" "$TRADE_LOG" || die "missing members=30"
grep -q "\[TOP10\]" "$TRADE_LOG" || die "missing [TOP10]"

if grep -q "\[TRADE_TICK\]\[SKIP\] reason=PREP_NOT_DONE" "$TRADE_LOG"; then
  die "trade skipped by PREP_NOT_DONE guard"
fi

log "SUCCESS: candidate -> trade verification passed"