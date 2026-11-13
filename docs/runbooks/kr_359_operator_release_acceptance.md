# KR PR #359: operator release and acceptance (2026-11-13)

**Code is CI-verified, not production-verified.** This runbook does not
authorize merging, database schema changes, broker transactions, or manual
order/position repair. #358 is **test-only** and must never be merged.

## Current observed truth (SELECT-only, 2026-11-13)

- `dual-agent` prior run SHA: `00f182ae9ff86a5615763f05f7a7e184a2e87277`;
  all 111 KR PB1 entry runs with a recorded SHA used that old commit.
- PR #359 source tree contains the exact code of #354–#357 and the resolved
  `trader/pb1_engine.py` overlap. The source PRs and integration unit tests
  previously passed, including isolated PostgreSQL migration 0063.
- In 644 historic-day PB1 BUY skips, 370 reported stale exchange quote,
  60 timing/price, and 214 existing holdings. That is **old-code evidence**,
  not the corrected hot path's observed performance.
- 25 of the 111 original entry run durations were >180s; this is **not**
  proof of a broker order submitted after the 180s limit.
- 2026-11-13 삼성E&A 028050: 33-cycle BUY, 8 prior SELL, 6 newly confirmed
  SELL, 19 DB shares remaining. One internal KIS-linked cumulative SELL fill
  exists with ID `0000004415`, but the evidence is not an independently
  queried original KIS broker record.
- KR CLOSE still `PENDING_SETTLEMENT`, one historical 2026-10-27 PB1 claim
  `UNCERTAIN`. The operational `0063` migration/triggers were not installed.
- The 2026-11-13 database's shared KR settlement evidence/application tables
  were empty. Do **not** infer this is safe or unsafe to repair without
  transaction-specific original KIS evidence.

### Pre-migration signed-contract audit (READ-ONLY)

The live database contained **930 signed PB1 BUY source orders with zero
malformed hashes or request-to-order scope mismatches**. Of four ACTIVE/OPEN
signed positions, **zero** had malformed signed source metadata. Five older
positions with status CLOSED and qty=0 lacked an exit-plan SHA in their old
entry metadata; those remain historical evidence debt, never automatic
backfill targets. The #359 release audit reports this split explicitly.

## Gate A: isolated code & native DB CI

Review the branch's CI, including the KR full acceptance and actual PostgreSQL
trigger tests. Verify base SHA and all source PR file blobs before changing
the account. Run `git rev-parse HEAD` **on the trading machine**, not GitHub;
the merged `dual-agent` SHA may differ from this PR's branch SHA.

## Gate B: approved deployment and schema cutover

Before installing anything, retain a read-only snapshot of active
account/epoch, source BUY contracts, positions, fills, execution claims,
schema migrations and triggers. Take the approved schema backup.

After a **separately authorized merge and controlled WSL update**, check the
effective code SHA and account. Apply `0063` through the repository's normal
approved migration mechanism, respecting the SQL splitter and a single
transaction. Do NOT manually execute a partial statement, manually create a
synthetic signed BUY, or install `0063` on an unverified old trading code path.

The following command **only reads** operational DB and should be executed
from the authorized operator WSL with its configured DB credentials:

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
NO_TRADE=1 KIS_ENV=practice STRATEGY_ENV=practice \
  .venv/bin/python -m scripts.kr_release_acceptance_readonly \
    --expected-sha "$(git rev-parse HEAD)" \
    --trade-date "$(TZ=Asia/Seoul date +%F)"
```

It checks code SHA from the day's recorded entry runs; one ACTIVE trading
epoch for the **current configured practice account**; schema migration 0063,
both enabled signed-contract guards; true submit-relative 180s vs total-run
duration; existing BUY skip breakdown; historical `UNCERTAIN` and CLOSE
`PENDING_SETTLEMENT` debt. It returns **2** for a database gate block or
**3** when DB gates pass but KIS/WSL independent proof remains unavailable.
It **never** reports overall live success from database rows alone.

## Gate C: authenticated practice KIS read-only native quote

During the Korean regular market session, select a legitimate **current**
Final30 code and run:

```bash
NO_TRADE=1 KIS_ENV=practice STRATEGY_ENV=practice \
  .venv/bin/python -m scripts.kr_quote_source_probe \
    --code 219130 --require-native-row
```

The example six-digit code above is **not a buy recommendation**. Replace it
with a *current* selected candidate. The probe disables all known order
permission flags and compares three **separate** evidence sources:
WS-first/current cache, independent REST inquire-price, and the actual
FHPST01060000 same-row `output2` time/volume/price source. The third
source must be authenticated, request-bound and from a single original row.
If absent (including after-hours), `--require-native-row` exits **3**, not
PASS. No quantity, date, exchange clock, or VWAP is fabricated.

KIS originals, exact account, date and order number must also be checked via
the already existing `scripts.kr_20261109_original_kis_proof` and
`scripts.kr_20261027_293490_readonly_proof` under explicit
`FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1`. A claim remains UNCERTAIN unless
original KIS execution and balance prove a specific matching original cycle.
Never relink, auto-satisfy, cancel or resubmit an ambiguous historical order.

## Gate D: regular-session observed execution

For the same operator SHA and account/epoch:
1. Confirm genuine WS `H0STCNT0` source day/clock/cumulative/VWAP and
   completed minute bar coverage across actual Final30, or explicit
   NOT_ATTESTED; check receipt freshness, no accidental time derivation.
2. Confirm both strict `KR_PB1_ENTRY_TIMING_MODE` policy and original
   BUY-to-SELL contract; risk gate and KIS submit timing. The 180s check is
   **submit elapsed time**, not whole tick elapsed time.
3. Confirm protective SELL is prioritized and not blocked by optional
   native BUY GET. Verify historical original KIS ACK, all-pages
   `daily_ccld`, physical fills, matched position cycle, zero duplicate
   execution and BUY/SELL value; never rely on fabricated broker records.
4. Review `PENDING_SETTLEMENT`, zero shared-settlement rows and the old
   `UNCERTAIN` claim as **unresolved proof**. Report at AM/CLOSE, not each
   tick. Do not loosen order submission, broker safety or guard policy
   to force a positive test.

**Gate conclusion:** Code CI PASS is not live PASS; the operator must supply
new independently observed WSL/KIS proof on the deployed code before declaring
full release acceptance. Keep #359 DRAFT until these gates are met.
