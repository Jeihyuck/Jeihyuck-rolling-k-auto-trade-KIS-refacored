# US 2026-10-19 execution incident recovery

Base: `dual-agent` / `8406a8ef2a8e0d6d5a85a7372a852a253b6cecd9`.
Evidence: user-supplied `nullim-dual-agent-us-logs-20261020-070002.tar.gz`.
No private logs, account identities, credentials or broker responses are committed.

## Observed failure and reproduced causes

- 22:30 KST AM startup denied `US_LIVE_TRADING_ENABLED=0`. This is an intentional gate, not bypassed.
- 23:23 restarted AM and 02:00 afternoon had permission allowed, with
  `KIS_ENV=practice`, `DISABLE_REAL_TRADING=1`, paper/live/armed enabled.
  The startup resolver explicitly permits practice under the real-account stop,
  but `config.assert_us_paper_order_allowed` rejected it. The native-client guard
  now follows that practice policy. The client still accepts ONLY practice,
  uses the fixed VTS URL, and retains paper/live/dry-run gates. Real accounts
  remain rejected; this change does not set any runtime permission flags.
- PREP's saved contract omitted the run_id that exact-run DB preflight requires.
  Thirty valid scored rows therefore did not establish a completed live
  contract. Recovery used only the artifact and incorrectly skipped rebuilding.
  The writer now persists its DB run_id; recovery, preflight and post-completion
  cleanup use one exact-run/artifact/provenance verdict. Missing identity,
  mixed-run rows and DB failures cannot clear EXIT_ONLY.
- The router wrote `BROKER_SUBMIT_ABORTED_PRE_IO`, but aggregation/replay only
  understood `BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY`. All 86 known-unsent
  attempts were counted as submitted and replayed as potentially real orders.
  Both certificates now exclude an attempt from sent counts, broker recovery
  and fill attribution. Contradictory ACK/fill/unknown evidence keeps it
  broker-possible. A still-active claim after a failed release remains an
  explicit unresolved error; no historical claim is automatically deleted.
- PREP locked-row write failure and DB completion failure were allowed to
  produce a successful result. Both now fail the PREP run explicitly.
- Pre-I/O errors now expose an allowlisted cause code and exception classes
  without logging arbitrary exception text, URLs, accounts or tokens.

## Latency evidence and scope

Observed tick-start to first **submit attempt log**, not HTTP send:

| Session | n | Median seconds | Max seconds |
| --- | ---: | ---: | ---: |
| AM | 16 | 267.6305 | 292.154 |
| Afternoon | 27 | 260.752 | 291.281 |

Active-claim replay median was approximately 103 seconds in both sessions.
Before-route replay now receives a 10-second broker-read stage budget on an
isolated provider/client, with a rotating starting action. Deferred actions
retain durable fences and are counted as unresolved. Full close replay stays
unbounded by this stage budget and continues to examine old source-day fills.
This is not a hard 10-second wall-clock guarantee: initial DB/journal reads,
in-flight DB writes and synchronous provider work can overrun the deadline.
The entire protective SELL path is NOT proven below 60 seconds.

The deterministic same-input replay test uses 20 source-day queries, each with
5 seconds of virtual I/O cost. Full replay costs 100 virtual seconds; bounded
replay costs 10, defers 18 actions and still reports all 20 unresolved actions.
This proves request scheduling and conservative deferral, not live KIS speed.

## Validation

Initial unmodified-code incident regressions: 3 failed, 2 passed. Failures were
native practice permission, false sent count and unnecessary broker lookup.

Tests include native router + native client guard + mocked HTTP ACK, duplicate
fence, response loss/restart fencing, contradictory abort/ACK preservation,
exact-run PREP source checks, actual PREP runner identity propagation and DB
save/completion failure, historical replay and bounded-work fairness.

Linux offline test command (kernel-enforced network denial inherited by every
child process; fails closed if seccomp cannot be installed):

```bash
PYTHONPATH=. python scripts/run_pytest_no_network.py -q tests/us tests/test_us_order_permission.py
PYTHONPATH=. python scripts/run_pytest_no_network.py -q tests/kr tests/test_execution_claim_contract.py tests/test_reconcile_execution_claims.py
```

PostgreSQL tests requiring an actual server cannot pass in this network-denied
local mode. Two local collector IPC tests also require socket operations that
this execution sandbox disallows. Repository CI supplies PostgreSQL and the
IPC environment: the verified US merge-gate run on `95c044bc` passed **2,657
US tests (2 skipped)** and **7/7 offline scenarios**. CI and WSL/KIS practice
acceptance are separate results.

The KR suite exposed one pre-existing date-sensitive test failure on both
`8406a8ef` and the candidate: a fixed Oct-19 reconcile clock was combined with
the repository's actual current date. The test now freezes both module clock
references to the same Oct-19 instant; all original fence assertions remain.
There is no KR production-code change in this PR.

## Deployment and outstanding operational acceptance

1. Review the single Draft PR and its exact HEAD CI. No auto-merge or deployment.
2. On approved rollout, use a fresh same-day PREP; confirm the artifact run_id,
   completed DB run and all locked rows agree. Existing revision-pin and broker
   progress safeguards remain in force; never delete pins or markers to force BUYs.
3. Confirm order-permission diagnosis at both session start and native client.
   Do not set `DISABLE_REAL_TRADING=0` to work around the old mismatch.
4. Observe a strategy-valid practice order through HTTP begin, broker order
   number, exact fill and position update. Do not create an order just to make
   the acceptance metric pass or weaken the strategy/safety conditions.
5. Confirm original-date late fills and actual position ownership. The archive's
   seven recovered fills are not asserted to belong to the 86 unsent attempts.
6. Existing uncertain NET/META/TMO actions require exact broker evidence. This
   PR does not manufacture fill prices, reset unknown claims or erase the 88
   unresolved actions. Broker/local fill mismatches must remain visible.
7. Compare at least 20 baseline/candidate ticks at the actual HTTP boundary.
   Any claim of operational recovery or a 60-second target requires this new
   WSL/KIS practice evidence; unit tests and CI alone do not establish it.

Rollback: revert this PR's single implementation commit through a reviewed
change. No schema migration, operational DB cleanup, scheduler mutation,
strategy threshold change or runtime flag change is included.
