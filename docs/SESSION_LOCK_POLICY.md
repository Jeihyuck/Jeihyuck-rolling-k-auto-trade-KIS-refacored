# Session lock policy

Trading wrapper locks are scoped by market and session (`kr-prep`, `kr-am`,
`kr-afternoon`, `kr-close`, and the corresponding US sessions). They use
`flock` for atomic exclusion and store PID, owner token, market, session,
trade date, creation time, and command metadata. A file that is not currently
flock-held is stale evidence, not an active process, and is replaced safely by
the next owner. Empty, malformed, dead-owner, expired, previous-date, and
out-of-scope metadata is reported with `[LOCK][STALE][DETECTED]`.

Mail/snapshot locks protect log packaging only and are excluded from trading
and deployment decisions. Deployment synchronization may be deferred while a
real session is running; wrappers continue on the currently checked-out code.
Only an active flock for the same session produces a successful duplicate skip.

Health output reports `stale_lock_detected_count`,
`stale_lock_removed_count`, `lock_warning_count`,
`duplicate_session_skips`, `active_same_session_count`, and
`order_idempotency_skips`. Lock warnings and successful duplicate skips do not
make health fail. Database advisory-lock failures and transactional order-ledger
idempotency remain hard protections and are not replaced by file locks.

## Lock inventory

| Lock | Creator / remover | Scope and metadata | Stale handling | Session effect |
| --- | --- | --- | --- | --- |
| `runtime/locks/kr-{prep,am,afternoon,close}.lock` | Corresponding KR wrapper / owner-token cleanup trap | One KR session; structured PID, token, market, session, trade date, timestamp, command | Validate metadata, PID and `/proc` command; replace stale metadata | Only a verified live same-session owner skips, with exit 0 |
| `runtime/locks/us-{prep,am,afternoon,close,trader}.lock` | Corresponding US wrapper / owner-token cleanup trap | One US session; same structured metadata | Same validation, TTL and next-run recovery | Only the identical US session skips |
| `runtime/locks/{kr,us}-mail-snapshot.lock` | Session-log reader (shared) and mail packager (exclusive) / file may remain | Mail snapshot consistency; `flock` state only | Leftover file has no trading meaning | Never blocks trading or deploy preflight |
| `runtime/locks/deploy-global.lock` | Code synchronizer / kernel releases flock | Code synchronization only | Unlocked file is harmless | Defers sync; trading continues with current pinned checkout |
| session manifest `*.lock` | Manifest writer / kernel releases flock | Atomic manifest update only | Unlocked file is harmless | No trading preflight effect |

Legacy empty/plain-PID/owner-sidecar files are accepted as inputs but classified
as stale only after their flock is acquired. A held flock is never unlinked or
bypassed: verified same-session owners return 75 and unverified owners safely
skip with return 76. Cleanup never signals another PID and never removes metadata whose
owner token has changed. `SIGINT`, `SIGTERM`, errors, and normal exits flow
through the session finalizer; `SIGKILL` is recovered on the next acquisition.
