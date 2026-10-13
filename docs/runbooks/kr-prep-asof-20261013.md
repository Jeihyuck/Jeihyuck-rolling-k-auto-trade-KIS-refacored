# KR universe P0: exact-asof prebuild (2026-10-13)

Root incident: KIS market-cap ranking [091] returned only 30 rows per market
(no further tr_cont pages); KIS also returned `EGW00201` on a second request.
Prior PREP required 150+150 and had no strictly dated alternate source.

## Execution and ownership

Windows Task Scheduler remains the **only** automatic owner. This PR adds an
explicit-date, non-ordering WSL prebuild wrapper but **does not register or
change any task**. Do not add cron/systemd or automatic GitHub Actions trading
jobs. Coordinate a single-owner after-EOD invocation only after verifying KRX
daily data publication in the practice environment.

After deploying this branch, a manual **data-only** validation for the
completed Oct 13 session (for Oct 14 PREP) can be performed using:

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
bash scripts/wsl/prebuild-kr-universe-asof.sh 2026-10-13
```

Do **not** run `run-kr-prep.sh` merely to test this data path.
No KIS broker order is issued by the prebuild wrapper.
If KRX market-cap endpoint is unavailable, no unverified dated cache is
fabricated. The existing KR position-protection workflow is not changed.

## Provenance and operator acceptance

- Assert `requested_as_of == actual_as_of`; PREP rejects non-KRX/undated
  cache entries. The KRX call uses `max_rollback_days=0`.
- Target remains KOSPI 150 + KOSDAQ 150 before OHLCV sanitization;
  at least 100 validated members must survive sanitization.
- Supabase SUCCESS is published only after validation and saved transactionally;
  repeat builds reuse a verified same-date snapshot instead of resetting it.
- Check `public.universe_runs` and `public.universe_members` for `as_of`,
  `actual_as_of`, source, 6-digit unique tickers, market, market_cap, and
  post-sanitization count.
- pykrx may have an upstream issue or may not provide an independently
  attested publication timestamp. A returned requested date is not, alone,
  proof of actual exchange publication. Test live upstream date fidelity
  before merging to a real trading environment.
- Run the normal 06:30 PREP on a separately scheduled **next trading day**
  in practice to verify universe → four-family scoring → Final30 and complete
  BUY/SELL integrity. A data-only prebuild does not certify the whole PREP.

## Failure policy

When no trustworthy exact-asof source or snapshot exists, fail CLOSED for
new BUY and log the primary KRX failure. Never rewrite an older snapshot as
the new date. Never remove/reclassify broker fills, unresolved orders or
positions in response to a PREP failure.
