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


## 2026-10-14 live-source acceptance result and authentication

A dedicated **read-only** GitHub Actions test on 2026-10-14 returned
`KRX fetch failed ... EmptyDataFrame` for `2026-10-13 KOSPI`.
The upstream pykrx call reported `KRX 로그인 실패:
KRX_ID 또는 KRX_PW 환경 변수가 설정되지 않았습니다.`.
Therefore the pre-existing mocked provider suite passing is NOT proof that
the current provider can build a 300-name universe in production.

Before declaring MERGE READY, verify KRX login exists on the actual WSL
host without printing credential values:

```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
set -a
source .env
set +a
for key in KRX_ID KRX_PW; do
  if [[ -n "${!key:-}" ]]; then echo "$key=SET"; else echo "$key=MISSING"; fi
done
```

If either is missing, configure authentication **outside Git and CI logs**.
Do not place credentials in PR content, issue comments, job output or artifacts.

A separate PR-only `KR PREP Live Marketcap Readonly Acceptance` checks for
`KRX_ID`/`KRX_PW` GitHub Actions secrets. It fetches the real dated source
and requires exactly 150 KOSPI + 150 KOSDAQ unique positive-cap symbols.
Absence of secrets or an upstream provider failure is a HARD acceptance failure.
A GitHub runner failing because it has no secrets does not prove the WSL host
has the same problem; WSL must be checked independently.

The prebuild is data-only, but once the credentials work the acceptance still
requires provider source-date validation, the subsequent 300→Final30 chain and
a fresh KIS practice session test. Until then the PREP repair remains DRAFT and
no earlier or undated data may be promoted for BUY.
