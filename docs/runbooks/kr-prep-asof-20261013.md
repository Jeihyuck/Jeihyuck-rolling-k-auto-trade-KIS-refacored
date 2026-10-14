# KR PREP 150+150 recovery using KIS official daily master files

## Source policy and constraints
- No KRX market-cap feed, credentials, paid market-data API or data-source switch
  is required by this PREP PR. KIS official public master ZIPs are used:
  `https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip`
  and `.../kosdaq_code.mst.zip`.
- The source contracts are the official KIS sample parsers
  `open-trading-api/stocks_info/kis_kospi_code_mst.py` (227-character record tail after LF removal)
  and `kis_kosdaq_code_mst.py` (221-character record tail after LF removal).
- Decode each file as CP949 *before* splitting the official character-width columns; byte-width splitting corrupts multibyte fields. The capitalisation
  field is nine characters before the final group (3), credit (1), mortgage (1),
  and stock lending (1) bytes. Filter invalid, duplicate or zero-cap codes.
  Rank each market independently and require **150 KOSPI + 150 KOSDAQ**.
- The daily master is **not a historical as-of API**. No proof of past
  month/date can be synthesized from it, nor from HTTP Date alone. To label
  members for the previous trading day, canonical PREP requires a morning
  05:00–09:00 KST fetch, the requested date to equal the previous verified
  trading session, and the ZIP Last-Modified to be **today in KST**.
  This is an operational previous-close convention, not independently
  authenticated individual stock price timestamps.
- The public KIS ZIP may not emit Last-Modified or may be stale. In that case
  fail closed for new BUY and report `kis_master_last_modified_missing` /
  `kis_master_stale_publication`. Never turn the old KIS [091] ranking
  (30 per market), stale DB data, or undated seeds into a 150+150 success.
- `source_sha256`, `source_published_on`, `actual_as_of` and market
  are stored in member metadata. Committed DB member identity, correct date
  and provider must match before reuse and after build.

## Live read-only validation (no trading)
```bash
cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
bash scripts/wsl/verify-kis-master-readonly.sh
```
The script verifies that **today's** official files are accessible and
parseable, with 150 distinct valid cap-ranked members from each exchange.
It does **not** claim a historical as-of date or write into the database.
GitHub's dedicated `KR PREP KIS Master Live Readonly Acceptance` performs
the identical check in CI without broker credentials.

## Full pre-merge acceptance
1. KIS read-only CI: 300 distinct, nonzero-cap members from today's source
   and a recent Last-Modified from both ZIP files.
2. Offline tests: CP949 decoding, fixed-width parsing, duplicates, zero/
   invalid caps, missing/stale Last-Modified, wrong as-of, morning guard.
3. PREP DB: committed KIS provider and exact previous-session identity.
   No regression to the prior KRX-only implementation.
4. Following morning with KIS practice data: canonical PREP scoring →
   Final30 30 → durable DB status and existing BUY/risk gating.
   Do not restart a live trading session to test it.
5. No scheduler changes, no broker orders in read-only CI, and no
   changes to protected SELL/reconciliation flows.

## Primary incident
2026-10-14 06:30 KIS [091] ranking supplied only about 30 per market, while
PREP required 150+150. Original attempt fell through to an absent emergency
seed and persisted a FAILED universe with zero members. The KIS master
solution avoids the [091] 30-row pagination contract instead of relaxing
the existing 300-name design policy.

### 2026-10-14 official-KIS layout correction
- Live KIS files first proved 150+150 retrieval, but the initial mock failed to catch a one-character cap offset. Official KIS scripts slice 228 (KOSPI) and 222 (KOSDAQ) **including** a terminal newline, while field specifications total 227 and 221. After whole-file CP949 decoding and splitlines(), the parser must use **227/221**.
- New regression checks the exact nine-character capitalisation value and the official field-width totals, not only the number of tickers. Fresh real-source CI must pass after this fix.

### Durable DB source evidence (pre-merge safety review)
- `universe_runs.build_reason` now preserves both KIS ZIP SHA-256 digests and the publication date in a compact versioned `kis_master_v1` token. `UniverseRepo.get_latest_successful_universe_snapshot` reads it back.
- Reused SUCCESS snapshots are rejected if the token is missing, malformed, fails previous-session/date validation, or has invalid checksums. The stored record still is not a cryptographic signature of KIS's source; it makes the exact input ZIP files auditable and rejects undated legacy runs.
- This uses the existing DB column; no schema or Windows scheduler changes. Existing prior SUCCESS rows that lack the token are intentionally **not** re-admitted for new BUY without fresh proof.
- Added an end-to-end mocked universe build regression asserting 300 persisted members and both ZIP hashes are supplied to the DB store call.
