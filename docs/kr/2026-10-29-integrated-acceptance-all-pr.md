# 2026-10-29 KR combined P0/P1 acceptance — NO DEPLOY

This candidate integrates Draft PRs **#283, #286, #287, #288, #289, #290** against dual-agent `b1936bad6bf8ae6c7f789f6414f93d89298c4a1d` with exact shared-module edits.

**P0:** actual physical PostgreSQL TEXT fills.raw_json cumulative notional proof; durable KR PB1 order strategy owner; exact original KIS BUY + account/epoch/cycle/qty/amount/hashed-contract recovery of missing DAY close policy, with fail-closed no-guess fallback. No unsolicited fill or broker order mutation.

**Policy:** practice PB1 conditional MOMENTUM/BREAKOUT SWING enabled by default unless environment explicitly says 0; prior positions not reclassified; real env excluded.

**Timing:** quote sample shadow, KIS native completed 1m bar observation and owner-specific timing predicates. `KR_PB1_ENTRY_TIMING_MODE=SHADOW` by default: new BUY behavior unchanged. Independent read-only opening observer is **not installed or scheduled**, so deployment must be expressly approved.

**Acceptance prerequisites:** all CI passing on this integrated SHA, real 2026-10-29 Mico 1m data replay, native KIS practice schema proof, PostgreSQL text+REAL notional replay, original 79@26050 BUY->DAY policy close verified, same-day duplicate/cross-sleeve false positive blocked, every protective SELL retains priority and 180s deadline, price/fee/MDD backtest for unvalidated timing thresholds and overnight SWING risk review.

**Non-goals:** no automatic merge, WSL/scheduler change, KIS broker activity, Supabase production write, DB schema mutation, fabricated 1m bars or guaranteed profit. Test-generated fixtures are not live price evidence. Missing proof means UNKNOWN/WAIT, not READY.

Do not assert operational-ready or release any feature flag without above evidence.
