# FINAL V2.0 KR workstream — initial implementation, NOT ready to merge
Base 5aa3c04702aa6f0eaeed2e9672bfc6db424d4850. US stays in #400 (US-only scope); #378 is 379 commits behind at review, so new branch rather than merging stale code. Preserve #401, #397, #390; no deployment, order or operating DB mutation.
## P0 matrix
- P0-01 latency: OPEN; baseline KR 99.4s -> 132.7s (44/35 ticks, 09:00-10:28); P95 114.5 -> 174.5s; no raw WSL stage timings.
- P0-02/03/04 read paths, WS/REST, quota: PARTIAL canonical bounded direct quote can reuse existing 5s native REST cache without fetching; native-only and explicit refresh excluded. Need exact caller coverage, native attempt metrics, priority budget.
- P0-05 incremental features: OPEN.
- P0-06 independent protective SELL: OPEN, preserve single submitter and fresh authority.
- P0-07/08 PREP collection: PARTIAL separate atomic, day/account/SHA/digest-scoped observation-only manifest published after successful PREP; observer uses valid seed when execution shortlist absent/empty, bounded HOT5/WARM5 only. Within-family score round-robin is provisional observation ranking, not account risk/quantity eligibility. Verify actual export style fields, runtime PREP path and first-minute receipt; pre-open subscription still open.
- P0-09/10/11 opening intelligence, dynamic promotion, bounded events: OPEN. Reuse #400 common helpers after compatibility review; no direct observer orders.
- P0-12 existing entry policies: unchanged; full regression pending. KR 5+3 path preserved; US opening30 remains mandatory.
- P0-13 recovery: OPEN. KR 09:01 FULL_RECOVERY_UNVERIFIED precedes 10:27 S1 UNCERTAIN; trace both, no blind claim release. Observer independent; native proof still needed.
- P0-14 stops: OPEN; compare requested -3/-6 with observed -7/-8, verify exact strategy policy before changes.
- P0-15/16 resources/evidence: PARTIAL direct shared-cache hit log and PREP source; DB query reuse and unified telemetry open.
- P0-17 performance: OPEN; targets mean <=60s then <=30s, protected evaluation 5–10s, quote P95 <=5s, HOT completed bar P95 <=60s, cache P95 <=100ms. No performance claim. Preserve PR249/257 gates, report failures/timeouts separately, no safety bypass.
## Acceptance
Exact-head KR/US CI; actual WSL/KIS practice acceptance, no fabricated orders, exact native evidence for uncertain claims. Replay Pumtek 79,900 >79,800, Cosmax missing minute, S1 uncertain, post-buy lower lows; 20-session target subject to data availability. Existing strategies and order authority remain unchanged. Explicit rollback KR_PREP_COLLECTION_PRIORITY_ENABLED=0; old shortlist publisher unchanged. No merge until remaining mandatory scope and evidence resolved.