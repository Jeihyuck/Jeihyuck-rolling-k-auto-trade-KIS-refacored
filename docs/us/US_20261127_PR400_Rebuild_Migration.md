# US400 rebuild: preservation and acceptance

Archives (immutable reference branches; no force pushes):
- archive/us400-20261127-585b08a8: 585b08a88c831ea58cb8bed2915e84f68d43f677
- archive/us408-20261127-39a8c8e9: 39a8c8e931c540ef5d1c8193ee1e97921f2f822e
- dual-agent base: 5aa3c04702aa6f0eaeed2e9672bfc6db424d4850

The original ledger remains mandatory. All 41 IDs, FINAL V2 and V1.2 P0-12..17 remain in scope; no completion is implied by migrating code. KR compatibility only. No forced trades, DB mutation, strategy threshold changes, deployment or automatic merge.

| Stage | PR | Ownership and original ledger IDs | Remaining acceptance |
|---|---|---|---|
| 1 | #409 | order lifecycle 18–34, CLOSE 38 (shared primitives in repos; native quote prerequisite in stage 2) | focused PostgreSQL, exact BUY/SELL/restart/partial lineage; DHR/KO/TMO original-day KIS proof; no blind repair |
| 2 | #410 | collection/data 01–12, 17, 29, 41; V2-01..05, P0-13/17 | dynamic rank and repair deadline, independent benchmark recovery, native minute coverage and API quotas |
| 3 | #411 | protective and HOT canonical execution 13,25,39; V2-06, P0-15/16 | independent 5–10s all phases, cross-tick HOT consumption and latency; single writer |
| 4 | #412 | feature engine and performance 39,40; V2-08/10, P0-12/14/17 | remaining incremental formulas, equivalent workload mean <=60s then <=30s, p95 evidence, full final CI |
| 5 | portfolio and complete ledger | 35–36, signed policy and valuation, V2-07/09, final integrated 40 | signed original policy, KIS ACK/fill, WSL exact SHA/config, CLOSE five axes; economic research 14–16/37 stays non-authorizing |

Stages overlap on shared contracts by dependency, not duplicate ownership. Stacked draft bases isolate diffs; after predecessor passes and is merged, retarget to dual-agent and rerun all required CI at exact HEAD. Some general CI only triggers on dual-agent. Preserve no-skip offline native-stub gates and original credential restrictions; native skip is not operating acceptance. No old PR closes until exact tree/file reconciliation and requirement transfer verified. Old branches must not be used for deployment. Archive code had 4700 offline tests passed / 2 skipped; new branches require independent verification. Actual WSL and authenticated KIS are not accessible from the migration session.
