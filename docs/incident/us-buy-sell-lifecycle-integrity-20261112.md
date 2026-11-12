# US BUY→SELL lifecycle integrity — P0 evidence and release gate (2026-11-12 KST)

**Source baseline:** dual-agent `00f182ae9ff86a5615763f05f7a7e184a2e87277`. **Pull request:** #353 (DRAFT). **Environment:** KIS practice, PostgreSQL Supabase; evidence was read-only and no WSL shell or authenticated KIS request was available in this session.

## Root cause and source of truth

A broker balance provides an observed share count, not the original strategy owner, selected Pullback/Momentum/Breakout/Minervini entry family, immutable signed V2/V3 sell rules, portfolio cycle or pending SELL state. Treat **`us_order_intents`' original pre-POST signed BUY management contract** as the earliest durable original for new US_STANDARD orders; `us_orders` and `us_fills` form the actual KIS execution evidence. `us_position_risk_state` keeps changing lifecycle/stop state, while daily `us_positions` is an observation and not an immutable contract. TQQQ uses its dedicated state and cycle.

| Step | Existing canonical code | Durable evidence | Failure handling |
|---|---|---|---|
| BUY chosen style and V2/V3 frozen settings | `entry_exit_contract.py`; `order_router.route_order` | `us_order_intents.meta.entry_exit_contract` plus SHA256/version and style | Missing/invalid signed original → no KIS BUY |
| Claim and broker POST | `order_router.route_order`; execution claim and event journal | intent, `us_execution_claims`, attempts, `us_order_events` | POST unknown → no blind duplicate POST |
| Broker ACK and actual fill | `db/repos.py` save ACK and native cumulative fill reconciliation | `us_orders`, `us_fills` | ACK does not imply fill; reject unmatched scope |
| Active cycle, TP progress, HWM | `position_lifecycle_state.py`, `db/repos.py`, profit lifecycle | `us_position_risk_state`, `us_profit_capture_lifecycle` | never infer full fill from ACK or synthetic source |
| Authoritative KIS balance → day snapshot | `db/repos.save_position_snapshot`; `original_buy_lineage.py` | verified previous signed intent/order/fill plus exact account/epoch/cycle | observed shares may persist as UNVERIFIED; no guessed exit authority |
| Next-day position → SELL | `pb1/us_exit_position_resolver.py`, `pb1/us_exit_engine.py`, `order_router` | frozen contract + scoped current holding + actual TP/SELL evidence | one bad symbol must not clear unrelated protective SELL |
| Dedicated TQQQ cycle | `infinite/repository.py` and `infinite/integration.py` | native same-cycle KIS fills + authoritative broker qty | READY stays READY without matching actual native fills; CAS stops stale writers |

## Database read-only incident record

Latest available positive balance snapshot is **2026-11-11** (not a live 2026-11-12 KIS GET). All ten US_STANDARD positions have signed V2 contract copies but NULL formal account, portfolio epoch and cycle columns in that snapshot. Such signed copies alone do NOT authorize reconstructing historical original orders.

| Symbol | Shares 11/11 | Formal scope 11/11 | Original evidence status |
|---|---:|---|---|
| ABBV | 9 | missing | KIS original historical certification required |
| AMGN | 6 | missing | 11/10 prior contract/cycle/portfolio and risk-state original present; account hash in signed meta; bounded continuity proof candidate |
| DHR | 14 | missing | original historical certification required |
| HCA | 4 | missing | original historical certification required |
| KO | 27 | missing | older unresolved SELL in different/legacy epochs; broker status required |
| KR | 39 | missing | older unresolved SELL in different epoch; broker status required |
| MCK | 1 | missing | original historical certification required |
| PFE | 87 | missing | original historical certification required |
| PG | 12 | missing | original historical certification required |
| TMO | 5 | missing | 2026-10-30 current-epoch OPEN SELL; broker status required |
| TQQQ | 2 | TQQQ state, not US_STANDARD | same current-cycle native BUY cumulative fill of 2 in DB, but state READY; fresh KIS balance must confirm |

No real database row was repaired. No broker request was submitted. Current epoch alone or a signed snapshot copy cannot prove unknown historical original account/cycle. The existing #352 conditional V2 SELL validation policy must remain untouched.

## Fixes proposed in PR #353

1. Preserve immutable signed V2/V3 and selected entry family/version at the original BUY intent; verify full DB readback before any broker submission.
2. Explicit PostgreSQL position UPSERT checks account hash, portfolio epoch, cycle, original lifecycle and contract against existing row; RETURNING validates same-transaction post-write identity.
3. Find a uniquely verified original source even when one or several later observation days lost scope copies; only with unchanged positive shares, no intervening orders and no conflicting copied identity. No retroactive historical rewrite or whole-history every-tick reconcile.
4. TQQQ READY→ACTIVE requires exact active-cycle/epoch actual cumulative native BUY evidence with net quantity matching broker; no ACK/broker average fallback activation. Optimistic DB version CAS prevents stale READY overwriting ACTIVE.
5. Explicitly unregistered future strategy owner is rejected before KIS/DB entry; existing US_STANDARD four families and TQQQ owner stay unchanged.

## Gates still outstanding

- P0 integration for additional BUY/lot and mixed-contract holding, authoritative partial SELL allocation and late/cancelled fill variants.
- Practice authenticated KIS original order/fills and fresh account-wide balance / current orderable quantity.
- Historical KO/KR/TMO broker order status; distinguish same-cycle exposure from unrelated previous epoch.
- Real PostgreSQL migration/rollback and full crash injection / concurrent first fills / native SELL path tests; check **exact final SHA** CI (do not cite a previous successful SHA).
- WSL SHA/code pin and Windows scheduled task, practice natural BUY/FILL, next-day carry and actual condition-triggered SELL/FILL.

**Release state: DRAFT / HOLD.** No strategy or user-approved ACTIVE setting is downgraded. This code is not a claim of operational repair or readiness.
