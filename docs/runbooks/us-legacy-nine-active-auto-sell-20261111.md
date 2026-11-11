# Existing 9 US_STANDARD V2 positions: active SELL rescue

This implementation **activates** the nine-owner path in KIS practice by default
(US_LEGACY_NINE_AUTO_SELL_ACTIVE defaults to 1). It never uses SHADOW and never
turns all existing holdings into mandatory liquidation.

The canonical router sets automated_sell_authorized=true ONLY after all of:
1. Frozen original signed V2 contract matches current legacy us_positions SHA.
2. Unique original BUY order AND same-epoch native KIS cumulative BUY fill.
3. Last archived open runtime lifecycle (prior to the 2026-11-05 new-day reset)
   matches actual authenticated KIS practice account, epoch, owner, source BUY
   date; never treats ENTRY:US_STANDARD:<symbol> as a real holding cycle.
4. Every partially executed SELL matches the real old runtime cycle, local
   order/fill totals, and authenticated KIS original-day final cumulative
   fill; no synthetic fill, disagreement, or unknown SELL.
5. Historical KO/KR/TMO OPEN/ACKs, including prior epochs, are either proven
   KIS terminal with matching cumulative DB fill, or that symbol is BLOCKED.
6. All already-DONE TP stages for this source original runtime cycle stay DONE.
   Repeating TP1 after a finished TP1 is blocked (without changing V2 policy).
7. Fresh complete KIS holding + orderable and current-day order book match,
   with no BUY/SELL/cancel pending for that symbol.
8. A new *prospective* management cycle is derived from CURRENT account,
   active owner portfolio, immutable old runtime cycle, and V2 contract SHA.
   It is attached only to the new SELL and marked forward_management_not_original_buy.
   The original BUY, historical fills and original portfolio scope are untouched.
9. The normal router still handles risk checks, intent persistence, account
   claim, journal/outbox, Stage4 worker fence, native KIS limit SELL and fills.
   A second fresh broker balance/order GET occurs at the last pre-HTTP
   submit boundary; change or uncertainty aborts without automatic retries.

Target symbols: ABBV, DHR, HCA, KO, KR, MCK, PFE, PG, TMO.
Never TQQQ_INFINITE, other owners, real brokerage, or new fully scoped buys.

No separate WSL enable switch is required for the code path to be active when
the normal practice order environment is already enabled. However no
automated SELL can run until authenticated native KIS GETs succeed and an
unchanged V2 strategy independently produces a SELL signal.
US_LEGACY_NINE_AUTO_SELL_ACTIVE=0 is the explicit emergency rollback.

Safety acceptance before calling this operationally complete:
- green exact-head full CI; native WSL practice KIS original-day proof,
  full account balance, source-day old SELL order statuses, current order book;
- prove no old unresolved SELL can be newly submitted; sample test old
  TP1 already DONE then next V2 TP2/HARD_STOP and DB native cumulative fill;
- crash + restart + duplicate action + cross account, current/pre-HTTP KIS
  quantity changes and Stage4 outage must be fail-closed;
- after KIS actual paper fill, verify new SELL order intent, claim/outbox,
  KIS order no, fill and current balance are consistent. Do not infer live
  KIS success from a mocked CI test.

Do not run this helper as an order-submit CLI: it has no POST. The only
execution call site is the existing order_router with an actual V2 SELL intent.
