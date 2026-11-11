# US legacy-nine SELL rescue — operating contract (2026-11-11)

## Purpose

Original BUY/SELL KIS fills, 2026-11-04 historical runtime lifecycle IDs, and original V2 signed contracts remain; the post-11-05 position snapshot lost canonical account/portfolio/cycle fields. The strict canonical order router correctly rejects an unproven original cycle. Never invent a historical ID or backdate an epoch.

**An independently authenticated KIS broker/manual SELL does not require our application position-cycle ID.** It does require the correct practice account, full fresh broker holdings/orderable shares and no unknown competing SELLs.

This is an ACTIVE GET-only manual operation preflight, not a SHADOW trading strategy; it never places or cancels orders, releases claims or edits DB.

## Immediate intervention: broker manual SELL

1. Pause overlapping US AM, Afternoon and Stage4 WSL order sessions; verify no process can submit concurrently.
2. In the same authenticated KIS practice account's supported manual order screen (MTS/HTS if supported), check exact ticker, exchange, holding and orderable shares.
3. Inspect every unresolved DB SELL against its **original-day KIS broker state**, especially:
   - KO: 2026-08-25 #0000041180, 2026-08-26 #0000038384, 2026-10-15 #0000043554
   - KR: 2026-10-15 #0000043559
   - TMO: 2026-10-30 #0000036019
   An old local OPEN/ACK does not mean live KIS OPEN; nor does age prove cancellation.
4. For an algo-triggered exit, respect the original immutable V2 staged TP and stop contract. A discretionary sell must be explicitly labeled manual intervention, not an automatic strategy signal.
5. Submit only in the authenticated broker interface, quantity <= independently confirmed **current orderable** shares, with reviewed order price/limit. A limit order may not fill. Never duplicate a timed-out or unknown order.
6. Obtain broker ACK, original order number, cumulative actual fills, remaining qty and ending balance. Reconcile the manual external order into the journal/DB, retaining independent manual provenance. Do not resume the bot until outstanding SELL conflicts are cleared.

## Read-only evidence command on authorized WSL

    cd ~/apps/Jeihyuck-rolling-k-auto-trade-KIS-refacored
    git fetch origin && git checkout dual-agent && git pull --ff-only
    KIS_ENV=practice WSL_RUN_MARKET=US NO_TRADE=1 DRY_RUN=1 \
      DISABLE_REAL_TRADING=1 US_ORDER_ARMED=0 \
      .venv/bin/python -m scripts.us_legacy_nine_sell_readiness \
        --trade-date YYYY-MM-DD --confirm-practice-readonly

Run only with other trading sessions stopped. This collects KIS GET-only across configured exchanges/current date and original dates of every unresolved US SELL, plus PostgreSQL SELECT in a READ ONLY transaction. Its output is manual broker review guidance; **automated_sell_authorized is always false**. It does not mutate positions, release stage4 claims, submit a SELL or force a policy.

## Permanent automatic rescue — separate active forward-only SELL implementation

The new code must create an effective-now, signed management authorization, **not** rewrite the original October BUY portfolio epoch. Require:
- Original BUY KIS order and actual native fills, matching immutable V2 SHA and US_STANDARD owner; never include TQQQ Infinite.
- Historical pre-Nov-05 real runtime lifecycle ID and TP stage ledger, with original SELL states verified against source-day KIS.
- Fresh native broker balance, orderable_qty and order book, no unresolved claim or duplicate SELL; one account-scoped writer.
- Exact V2 TP/stops remain authoritative. V3 transition is separate and cannot be inferred or silently enabled.
- Explicit effective-now forward carry identifier distinct from original BUY scope; durable signed claim, canonical outbox, single KIS submit, native fill and reconciliation.
- Test next-day restart, TP1->TP2, full exit, reentry, cancellation, Stage4 stop, KIS timeout, late fill, account/epoch mismatch and conflicting orders against real PostgreSQL and WSL practice evidence.

No automatic SELL authorization until independent code, native test, WSL authenticated broker evidence and approved paper order validate the transition.
