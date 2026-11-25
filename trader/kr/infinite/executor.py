from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any
import logging

from trader.kr_price_utils import normalize_kr_etf_order_price

from trader.kis_wrapper import extract_order_no, is_order_accepted

from .models import Action, BrokerOrderState, BrokerPosition, Decision, OrderIntent

logger = logging.getLogger(__name__)


class KISExecutor:
    """Narrow adapter over existing public domestic KIS methods."""

    def __init__(self, kis: Any, kis_env: str, balance_snapshot: dict | None = None):
        self.kis = kis
        self.kis_env = kis_env
        self.balance_snapshot = balance_snapshot

    def _balance(self) -> dict:
        raw = self.balance_snapshot if self.balance_snapshot is not None else self.kis.get_balance()
        if not isinstance(raw, dict) or raw.get("_stub"):
            raise RuntimeError("KR_INF_RECONCILE_BROKER_UNAVAILABLE")
        return raw

    @staticmethod
    def _position_row(raw: dict, symbol: str) -> dict:
        holdings = raw.get("output1") or raw.get("stocks") or []
        return next((item for item in holdings if str(item.get("pdno") or item.get("code") or "").lstrip("A") == symbol), {})

    def position(self, symbol: str) -> BrokerPosition:
        raw = self._balance()
        row = self._position_row(raw, symbol)
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        orderable = int(float(row.get("ord_psbl_qty") or row.get("orderable_qty") or qty))
        average = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        return BrokerPosition(qty, orderable, average, float(self.kis.get_current_price(symbol)))

    def account_equity(self) -> float:
        raw = self._balance()
        summary = raw.get("output2") or {}
        if isinstance(summary, list):
            summary = summary[0] if summary else {}
        for key in ("tot_evlu_amt", "tot_asst_icdc_amt", "nass_amt"):
            value = float(summary.get(key) or 0)
            if value > 0:
                return value
        raise RuntimeError("KR_INF_ACCOUNT_EQUITY_UNAVAILABLE")

    def orderable_cash(self, symbol: str, price: float) -> float:
        return float(self.kis.get_orderable_cash(symbol, price)[0])

    def verified_add_orderable_cash(self, symbol: str, price: float) -> float:
        """For an already-held Infinite BUY, require native KIS PSBL source.

        The generic KIS helper may return last known cash, cached PSBL cash
        or balance's gross cash on request failure. Such values are not
        evidence of this order's actual buying power. No new order is
        permitted from any fallback; protected SELL does not call here.
        """
        cash, proof = self.kis.get_orderable_cash(symbol, price)
        if (not isinstance(proof, dict)
                or proof.get("native_source_verified") is not True
                or proof.get("source") != "psbl_order"
                or proof.get("selected_key") not in {"ord_psbl_cash", "ord_psbl_amt"}
                or not isinstance(proof.get("raw_fields"), dict)):
            raise RuntimeError("KR_INF_NATIVE_PSBL_ORDERABLE_CASH_UNVERIFIED")
        raw = proof["raw_fields"].get(proof["selected_key"])
        if raw in (None, ""):
            raise RuntimeError("KR_INF_NATIVE_PSBL_ORDERABLE_FIELD_MISSING")
        amount = float(cash)
        if not (amount >= 0 and amount < float("inf")):
            raise RuntimeError("KR_INF_NATIVE_PSBL_ORDERABLE_CASH_INVALID")
        return amount

    def submit(self, decision: Decision, symbol: str) -> tuple[str | None, dict]:
        if decision.action in {Action.BUY, Action.RECOVERY}:
            if int(decision.qty or 0) <= 0:
                raise ValueError("KR_INF_BUY_QTY_INVALID")
            # KIS's buy_stock_limit() uses the same dedicated ETF tick.
            # Pre-normalize here with the same shared tick function so the
            # exact submitted LIMIT is observable and is reproducible from
            # the immutable decision price stored in the intent journal.
            # This does not relax entry/capital policy or change the order.
            intended_price = float(decision.notional) / int(decision.qty)
            # The tick normalizer itself rounds UP. Truncating a fractional
            # owner LIMIT first could undercut the actual owner reference:
            # 55,215.1 would incorrectly become 55,215 instead of 55,220.
            submit_price, tick_size = normalize_kr_etf_order_price(
                intended_price, side="BUY",
            )
            if submit_price <= 0:
                raise ValueError("KR_INF_SUBMIT_PRICE_INVALID")
            logger.info(
                "[KR_INF][BUY_LIMIT_PRICE] symbol=%s qty=%s decision_price=%.8f "
                "kis_submit_price=%s tick_size=%s "
                "price_source=DECISION_NOTIONAL_ETF_TICK_NORMALIZED",
                symbol, decision.qty, intended_price,
                submit_price, tick_size,
            )
            response = self.kis.buy_stock_limit(symbol, decision.qty, submit_price)
        elif decision.action in {Action.SELL_PARTIAL, Action.SELL_ALL}:
            response = self.kis.sell_stock(symbol, decision.qty)
        else:
            raise ValueError("NON_ORDER_DECISION")
        if not is_order_accepted(response, kis_env=self.kis_env):
            raise RuntimeError("KR_INF_BROKER_ORDER_REJECTED")
        order_id = extract_order_no(response)
        if not order_id and self.kis_env.lower() in {"real", "live"}:
            raise RuntimeError("KR_INF_BROKER_ORDER_ID_MISSING")
        return order_id, response

    def _balance_delta_state(self, intent: OrderIntent) -> BrokerOrderState | None:
        """Use a fresh authoritative KIS holding delta when order-detail lookup is unavailable.

        This is deliberately proof-based: without an immutable pre-order holding
        baseline we do not guess.  It is the fallback needed for incidents such
        as 2026-08-24 where SELL ACK succeeded, daily-ccld returned HTTP 500, and
        broker holdings later proved 6 -> 3 shares.
        """
        metadata = dict(intent.metadata or {})
        if metadata.get("pre_order_holding_qty") is None:
            return None
        pre_qty = int(metadata.get("pre_order_holding_qty") or 0)
        raw = self._balance()
        row = self._position_row(raw, "122630")
        post_qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        post_avg = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        if intent.side in {"BUY", "RECOVERY"}:
            delta = post_qty - pre_qty
        else:
            delta = pre_qty - post_qty
        if delta < 0 or delta > int(intent.requested_qty or 0):
            return BrokerOrderState("RECONCILE_PENDING")
        if delta == 0:
            return None
        status = "FILLED" if delta >= int(intent.requested_qty or 0) else "PARTIALLY_FILLED"
        if intent.side in {"BUY", "RECOVERY"}:
            # Holding delta can support a *quantity discrepancy* diagnostic,
            # but post-trade average cost is not an exact execution-price/
            # notional receipt for this original KIS order (other purchases,
            # fees, or corrections can affect the holdings average). Never
            # manufacture a confirmed 13-share cash amount from that delta.
            # Keep the original unresolved order fenced until KIS original
            # fill price evidence or an explicitly approved repair exists.
            logger.warning(
                "[KR_INF][BALANCE_ONLY_BUY_FILL][UNVERIFIED_PRICE] "
                "order_id=%s cycle=%s observed_delta_qty=%s "
                "action=RECONCILE_PENDING_NO_SYNTHETIC_CASH",
                intent.broker_order_id, intent.cycle_id, delta,
            )
            return BrokerOrderState("RECONCILE_PENDING")
        # SELL quantity may still be proven for risk cleanup after daily-ccld
        # failure, but no realized proceeds or fill price are fabricated.
        return BrokerOrderState(status, delta, 0.0, None)

    def _historical_unchanged_balance_expiry(
        self, intent: OrderIntent, *, lookup_date: date, trade_date: date
    ) -> BrokerOrderState | None:
        """Prove an old domestic day order is no longer a live pending fence.

        KRX cash orders are day orders.  When the original trade date is over,
        daily-ccld is temporarily unavailable, and a fresh authoritative broker
        balance still equals the immutable pre-order quantity, a zero-fill order
        cannot remain live.  This proof is deliberately unavailable without a
        broker order id/baseline and never applies to a current-day order.
        """
        if lookup_date >= trade_date:
            return None
        if not str(intent.broker_order_id or "").strip():
            return None
        if int(intent.filled_qty or 0) != 0:
            return None
        metadata = dict(intent.metadata or {})
        if metadata.get("pre_order_holding_qty") is None:
            return None
        try:
            pre_qty = int(float(metadata.get("pre_order_holding_qty")))
            requested = int(intent.requested_qty or 0)
            if pre_qty < 0 or requested <= 0:
                return None
            raw = self._balance()
            row = self._position_row(raw, "122630")
            post_qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        except Exception:
            return None
        if post_qty != pre_qty:
            return None
        return BrokerOrderState("EXPIRED", 0)

    def _original_daily_ccld_pages(self, order_date: date) -> dict | None:
        """Return bounded *complete* KIS pages for the original order day.

        Continuation calls happen only when KIS explicitly supplies a
        continuation cursor. A failed page, repeated cursor or unbounded
        history cannot become false no-order/fill proof. Protect all other
        KIS data/order traffic with a strict maximum of three pages.
        """
        query_day = order_date.strftime("%Y%m%d")
        fk = nk = ""
        cursors: set[tuple[str, str]] = set()
        rows: list[dict] = []
        for page in range(1, 4):
            try:
                raw = self.kis.inquire_daily_ccld(
                    start_date=query_day,
                    end_date=query_day,
                    **({"ctx_area_fk100": fk, "ctx_area_nk100": nk}
                       if page > 1 else {}),
                )
            except Exception:
                # First-page HTTP failure means original order history is
                # UNAVAILABLE, not an incomplete pagination cursor. Preserve
                # the established old-day unchanged-balance SELL expiry proof
                # only for this distinct condition.
                return {"_kr_inf_first_page_unavailable": True} if page == 1 else None
            if (not isinstance(raw, dict)
                    or raw.get("_diag_stub") or raw.get("_stub")
                    or raw.get("_ccld_status")
                    or str(raw.get("_fill_source") or "").lower()
                    == "daily_ccld_fail_open"
                    or raw.get("rt_cd") not in (None, "0", 0)):
                return {"_kr_inf_first_page_unavailable": True} if page == 1 else None
            chunk = raw.get("output1") or raw.get("output") or []
            if isinstance(chunk, dict):
                chunk = [chunk]
            if not isinstance(chunk, list) or any(
                not isinstance(item, dict) for item in chunk
            ):
                return None
            rows.extend(chunk)
            meta = raw.get("_response_meta") or {}
            # A missing tr_cont header means no continuation is signalled.
            # str(None) becomes "NONE" and otherwise rejects every valid
            # single-page broker response lacking _response_meta.
            cont = str(
                (meta.get("tr_cont") or "") if isinstance(meta, dict) else ""
            ).strip().upper()
            if cont not in {"", "F", "M", "D", "E"}:
                return None
            next_fk = str(raw.get("ctx_area_fk100")
                          or raw.get("CTX_AREA_FK100") or "").strip()
            next_nk = str(raw.get("ctx_area_nk100")
                          or raw.get("CTX_AREA_NK100") or "").strip()
            if cont in {"D", "E"} or (
                not cont and not next_fk and not next_nk
            ):
                return {"rt_cd": "0", "output1": rows,
                        "_kr_inf_ccld_verified_pages": page}
            if not next_fk and not next_nk:
                return None
            cursor = (next_fk, next_nk)
            if cursor in cursors:
                return None
            cursors.add(cursor)
            fk, nk = cursor
        logger.warning(
            "[KR_INF][KIS_CCLD_PAGINATION_LIMIT] day=%s pages=3 "
            "action=RECONCILE_PENDING",
            query_day,
        )
        return None

    def verify_no_same_day_symbol_order(self, symbol: str, day: date) -> bool:
        """Conservative pre-hashkey retry evidence; no empty-on-error fallback.

        Any native row for the symbol (including a cancelled or filled row)
        blocks this narrow retry path. A missing symbol in any returned row
        also prevents proving absence. No other order method is called.
        """
        if self.kis_env != "practice" or symbol != "122630":
            return False
        raw = self._original_daily_ccld_pages(day)
        if (not isinstance(raw, dict) or raw.get("rt_cd") != "0"
                or not raw.get("_kr_inf_ccld_verified_pages")
                or not isinstance(raw.get("output1"), list)):
            return False
        for item in raw["output1"]:
            code = str(item.get("pdno") or item.get("code") or "").strip().removeprefix("A")
            if len(code) != 6 or not code.isascii() or not code.isdigit() or code == symbol:
                return False
        return True

    def order_state(self, intent: OrderIntent, trade_date: date) -> BrokerOrderState:
        # Reconcile against the order's original trading day, not the current
        # session day. Domestic day orders do not migrate to a later trade date;
        # using today's date can make an old SELL stay RECONCILE_PENDING forever.
        lookup_date = intent.trade_date or trade_date
        raw = self._original_daily_ccld_pages(lookup_date)
        first_page_unavailable = bool(
            isinstance(raw, dict)
            and raw.get("_kr_inf_first_page_unavailable") is True
        )
        if raw is None or first_page_unavailable:
            # A failed first page is NOT evidence of no broker execution.
            # A partial/incomplete subsequent page is never order-expiry
            # evidence. Keep unknown BUY cash and duplicate submissions fenced.
            if intent.side in {"SELL_PARTIAL", "SELL_ALL"} and str(
                intent.broker_order_id or ""
            ).strip():
                try:
                    balance_proof = self._balance_delta_state(intent)
                except Exception:
                    balance_proof = None
                if balance_proof is not None:
                    return balance_proof
                # Existing legacy contract: an old domestic day SELL that
                # still shows exactly the immutable pre-order holding count
                # can expire *only* when the first KIS page was unavailable;
                # NEVER when any pagination page was incomplete.
                if first_page_unavailable:
                    historical_expiry = self._historical_unchanged_balance_expiry(
                        intent, lookup_date=lookup_date, trade_date=trade_date,
                    )
                    if historical_expiry is not None:
                        return historical_expiry
            return BrokerOrderState("RECONCILE_PENDING")
        rows = raw["output1"]
        if intent.broker_order_id:
            matches = [item for item in rows if str(item.get("odno") or item.get("ODNO") or item.get("ord_no") or "") == intent.broker_order_id]
        else:
            expected_side = "BUY" if intent.side in {"BUY", "RECOVERY"} else "SELL"
            def correlated(item) -> bool:
                symbol = str(item.get("pdno") or item.get("code") or item.get("symbol") or "").lstrip("A")
                side_raw = str(item.get("sll_buy_dvsn_cd") or item.get("side") or item.get("sll_buy_dvsn_name") or "").upper()
                side = "BUY" if side_raw in {"02", "BUY", "매수"} or "BUY" in side_raw else "SELL" if side_raw in {"01", "SELL", "매도"} or "SELL" in side_raw else ""
                requested = int(float(item.get("ord_qty") or item.get("requested_qty") or item.get("tot_ord_qty") or 0))
                return symbol == "122630" and side == expected_side and requested == intent.requested_qty
            matches = [item for item in rows if correlated(item)]
            if len(matches) > 1:
                raise RuntimeError("KR_INF_PENDING_ORDER_AMBIGUOUS")
        row = matches[0] if len(matches) == 1 else None
        if row is not None:
            # A broker order number alone is not owner/symbol proof. KIS
            # order numbers can be reused across dates/accounts; we query
            # the original trade date and additionally reject any explicit
            # conflicting native symbol or BUY/SELL side evidence.
            native_symbol = str(
                row.get("pdno") or row.get("code") or row.get("symbol") or ""
            ).strip().removeprefix("A")
            if native_symbol and native_symbol != "122630":
                logger.error(
                    "[KR_INF][BROKER_ORDER_IDENTITY_MISMATCH] "
                    "order=%s expected_symbol=122630 observed_symbol=%s "
                    "action=RECONCILE_PENDING",
                    intent.broker_order_id, native_symbol,
                )
                return BrokerOrderState("RECONCILE_PENDING")
            native_side_raw = str(
                row.get("sll_buy_dvsn_cd") or row.get("side")
                or row.get("sll_buy_dvsn_name") or ""
            ).strip().upper()
            sides = {"01": "SELL", "02": "BUY",
                     "SELL": "SELL", "BUY": "BUY",
                     "매도": "SELL", "매수": "BUY"}
            expected_side = "BUY" if intent.side in {"BUY", "RECOVERY"} else "SELL"
            if native_side_raw and sides.get(native_side_raw) != expected_side:
                logger.error(
                    "[KR_INF][BROKER_ORDER_SIDE_MISMATCH] "
                    "order=%s expected=%s observed=%s "
                    "action=RECONCILE_PENDING",
                    intent.broker_order_id, expected_side, native_side_raw,
                )
                return BrokerOrderState("RECONCILE_PENDING")
        if row is None:
            balance_proof = self._balance_delta_state(intent)
            if balance_proof is not None:
                return balance_proof
            historical_expiry = self._historical_unchanged_balance_expiry(
                intent, lookup_date=lookup_date, trade_date=trade_date
            )
            if historical_expiry is not None:
                return historical_expiry
            return BrokerOrderState("RECONCILE_PENDING")
        qty_raw = next(
            (row.get(key) for key in ("tot_ccld_qty", "ccld_qty", "filled_qty") if row.get(key) not in (None, "")),
            None,
        )
        try:
            native_qty = Decimal(str(qty_raw)) if qty_raw is not None else None
            qty = (
                int(native_qty)
                if native_qty is not None
                and native_qty.is_finite()
                and native_qty == native_qty.to_integral_value()
                else None
            )
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            qty = None
        # Native executed share count must be an exact nonnegative integer;
        # int(float(...)) would accept 13.5 -> 13 as confirmed execution.
        if qty is None or qty < 0 or qty > int(intent.requested_qty or 0):
            return BrokerOrderState("RECONCILE_PENDING")
        # Native KIS total executed amount is a more exact economic
        # receipt than qty * a rounded average. Neither an order LIMIT
        # nor a changed holdings average may replace missing fill cash.
        total_field = row.get("tot_ccld_amt")
        avg_field = next(
            (row[key] for key in ("avg_prvs", "avg_price", "ccld_unpr")
             if row.get(key) not in (None, "")),
            None,
        )
        total = Decimal("0")
        avg = None
        if qty > 0:
            try:
                if total_field not in (None, ""):
                    total = Decimal(str(total_field))
                    if not total.is_finite() or total <= 0:
                        raise ValueError("NONPOSITIVE_NATIVE_CCLD_AMOUNT")
                    avg = total / Decimal(qty)
                elif avg_field not in (None, ""):
                    avg = Decimal(str(avg_field))
                    if not avg.is_finite() or avg <= 0:
                        raise ValueError("NONPOSITIVE_NATIVE_CCLD_AVERAGE")
                    total = avg * Decimal(qty)
                else:
                    raise ValueError("KIS_CCLD_NO_NATIVE_CASH_PROOF")
            except (InvalidOperation, TypeError, ValueError, OverflowError):
                logger.warning(
                    "[KR_INF][KIS_FILL_CASH_UNVERIFIED] "
                    "order=%s date=%s qty=%s action=RECONCILE_PENDING",
                    intent.broker_order_id, lookup_date, qty,
                )
                return BrokerOrderState("RECONCILE_PENDING")
        if qty >= intent.requested_qty:
            status = "FILLED"
        elif qty > 0:
            # The original partial fill remains a durable pending fence.
            status = "PARTIALLY_FILLED"
        elif lookup_date < trade_date:
            status = "EXPIRED"
        else:
            status = "PENDING"
        return BrokerOrderState(status, qty, total, float(avg) if avg else None)
