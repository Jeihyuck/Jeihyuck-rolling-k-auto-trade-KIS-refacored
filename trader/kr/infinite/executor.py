from __future__ import annotations

from datetime import date
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
            before_native_round = int(intended_price)
            submit_price, tick_size = normalize_kr_etf_order_price(
                before_native_round, side="BUY",
            )
            if submit_price <= 0:
                raise ValueError("KR_INF_SUBMIT_PRICE_INVALID")
            logger.info(
                "[KR_INF][BUY_LIMIT_PRICE] symbol=%s qty=%s decision_price=%.8f "
                "kro_priced_input=%s kis_submit_price=%s tick_size=%s "
                "price_source=DECISION_NOTIONAL_ETF_TICK_NORMALIZED",
                symbol, decision.qty, intended_price,
                before_native_round, submit_price, tick_size,
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

    def order_state(self, intent: OrderIntent, trade_date: date) -> BrokerOrderState:
        # Reconcile against the order's original trading day, not the current
        # session day. Domestic day orders do not migrate to a later trade date;
        # using today's date can make an old SELL stay RECONCILE_PENDING forever.
        lookup_date = intent.trade_date or trade_date
        try:
            raw = self.kis.inquire_daily_ccld(
                start_date=lookup_date.strftime("%Y%m%d"),
                end_date=lookup_date.strftime("%Y%m%d"),
            )
        except Exception:
            raw = None
        rows = (raw.get("output1") or raw.get("output") or []) if isinstance(raw, dict) else []
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
            qty = int(float(qty_raw)) if qty_raw is not None else None
        except (TypeError, ValueError):
            qty = None
        avg = float(row.get("avg_prvs") or row.get("avg_price") or row.get("ccld_unpr") or 0)
        if qty is None:
            return BrokerOrderState("RECONCILE_PENDING")
        if qty >= intent.requested_qty:
            status = "FILLED"
        elif qty > 0:
            # A historical partial fill needs explicit lifecycle review; do not
            # silently advance TP stage or create a replacement SELL.
            status = "PARTIALLY_FILLED"
        elif lookup_date < trade_date:
            # A zero-fill domestic day order cannot remain live overnight.
            status = "EXPIRED"
        else:
            status = "PENDING"
        return BrokerOrderState(status, qty, qty * avg, avg or None)
