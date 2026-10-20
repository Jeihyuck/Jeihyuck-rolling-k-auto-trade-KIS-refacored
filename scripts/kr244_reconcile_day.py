"""KR244 historical KIS↔DB SELL reconciliation, explicitly invoked by an operator.

Default mode is DB read-only. --apply-verified is practice-only and may write
ONLY through the existing broker-evidence reconcile_today pipeline. It cannot
create, resubmit, cancel or modify broker orders. No fabricated fill prices.

Example (with the actual practice credentials and DB configured):
  python -m scripts.kr244_reconcile_day --env practice --trade-date 2026-10-20
  python -m scripts.kr244_reconcile_day --env practice --trade-date 2026-10-20 --apply-verified
"""
from __future__ import annotations

import argparse
from datetime import date
import json
import os
from uuid import uuid4

from trader.db.engine import get_engine
from trader.kr.unpriced_sell_audit import audit_kr_pending_sell_orders
from trader.time_utils import now_kst


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KR practice exact-order SELL reconciliation")
    parser.add_argument("--env", choices=("practice", "real"), required=True)
    parser.add_argument("--trade-date", required=True, help="KST YYYY-MM-DD")
    parser.add_argument("--apply-verified", action="store_true", help="Write only actual KIS execution evidence (practice only)")
    args = parser.parse_args(argv)
    trade_date = date.fromisoformat(args.trade_date)
    if trade_date > now_kst().date():
        parser.error("Cannot reconcile a future trading day")
    if args.apply_verified:
        if args.env != "practice":
            parser.error("--apply-verified is restricted to the practice account")
        if trade_date != date(2026, 10, 20):
            parser.error("--apply-verified is restricted to the proven Oct-20 SELL recovery")
        if os.getenv("DISABLE_REAL_TRADING", "") != "1":
            parser.error("DISABLE_REAL_TRADING=1 must explicitly prevent broker orders")
        if "openapivts.koreainvestment.com" not in str(os.getenv("KIS_REST_URL") or ""):
            parser.error("KIS endpoint must be the KIS practice endpoint")
        kis_env = str(os.getenv("KIS_ENV") or "").strip().lower()
        strategy_env = str(os.getenv("STRATEGY_ENV") or "").strip().lower()
        if kis_env != "practice" or strategy_env != "practice":
            parser.error("KIS_ENV and STRATEGY_ENV must both be explicitly practice")

    engine = get_engine()
    try:
        before = audit_kr_pending_sell_orders(engine, env=args.env, trade_date=trade_date)
        result = {"before": before, "mode": "READ_ONLY"}
        if args.apply_verified:
            from trader.kis_wrapper import KisAPI
            from trader.reconcile_kis import reconcile_today
            from trader.run_context import RunContext
            from trader.account_state import get_account_key
            from scripts.kr244_kis_readonly_probe import EXPECTED_OCT20_SELLS
            import hashlib
            expected_scope_prefix = "fef5d08b17"
            actual_scope_prefix = hashlib.sha256(
                get_account_key(env="practice").encode("utf-8")
            ).hexdigest()[:len(expected_scope_prefix)]
            if actual_scope_prefix != expected_scope_prefix:
                raise RuntimeError("KR244_DB_ACCOUNT_SCOPE_MISMATCH")
            kis = KisAPI(kis_env="practice")
            if str(getattr(kis, "KIS_ENV", getattr(kis, "env", "")) or "practice").lower() not in ("practice", ""):
                raise RuntimeError("KIS environment mismatch; refusing DB mutation")
            ctx = RunContext(
                run_id=str(uuid4()), env="practice", strategy="pb1_pullback_close",
                started_at=now_kst(), dry_run=False,
            )
            evidence = reconcile_today(
                engine=engine, kis=kis, ctx=ctx,
                trade_date=trade_date, require_authoritative=True,
                strict_sell_targets=EXPECTED_OCT20_SELLS,
            )
            after = audit_kr_pending_sell_orders(engine, env=args.env, trade_date=trade_date)
            result = {"before": before, "broker_reconciliation": evidence, "after": after,
                      "mode": "APPLY_KIS_EXECUTION_EVIDENCE_ONLY"}
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
