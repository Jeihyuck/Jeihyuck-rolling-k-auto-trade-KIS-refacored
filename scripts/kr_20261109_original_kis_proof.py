"""2026-11-09 KR original KIS trade proof for Kakao/Mico/KR_INFINITE.

Read-only: this script NEVER submits/cancels a broker order or changes DB data.
Independent KIS original-day, all-page records are necessary before recovering
a lost fill or retiring an imported PB1 mirror. A local SHA-256 entry echo and
MTS holdings alone are not an original execution-price proof.

Example, from the deployed practice-account WSL environment:
  FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1 KIS_ENV=practice STRATEGY_ENV=practice \
  python -m scripts.kr_20261109_original_kis_proof

Only after reviewing this report should an authorized operator use existing
`scripts.kr_infinite_certify_frozen_exit` to certify the 122630 final exit.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import os
from typing import Any, Mapping


@dataclass(frozen=True)
class Target:
    code: str
    owner: str
    trade_date: date
    side: str
    qty: int
    odno: str

    @property
    def key(self) -> str:
        return f"{self.code}:{self.trade_date:%Y%m%d}:{self.side}:{self.odno}"


TARGETS = (
    Target("293490", "PB1", date(2026, 10, 27), "BUY", 174, "0000011280"),
    Target("059090", "PB1", date(2026, 10, 29), "BUY", 79, "0000011865"),
    Target("122630", "KR_INFINITE_V1", date(2026, 10, 26), "BUY", 11, "0000011438"),
    Target("122630", "KR_INFINITE_V1", date(2026, 10, 27), "SELL", 5, "0000008799"),
    Target("122630", "KR_INFINITE_V1", date(2026, 10, 27), "SELL", 6, "0000017474"),
)


def _safe_qty(value: Any) -> int | None:
    try:
        from decimal import Decimal
        d = Decimal(str(value))
        return int(d) if d.is_finite() and d >= 0 and d == d.to_integral_value() else None
    except (ValueError, TypeError, ArithmeticError):
        return None


def _original_order_day(row: Mapping[str, Any]) -> date | None:
    """Resolve the broker submission's calendar day without guessing timezone."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    # submitted_at is the actual original order submission boundary. Prefer
    # that over a later DB reconciliation timestamp or an imported position.
    stamp = row.get("submitted_at") or row.get("acked_at") or row.get("created_at")
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(stamp, datetime) or stamp.tzinfo is None:
        return None
    return stamp.astimezone(ZoneInfo("Asia/Seoul")).date()


def _match_db_source(
    target: Target, *, scoped_orders: list[Mapping[str, Any]],
    infinite_intents: list[Mapping[str, Any]], active_epoch: str,
    active_account: str,
) -> tuple[bool, str]:
    if target.owner == "PB1":
        candidates = [
            row for row in scoped_orders
            if str(row.get("trading_epoch_id") or "") == active_epoch
            and str(row.get("account_id") or "") == active_account
            and str(row.get("strategy") or "") == "pb1_pullback_close"
            and _original_order_day(row) == target.trade_date
            and str(row.get("code") or "").zfill(6) == target.code
            and str(row.get("side") or "").upper() == target.side
            and str(row.get("kis_odno") or "").zfill(10) == target.odno
            and _safe_qty(row.get("qty")) == target.qty
            and str(row.get("position_cycle_id") or "")
            and str(row.get("portfolio_epoch_id") or "")
        ]
        if len(candidates) != 1:
            return False, "ORIGINAL_PB1_ACCOUNT_ORDER_OR_CYCLE_NOT_UNIQUE"
        return True, "PB1_ACCOUNT_EPOCH_ORDER_CYCLE_MATCH"
    candidates = [
        row for row in infinite_intents
        if str(row.get("trading_epoch_id") or "") == active_epoch
        and str(row.get("strategy_id") or "") == "KR_INFINITE_V1"
        and str(row.get("symbol") or "") == target.code
        and str(row.get("broker_order_id") or "").zfill(10) == target.odno
        and _safe_qty(row.get("requested_qty")) == target.qty
        and (
            (target.side == "BUY" and str(row.get("side") or "") == "BUY")
            or (target.side == "SELL" and str(row.get("side") or "") in {"SELL_ALL", "SELL_PARTIAL"})
        )
        and str(row.get("trade_date") or "")[:10] == target.trade_date.isoformat()
        and str(row.get("cycle_id") or "")
    ]
    if len(candidates) != 1:
        return False, "KR_INFINITE_ORIGINAL_INTENT_OR_CYCLE_NOT_UNIQUE"
    return True, "KR_INFINITE_ORIGINAL_INTENT_MATCH"


def review_original_kr_trades(
    *, pages: Mapping[date, Mapping[str, Any]], scoped_orders: list[Mapping[str, Any]],
    infinite_intents: list[Mapping[str, Any]], active_epoch: str,
    active_account: str, broker_balance: Mapping[str, Any] | None,
    balance_source: str,
    targets: tuple[Target, ...] = TARGETS,
) -> dict[str, Any]:
    """Return a *read-only* proof report; no automatic DB repair authority."""
    from scripts.kr257_historical_kis_evidence_probe import prove_exact_cumulative_economics

    results = []
    valid_balance = bool(
        balance_source == "api" and isinstance(broker_balance, Mapping)
        and str(broker_balance.get("rt_cd") or "") == "0"
        and isinstance(broker_balance.get("output1"), list)
    )
    quantities: dict[str, int] = {}
    if valid_balance:
        for row in broker_balance["output1"]:
            if not isinstance(row, Mapping):
                valid_balance = False
                break
            code = str(row.get("pdno") or row.get("code") or "").removeprefix("A").zfill(6)
            qty = _safe_qty(row.get("hldg_qty", row.get("qty", 0)))
            if qty is None:
                valid_balance = False
                break
            quantities[code] = quantities.get(code, 0) + qty
    for target in targets:
        identity_ok, identity_reason = _match_db_source(
            target, scoped_orders=scoped_orders, infinite_intents=infinite_intents,
            active_epoch=active_epoch, active_account=active_account,
        )
        page = pages.get(target.trade_date)
        page_ok = bool(
            isinstance(page, Mapping) and page.get("_sep29_complete") is True
            and str(page.get("rt_cd") or "") == "0"
            and isinstance(page.get("output1"), list)
        )
        if not identity_ok:
            proof = {"verified": False, "reason": identity_reason}
        elif not page_ok:
            proof = {"verified": False, "reason": "ORIGINAL_DAY_ALL_PAGES_UNVERIFIED"}
        else:
            proof = prove_exact_cumulative_economics(
                page["output1"], trade_date=target.trade_date,
                code=target.code, side=target.side, order_no=target.odno,
                requested_qty=target.qty,
            )
        results.append({
            "key": target.key, "owner": target.owner, "code": target.code,
            "trade_date": target.trade_date.isoformat(),
            "side": target.side, "requested_qty": target.qty,
            "broker_order_suffix": target.odno[-4:],
            "db_order_scope_proven": identity_ok,
            "db_source_status": identity_reason,
            "original_day_complete": page_ok,
            **proof,
        })

    complete = bool(results) and all(row.get("verified") is True for row in results)
    return {
        "status": (
            "KIS_ORIGINAL_TRADES_PROVEN_READONLY" if complete
            else "RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF"
        ),
        "trade_proofs_verified": sum(bool(row.get("verified")) for row in results),
        "trade_proofs_expected": len(results),
        "broker_balance_source_proven": valid_balance,
        "current_broker_holdings_by_code": (
            {code: quantities.get(code, 0) for code in ("293490", "059090", "122630")}
            if valid_balance else None
        ),
        "historical_economic_repair_executed": False,
        "db_rows_modified": 0,
        "broker_order_submissions": 0,
        "results": results,
        "note": (
            "KIS originals require separate account-bound cycle and state certification. "
            "A completed proof report never writes a fill or retires a mirror by itself."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="KR 2026-11-09 read-only historical KIS proofs")
    parser.add_argument("--env", default="practice", choices=["practice"])
    args = parser.parse_args(argv)
    if (
        os.getenv("FORCE_HTTP") != "1"
        or os.getenv("NO_TRADE") != "1"
        or os.getenv("DRY_RUN") != "1"
        or os.getenv("KIS_ENV") != "practice"
        or os.getenv("STRATEGY_ENV") != "practice"
    ):
        print(json.dumps({"status": "BLOCKED", "reason": "PRACTICE_READ_ONLY_FLAGS_REQUIRED"}))
        return 3
    for key, value in (
        ("LIVE_TRADING_ENABLED", "0"), ("KR_LIVE_TRADING_ENABLED", "0"),
        ("KR_ORDER_ARMED", "0"), ("DISABLE_LIVE_TRADING", "1"),
        ("DISABLE_REAL_TRADING", "1"),
    ):
        os.environ[key] = value

    from trader.db.engine import make_engine
    from trader.db.schema import schema_for_engine
    from trader.kr.recovery_scope_20261104 import resolve_kr_recovery_scope
    from trader.kis_wrapper import KisAPI
    from trader.kr.runtime_integrity_20260929 import _daily_ccld_all_pages_for_date
    import sqlalchemy as sa

    engine = make_engine()
    try:
        kis = KisAPI(kis_env="practice")
        if str(kis.env).lower() != "practice":
            raise RuntimeError("NOT_PRACTICE_KIS")
        scope = resolve_kr_recovery_scope(engine=engine, env="practice", kis=kis)
        if not scope or not scope.trading_epoch_id or not scope.account_key:
            raise RuntimeError("KR_ACCOUNT_EPOCH_UNPROVEN")
        schema = schema_for_engine(engine)
        with engine.connect() as conn:
            orders = [
                dict(row) for row in conn.execute(sa.select(
                    schema.orders.c.code, schema.orders.c.side,
                    schema.orders.c.qty, schema.orders.c.kis_odno,
                    schema.orders.c.strategy, schema.orders.c.trading_epoch_id,
                    schema.orders.c.created_at, schema.orders.c.submitted_at,
                    schema.orders.c.acked_at,
                    schema.orders.c.position_cycle_id, schema.orders.c.portfolio_epoch_id,
                    schema.portfolio_epochs.c.account_id,
                ).join(
                    schema.portfolio_epochs,
                    schema.orders.c.portfolio_epoch_id == schema.portfolio_epochs.c.portfolio_epoch_id,
                ).where(
                    schema.orders.c.env == "practice",
                    schema.orders.c.code.in_(("293490", "059090")),
                    schema.orders.c.trading_epoch_id == scope.trading_epoch_id,
                )).mappings()
            ]
            intents = [
                dict(row) for row in conn.execute(sa.text("""
                    SELECT trading_epoch_id,strategy_id,symbol,cycle_id,trade_date,
                           side,requested_qty,broker_order_id
                    FROM public.kr_infinite_order_intents
                    WHERE trading_epoch_id=:epoch AND symbol='122630'
                """), {"epoch": scope.trading_epoch_id}).mappings()
            ]
        balance, source = kis.get_balance_cached(force=True, return_source=True)
        pages = {}
        for day in sorted({target.trade_date for target in TARGETS}):
            try:
                pages[day] = _daily_ccld_all_pages_for_date(kis, day)
            except Exception:
                # An unavailable original day must be reported as UNRESOLVED.
                pages[day] = {"rt_cd": "UNVERIFIED", "_sep29_complete": False}
        report = review_original_kr_trades(
            pages=pages, scoped_orders=orders, infinite_intents=intents,
            active_epoch=str(scope.trading_epoch_id),
            active_account=str(scope.account_key),
            broker_balance=balance, balance_source=source,
        )
        # Do not print account identifiers, original broker rows or credentials.
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0 if report["trade_proofs_verified"] == report["trade_proofs_expected"] and report["broker_balance_source_proven"] else 2
    except Exception as exc:
        print(json.dumps({"status": "BLOCKED", "reason": type(exc).__name__}))
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
