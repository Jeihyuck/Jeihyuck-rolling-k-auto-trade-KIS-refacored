#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one literal match, found {count}: {old[:120]!r}")
    write(path, text.replace(old, new, 1))


def replace_regex(path: str, pattern: str, new: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, new, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, found {count}: {pattern[:120]!r}")
    write(path, updated)


# Authoritative empty balances must close the persisted snapshot.
new_reconcile_persist = '''    logger.info(
        "[US_RECONCILE][AUTHORITATIVE] source=kis_balance positions=%d symbols=%s",
        len(positions),
        ",".join(position_symbols),
    )
    try:
        from trader.us.db.repos import save_position_snapshot
        saved = save_position_snapshot(
            positions,
            trade_date=trade_date,
            balance_fetch_status="OK",
            balance_parse_status="OK",
            authoritative_positions=True,
            preserve_previous_positions=False,
            close_source="kis_reconcile_balance",
        )
        if positions and int(saved or 0) < len(positions):
            raise RuntimeError(
                f"authoritative position persistence incomplete saved={saved} expected={len(positions)}"
            )
        logger.info(
            "[US_RECONCILE][UPSERT_POSITIONS] count=%d source=kis_balance_authoritative",
            saved,
        )
    except Exception as exc:
        logger.error("[US_RECONCILE][UPSERT_ERROR] failed to persist authoritative positions: %s", exc)
        return {
            "status": "POSITION_PERSIST_ERROR",
            "reason": "authoritative_position_persist_failed",
            "error": str(exc),
            "position_count": len(positions),
            "total_pvs": total_pvs,
            "positions": positions,
            "position_symbols": position_symbols,
            "balance_parse_status": balance_parse_status,
            "balance_fetch_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": True,
            "block_new_entry": True,
        }

    return {'''
replace_regex(
    "trader/us/execution/reconcile.py",
    r'    if positions:\n        logger\.info\(\n            "\[US_RECONCILE\]\[AUTHORITATIVE\].*?\n\n    return \{',
    new_reconcile_persist,
)

replace_once(
    "trader/us/db/repos.py",
    '''    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
''',
    '''    except Exception as exc:
        logger.error("[US_POSITIONS][SNAPSHOT][ERROR] %s", exc)
        if authoritative_positions:
            raise
    logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", count)
''',
)

# Close must finalize ACK orders after saving the final cumulative snapshots.
close_ack_block = '''        # 3. Final ACK reconciliation against the just-saved KIS snapshots.
        ack_reconcile_result: dict = {
            "status": "SKIP",
            "pending_count": 0,
            "confirmed_count": 0,
            "balance_reconcile_count": 0,
            "unresolved_count": 0,
            "failed_count": 0,
        }
        if not offline:
            try:
                from trader.us.execution.reconcile import reconcile_ack_orders_with_balance
                ack_reconcile_result = reconcile_ack_orders_with_balance(
                    provider=provider,
                    trade_date=trade_date,
                    env=env,
                )
                logger.info("[US_TRADE_CLOSE][ACK_RECONCILE] result=%s", ack_reconcile_result)
            except Exception as exc:
                ack_reconcile_result = {
                    "status": "ERROR",
                    "error": str(exc),
                    "pending_count": 0,
                    "confirmed_count": 0,
                    "balance_reconcile_count": 0,
                    "unresolved_count": 0,
                    "failed_count": 1,
                }
                logger.error("[US_TRADE_CLOSE][ACK_RECONCILE_ERROR] %s", exc)

        # 4. Reconcile'''
replace_once(
    "trader/us/runner/trade_close_runner.py",
    "        # 3. Reconcile",
    close_ack_block,
)
replace_once(
    "trader/us/runner/trade_close_runner.py",
    '''        reconcile_status = str(reconcile_result.get("status") or "UNKNOWN").upper()
        reconcile_error_statuses = {
''',
    '''        reconcile_status = str(reconcile_result.get("status") or "UNKNOWN").upper()
        ack_reconcile_status = str(ack_reconcile_result.get("status") or "UNKNOWN").upper()
        ack_reconcile_failed = bool(
            ack_reconcile_status not in {"OK", "SKIP"}
            or int(ack_reconcile_result.get("failed_count") or 0) > 0
        )
        ack_unresolved_count = int(ack_reconcile_result.get("unresolved_count") or 0)
        pending_count = max(pending_count, ack_unresolved_count)
        reconcile_error_statuses = {
''',
)
replace_once(
    "trader/us/runner/trade_close_runner.py",
    '''            or reconcile_status in reconcile_error_statuses
        ):
''',
    '''            or reconcile_status in reconcile_error_statuses
            or ack_reconcile_failed
        ):
''',
)
replace_once(
    "trader/us/runner/trade_close_runner.py",
    '''        elif reconcile_status in reconcile_error_statuses:
            status = "ERROR"
        elif position_snapshot_error:
''',
    '''        elif reconcile_status in reconcile_error_statuses:
            status = "ERROR"
        elif ack_reconcile_failed:
            status = "ERROR"
        elif position_snapshot_error:
''',
)
replace_once(
    "trader/us/runner/trade_close_runner.py",
    '''            "reconcile_status": reconcile_status,
            "balance": balance,
''',
    '''            "reconcile_status": reconcile_status,
            "ack_reconcile_status": ack_reconcile_status,
            "ack_reconcile_result": ack_reconcile_result,
            "balance": balance,
''',
)
