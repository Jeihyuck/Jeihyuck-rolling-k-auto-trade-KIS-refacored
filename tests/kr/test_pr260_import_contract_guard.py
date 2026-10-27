"""Regression guards for PR260: frozen BUY contracts vs KIS holdings import.

These tests document two distinct identities: an imported balance cycle must
never stand in for an exact broker-proven BUY cycle; missing provenance remains
fail closed, not a generic policy fallback.
"""
import inspect

from trader.db.repos import PositionsRepo, _plan_position_values
from trader.reconcile_kis import _restore_entry_meta_for_promoted_positions


def test_imported_holding_guard_requires_linked_buy_cycle_and_epoch():
    source = inspect.getsource(PositionsRepo.get_or_create_imported_cycle_for_kis_holding)
    assert 'KR_IMPORT_BLOCKED_LINKED_BUY_CONTRACT' in source
    assert 'self._schema.orders.c.trading_epoch_id == trading_epoch_id' in source
    assert 'self._schema.orders.c.portfolio_epoch_id == epoch_id' in source
    assert 'self._schema.orders.c.position_cycle_id.is_not(None)' in source
    assert source.index('KR_IMPORT_BLOCKED_LINKED_BUY_CONTRACT') < source.index(
        'SUPERSEDED_BY_IMPORTED_CURRENT_CYCLE'
    )


def test_reconciler_never_symbol_backfills_imported_cycle():
    source = inspect.getsource(_restore_entry_meta_for_promoted_positions)
    assert 'if position_origin in {"IMPORTED", "RECOVERY"}:' in source
    assert 'VERIFIED_IMPORT_PRESERVE' in source
    assert 'continue' in source.split(
        'if position_origin in {"IMPORTED", "RECOVERY"}:', 1
    )[1].split('# A SYSTEM position', 1)[0]


def test_missing_contract_keeps_safe_sentinel():
    record = _plan_position_values(None, missing=True)
    assert record["exit_policy_family"] == "POLICY_MISSING"
    assert record["force_eod_close"] is False
