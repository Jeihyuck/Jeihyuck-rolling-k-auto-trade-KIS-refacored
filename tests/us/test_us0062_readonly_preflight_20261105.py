"""Read-only 0062 migration evidence must never infer a stamp from indexes."""
from scripts.us0062_readonly_preflight import (
    VERSION, REQUIRED_INDEXES, SCOPE_FIELDS, assess_us0062_readiness,
)


def evidence(*, stamped=True, missing_index=False, missing_column=False, constraint=True):
    indexes = set(REQUIRED_INDEXES)
    if missing_index:
        indexes.remove("uq_us_orders_broker_account_epoch_day_0062")
    cols = {table: set(fields) for table, fields in SCOPE_FIELDS.items()}
    if missing_column:
        cols["us_orders"].remove("position_cycle_id")
    return assess_us0062_readiness(
        applied_versions={VERSION} if stamped else {"0061_kr_order_broker_day_epoch_identity.sql"},
        indexes=indexes, columns=cols,
        constraint_rows=([{
            "conname": "ck_us_order_scope_complete_0062",
            "convalidated": False,
            "definition": "CHECK ((portfolio_epoch_id IS NULL) OR (...)) NOT VALID",
        }] if constraint else []),
    )


def test_all_objects_without_schema_migrations_0062_is_not_ready():
    result = evidence(stamped=False)
    assert result["status"] == "MIGRATION_STAMP_MISSING"
    assert result["missing_indexes"] == []
    assert result["missing_columns"] == []
    assert result["orders_submitted"] == result["claims_released"] == 0


def test_missing_scoped_index_or_column_blocks_even_with_stamped_history():
    for result in (evidence(missing_index=True), evidence(missing_column=True),
                   evidence(constraint=False)):
        assert result["status"] == "SCHEMA_INCOMPLETE_OR_DRIFTED"


def test_unvalidated_not_valid_constraint_is_not_implied_invalid():
    result = evidence()
    assert result["status"] == "PROVENANCE_AND_CATALOG_PRESENT"
    assert result["scope_constraint_validated"] is False
    assert result["auto_repair_performed"] is False
