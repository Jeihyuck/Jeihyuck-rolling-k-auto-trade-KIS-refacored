from uuid import uuid4

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.schema import schema_for_engine


def create_schema_with_active_test_epoch(engine):
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    env = "practice"
    account_id = get_account_key(env=env)
    with engine.begin() as conn:
        active_epoch = conn.execute(
            sa.select(schema.trading_epochs.c.trading_epoch_id).where(
                schema.trading_epochs.c.env == env,
                schema.trading_epochs.c.account_id == account_id,
                schema.trading_epochs.c.status == "ACTIVE",
            )
        ).first()
        if active_epoch is None:
            conn.execute(
                schema.trading_epochs.insert().values(
                    trading_epoch_id=str(uuid4()),
                    env=env,
                    account_id=account_id,
                    status="ACTIVE",
                    reason="isolated execution-claim test",
                )
            )
    return schema
