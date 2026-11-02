"""CI-only PostgreSQL 16 migration rehearsal in a dedicated scratch schema.

Never connects to a user/production database; only runs when GitHub Actions
sets a loopback PBCORE_TEST_POSTGRES_URL.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import sqlalchemy as sa

from trader.db.migrate import split_postgres_sql


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") != "true" or not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="PG fixture is restricted to a GitHub Actions local Postgres service",
)
def test_0061_pg_migration_preserves_old_row_and_allows_recycled_odno():
    url = sa.engine.make_url(os.environ["PBCORE_TEST_POSTGRES_URL"])
    if url.host not in {"localhost", "127.0.0.1", "::1"}:
        pytest.skip("Never run identity index rehearsal against a remote database")

    engine = sa.create_engine(url)
    name = "kr_order_identity_nov02_ci"
    try:
        with engine.begin() as conn:
            # A scratch schema is entirely isolated from trading data.
            conn.exec_driver_sql(f"CREATE SCHEMA {name}")
            conn.exec_driver_sql(f"""
                CREATE TABLE {name}.orders (
                    order_id TEXT PRIMARY KEY,
                    env TEXT NOT NULL,
                    broker_order_id TEXT,
                    trading_epoch_id TEXT,
                    submitted_at TIMESTAMPTZ,
                    acked_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            conn.exec_driver_sql(f"""
                CREATE UNIQUE INDEX uq_orders_env_broker_order_id
                ON {name}.orders(env, broker_order_id)
            """)
            conn.exec_driver_sql(f"""
                INSERT INTO {name}.orders
                    (order_id,env,broker_order_id,trading_epoch_id,submitted_at)
                VALUES ('aug-old','practice','0000013020',NULL,'2026-08-26 09:15+09')
            """)

            raw = Path("migrations/0061_kr_order_broker_day_epoch_identity.sql").read_text()
            script = raw.replace("public.orders", f"{name}.orders")
            script = script.replace(
                "public.uq_orders_env_broker_order_id",
                f"{name}.uq_orders_env_broker_order_id",
            )
            for statement in split_postgres_sql(script):
                conn.exec_driver_sql(statement)
            conn.exec_driver_sql(f"""
                INSERT INTO {name}.orders
                    (order_id,env,broker_order_id,trading_epoch_id,submitted_at)
                VALUES ('oct-neosem','practice','0000013020','new-epoch','2026-10-30 10:30+09')
            """)
            rows = conn.exec_driver_sql(f"""
                SELECT order_id FROM {name}.orders
                WHERE broker_order_id='0000013020'
                ORDER BY submitted_at
            """).all()
            assert [r[0] for r in rows] == ["aug-old", "oct-neosem"]

            with pytest.raises(sa.exc.IntegrityError):
                with conn.begin_nested():
                    conn.exec_driver_sql(f"""
                        INSERT INTO {name}.orders
                            (order_id,env,broker_order_id,trading_epoch_id,submitted_at)
                        VALUES ('illegal-same-day','practice','0000013020',
                                'new-epoch','2026-10-30 11:15+09')
                    """)
            # Successful rehearsal rolls back the scratch schema as a whole.
            raise _RehearsalRollback()
    except _RehearsalRollback:
        pass
    finally:
        engine.dispose()


class _RehearsalRollback(Exception):
    pass
