from __future__ import annotations

from datetime import date

import sqlalchemy as sa
from sqlalchemy import Engine


class KrInfiniteCampaignsRepo:
    """Small isolated repo for KR infinite campaign state.

    Kept separate from the shared trading schema so the new strategy does not
    couple its lifecycle state to PB1 positions or require broad schema.py edits.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def get_active(self, *, env: str, strategy: str, code: str) -> dict | None:
        stmt = sa.text(
            """
            SELECT env, strategy, code, cycle_id, started_on, status,
                   deployed_tranches, last_fill_price, closed_on, close_reason,
                   created_at, updated_at
              FROM kr_infinite_campaigns
             WHERE env = :env
               AND strategy = :strategy
               AND code = :code
               AND status = 'ACTIVE'
             ORDER BY started_on DESC, created_at DESC
             LIMIT 1
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt, {"env": env, "strategy": strategy, "code": code}).mappings().first()
        return dict(row) if row else None

    def start_cycle(
        self,
        *,
        env: str,
        strategy: str,
        code: str,
        cycle_id: str,
        started_on: date,
    ) -> None:
        stmt = sa.text(
            """
            INSERT INTO kr_infinite_campaigns
                (env, strategy, code, cycle_id, started_on, status,
                 deployed_tranches, last_fill_price, created_at, updated_at)
            VALUES
                (:env, :strategy, :code, :cycle_id, :started_on, 'ACTIVE',
                 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (env, strategy, code, cycle_id) DO NOTHING
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "env": env,
                    "strategy": strategy,
                    "code": code,
                    "cycle_id": cycle_id,
                    "started_on": started_on,
                },
            )

    def sync_from_fills(
        self,
        *,
        env: str,
        strategy: str,
        code: str,
        cycle_id: str,
        started_on: date,
        deployed_tranches: int,
        last_fill_price: float,
    ) -> None:
        stmt = sa.text(
            """
            INSERT INTO kr_infinite_campaigns
                (env, strategy, code, cycle_id, started_on, status,
                 deployed_tranches, last_fill_price, created_at, updated_at)
            VALUES
                (:env, :strategy, :code, :cycle_id, :started_on, 'ACTIVE',
                 :deployed_tranches, :last_fill_price, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (env, strategy, code, cycle_id)
            DO UPDATE SET
                status = 'ACTIVE',
                deployed_tranches = EXCLUDED.deployed_tranches,
                last_fill_price = EXCLUDED.last_fill_price,
                updated_at = CURRENT_TIMESTAMP
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "env": env,
                    "strategy": strategy,
                    "code": code,
                    "cycle_id": cycle_id,
                    "started_on": started_on,
                    "deployed_tranches": int(max(0, deployed_tranches)),
                    "last_fill_price": float(max(0.0, last_fill_price)),
                },
            )

    def close_cycle(
        self,
        *,
        env: str,
        strategy: str,
        code: str,
        cycle_id: str,
        closed_on: date,
        reason: str,
    ) -> None:
        stmt = sa.text(
            """
            UPDATE kr_infinite_campaigns
               SET status = 'CLOSED',
                   closed_on = :closed_on,
                   close_reason = :reason,
                   updated_at = CURRENT_TIMESTAMP
             WHERE env = :env
               AND strategy = :strategy
               AND code = :code
               AND cycle_id = :cycle_id
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "env": env,
                    "strategy": strategy,
                    "code": code,
                    "cycle_id": cycle_id,
                    "closed_on": closed_on,
                    "reason": reason,
                },
            )
