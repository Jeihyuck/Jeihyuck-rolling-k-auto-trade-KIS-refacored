from __future__ import annotations

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import Engine, and_, desc, select

from trader.time_coerce import to_date
from .schema import schema_for_engine


def load_ohlcv_df(engine: Engine, env: str, symbol: str, end_date: str, lookback: int) -> pd.DataFrame:
    """Return OHLCV df sorted asc. Raise/return empty if insufficient."""
    _ = env
    code = str(symbol or "").zfill(6)
    end = to_date(end_date)
    limit_n = max(1, int(lookback))
    schema = schema_for_engine(engine)

    stmt = (
        select(
            schema.price_daily.c.date,
            schema.price_daily.c.open,
            schema.price_daily.c.high,
            schema.price_daily.c.low,
            schema.price_daily.c.close,
            schema.price_daily.c.volume,
            schema.price_daily.c.value,
        )
        .where(
            and_(
                schema.price_daily.c.code == code,
                schema.price_daily.c.date <= end,
            )
        )
        .order_by(desc(schema.price_daily.c.date))
        .limit(limit_n)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "value"])

    payload = []
    for row in reversed(rows):
        payload.append(
            {
                "date": pd.to_datetime(row.date),
                "open": float(row.open) if row.open is not None else None,
                "high": float(row.high) if row.high is not None else None,
                "low": float(row.low) if row.low is not None else None,
                "close": float(row.close) if row.close is not None else None,
                "volume": float(row.volume) if row.volume is not None else None,
                "value": float(row.value) if row.value is not None else None,
            }
        )

    return pd.DataFrame(payload)
