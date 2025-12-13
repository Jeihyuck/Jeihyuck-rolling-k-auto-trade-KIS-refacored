from __future__ import annotations

from typing import List, Dict

from rolling_k_auto_trade_api.best_k_meta_strategy import get_kospi_top_n


def kospi_universe(top_n: int) -> List[Dict[str, str]]:
    df = get_kospi_top_n(n=top_n)
    return [{"code": str(row.Code).zfill(6), "name": row.Name} for row in df.itertuples(index=False)]
