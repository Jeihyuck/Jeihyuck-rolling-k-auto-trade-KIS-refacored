import logging

import pandas as pd

from trader import pb1_runner


def _rows() -> list[dict]:
    return [
        {
            "code": f"{i:06d}",
            "name": f"stock{i}",
            "as_of": "2026-06-19",
            "rank": i,
            "rank_final30": i,
            "final_score": float(100 - i),
            "meta": {"rank": i, "rank_final30": i, "code": f"{i:06d}", "name": f"stock{i}", "final_score": float(100 - i)},
        }
        for i in range(1, 31)
    ]


def test_kr_canonical_artifact_source_passes_lock_validation(caplog):
    caplog.set_level(logging.INFO)
    pb1_runner._validate_trade_locked_final30_or_raise(
        final30_df=pd.DataFrame(_rows()),
        source_name="kr_canonical_artifact",
        as_of="2026-06-19",
        env="practice",
    )
    assert "[TRADE][FINAL30][LOCK][OK] source=kr_canonical_artifact" in caplog.text


def test_db_exact_fallback_source_passes_lock_validation(caplog):
    caplog.set_level(logging.INFO)
    pb1_runner._validate_trade_locked_final30_or_raise(
        final30_df=pd.DataFrame(_rows()),
        source_name="db_exact_fallback",
        as_of="2026-06-19",
        env="practice",
    )
    assert "[TRADE][FINAL30][LOCK][OK] source=db_exact_fallback" in caplog.text
