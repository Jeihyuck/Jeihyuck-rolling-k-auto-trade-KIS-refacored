import logging
from dataclasses import dataclass

from trader import pb1_runner


def _rows(as_of="2026-06-19"):
    return [{
        "code": f"{i:06d}", "name": f"stock{i}", "as_of": as_of,
        "rank": i, "rank_final30": i, "final_score": float(100 - i),
        "meta": {"rank": i, "rank_final30": i, "code": f"{i:06d}", "name": f"stock{i}", "final_score": float(100 - i)},
    } for i in range(1, 31)]


@dataclass
class BrokenArtifact:
    ok: bool = False
    reason: str = "FINAL30_CONTRACT_FAIL"
    final30_rows_payload: list[dict] | None = None


def test_artifact_rank_error_falls_back_to_db(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    import trader.kr.artifacts as artifacts
    import trader.db.repos as repos

    monkeypatch.setattr(artifacts, "validate_kr_prep_artifact", lambda **_: BrokenArtifact())
    monkeypatch.setattr(repos, "load_exact_final30_scored", lambda *args, **kwargs: _rows())

    df = pb1_runner._load_kr_injected_final30_df(
        trade_date="2026-06-20", expected_as_of="2026-06-19", env="practice", engine=object()
    )

    assert len(df) == 30
    assert df["rank_final30"].min() == 1
    assert df["rank_final30"].max() == 30
    assert df["rank_final30"].nunique() == 30
    assert "[KR_FINAL30][INJECT][ARTIFACT_FAIL]" in caplog.text
    assert "[KR_FINAL30][INJECT][FALLBACK_DB_OK]" in caplog.text
