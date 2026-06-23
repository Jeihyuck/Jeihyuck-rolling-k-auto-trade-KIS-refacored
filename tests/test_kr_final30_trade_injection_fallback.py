import logging
from dataclasses import dataclass
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


@dataclass
class BrokenArtifact:
    ok: bool = False
    reason: str = "FINAL30_CONTRACT_FAIL"
    final30_rows_payload: list[dict] | None = None


def test_kr_final30_artifact_rank_zero_falls_back_to_db(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    import trader.kr.artifacts as artifacts
    import trader.db.repos as repos

    def fake_validate(**kwargs):
        return BrokenArtifact()

    monkeypatch.setattr(artifacts, "validate_kr_prep_artifact", fake_validate)
    monkeypatch.setattr(repos, "load_exact_final30_scored", lambda *args, **kwargs: _rows())

    df = pb1_runner._load_kr_injected_final30_df(
        trade_date="2026-06-20",
        expected_as_of="2026-06-19",
        env="practice",
        engine=object(),
    )

    assert len(df) == 30
    assert df["rank_final30"].min() == 1
    assert df["rank_final30"].max() == 30
    assert df["rank_final30"].nunique() == 30
    assert df.attrs["final30_context"]["source"] == "db_exact_fallback"
    logs = caplog.text
    assert "[KR_FINAL30][INJECT][ARTIFACT_FAIL]" in logs
    assert "[KR_FINAL30][INJECT][FALLBACK_DB_OK]" in logs
    assert "[KR_FINAL30][CONTEXT] source=db_exact_fallback rows=30" in logs
