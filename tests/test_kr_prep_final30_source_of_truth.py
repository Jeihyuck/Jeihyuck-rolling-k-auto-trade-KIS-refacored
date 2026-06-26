from datetime import date

from trader.kr import artifacts
from trader.contracts.final30_contract import assert_final30_contract


def _rows(as_of="2026-06-19", bad=False):
    rows = []
    for i in range(1, 31):
        rank = 0 if bad else i
        rows.append({
            "code": f"{i:06d}", "name": f"stock{i}", "as_of": as_of,
            "rank": rank, "rank_final30": rank, "final_score": float(100 - i),
            "meta": {"rank": rank, "rank_final30": rank, "code": f"{i:06d}", "name": f"stock{i}", "final_score": float(100 - i)},
        })
    return rows


def test_prep_artifact_uses_db_roundtrip_rows_only(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    as_of = date(2026, 6, 19)
    trade_date = date(2026, 6, 20)

    db_roundtrip_rows, info = assert_final30_contract(
        _rows(bad=True), as_of=as_of.isoformat(), env="practice", source="test.db_roundtrip"
    )
    artifacts.publish_kr_prep_artifacts_atomic(
        trade_date=trade_date,
        expected_as_of=as_of,
        actual_as_of=as_of,
        env="practice",
        final30_rows=db_roundtrip_rows,
        db_exact_rows=len(db_roundtrip_rows),
        contract_hash=info["contract_hash"],
    )

    payload = artifacts._load(tmp_path / "signals/kr/latest_final30_scored.json")
    artifact_rows = payload["rows"]
    artifact_ranks = [r["rank_final30"] for r in artifact_rows]
    artifact_meta_ranks = [r["meta"]["rank_final30"] for r in artifact_rows]
    assert artifact_ranks == list(range(1, 31))
    assert artifact_meta_ranks == list(range(1, 31))
