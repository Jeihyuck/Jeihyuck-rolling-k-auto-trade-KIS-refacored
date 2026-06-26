import json
from datetime import date

from trader.kr import artifacts


def _bad_rows(as_of="2026-06-19"):
    return [{
        "code": f"{i:06d}", "name": f"stock{i}", "as_of": as_of,
        "rank": 0, "rank_final30": 0, "final_score": float(100 - i),
        "meta": {"rank": 0, "rank_final30": 0, "code": f"{i:06d}", "name": f"stock{i}", "final_score": float(100 - i)},
    } for i in range(1, 31)]


def test_artifact_rank_zero_is_validate_fail_not_validate_ok(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts, "ROOT", tmp_path)
    trade_date = date(2026, 6, 20)
    as_of = date(2026, 6, 19)
    runtime = tmp_path / "runtime/kr/watchlist" / trade_date.isoformat()
    latest = tmp_path / "signals/kr"
    runtime.mkdir(parents=True)
    latest.mkdir(parents=True)
    contract = {
        "schema_version": "kr_prep_contract_v1", "market": "KR", "env": "practice",
        "trade_date": trade_date.isoformat(), "expected_as_of": as_of.isoformat(), "actual_as_of": as_of.isoformat(),
        "rows": 30, "final30_rows": 30, "artifact_rows": 30, "db_exact_rows": 30,
        "contract_ok": True, "trade_can_proceed": 1,
    }
    payload = {"env": "practice", "trade_date": trade_date.isoformat(), "expected_as_of": as_of.isoformat(), "as_of": as_of.isoformat(), "rows": _bad_rows()}
    for p in (runtime / "prep_contract.json", latest / "latest_prep_contract.json"):
        p.write_text(json.dumps(contract), encoding="utf-8")
    for p in (runtime / "final30_scored.json", latest / "latest_final30_scored.json"):
        p.write_text(json.dumps(payload), encoding="utf-8")

    result = artifacts.validate_kr_prep_artifact(trade_date=trade_date, expected_as_of=as_of, env="practice")
    assert result.ok is False
    assert "FINAL30_CONTRACT_FAIL" in (result.reason or result.detail or str(result.details))
