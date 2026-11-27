from datetime import datetime
import json

from trader.kr import entry_priority_queue as pq
from trader.kr.opening_bar_contract import KST


def test_prep_seed_is_independent_and_never_order_authority(tmp_path, monkeypatch):
    monkeypatch.setattr(pq, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pq, "_current_sha", lambda: "a" * 40)
    monkeypatch.setenv("KR_PREP_COLLECTION_PRIORITY_ENABLED", "1")
    date, scope = "2026-11-27", "practice:12345678:01"
    path = pq.final30_path(date)
    path.parent.mkdir(parents=True)
    rows = [{"code": f"{i:06d}", "rank_final30": i, "entry_style_selected": "PULLBACK",
             "pullback_score": 100-i} for i in range(1,31)]
    rows[5].update(entry_style_selected="MOMENTUM", momentum_score=98)
    rows[7].update(entry_style_selected="MOMENTUM", momentum_score=97)
    path.write_text(json.dumps({"rows": rows, "trade_date": date, "market": "KR",
                               "env": "practice", "as_of": "2026-11-26"}))
    path.with_name("prep_contract.json").write_text(json.dumps({
        "trade_date": date, "market": "KR", "env": "practice", "contract_ok": True,
        "trade_can_proceed": True, "final30_rows": 30, "actual_as_of": "2026-11-26",
        "expected_as_of": "2026-11-26", "contract_hash": "test"}))
    before = datetime(2026,11,27,6,47,tzinfo=KST)
    open_at = datetime(2026,11,27,9,0,tzinfo=KST)
    assert pq.publish_prep_collection(date=date, account_scope=scope, now=before)["state"] == "PREP_COLLECTION_PUBLISHED"
    assert pq.collection_priorities(date=date, account_scope=scope, now=open_at)["hot"] == []
    observed = pq.observer_collection_priorities(date=date, account_scope=scope, now=open_at)
    assert observed["state"] == "PREP_COLLECTION_ACTIVE"
    assert {"000006", "000008"} <= set(observed["hot"])
    assert observed["is_order_authorization"] is False
    assert len(observed["hot"]) == len(observed["warm"]) == 5
    assert pq.observer_collection_priorities(date=date, account_scope="practice:87654321:01", now=open_at)["hot"] == []
    pq.publish_pb1_priority(date=date, account_scope=scope, eligible_codes=["000030"], now=open_at)
    assert pq.observer_collection_priorities(date=date, account_scope=scope, now=open_at)["hot"] == ["000030"]
    pq.publish_pb1_priority(date=date, account_scope=scope, eligible_codes=[], now=open_at)
    assert pq.collection_priorities(date=date, account_scope=scope, now=open_at)["hot"] == []
    assert pq.observer_collection_priorities(date=date, account_scope=scope, now=open_at)["state"] == "PREP_COLLECTION_ACTIVE"
    monkeypatch.setenv("KR_PREP_COLLECTION_PRIORITY_ENABLED", "0")
    assert pq.observer_collection_priorities(date=date, account_scope=scope, now=open_at)["hot"] == []
    monkeypatch.setenv("KR_PREP_COLLECTION_PRIORITY_ENABLED", "1")
    path.write_text(path.read_text() + " ")
    assert pq.load_prep_collection(date=date, account_scope=scope, now=open_at) is None


def test_rank_does_not_fill_missing_styles_or_include_infinite():
    rows = [{"code": "122630", "entry_style_selected": "MOMENTUM", "momentum_score": 99},
            {"code": "000001", "entry_style_selected": "MOMENTUM"},
            {"code": "000002", "entry_style_selected": "PULLBACK", "pullback_score": 80}]
    assert pq._prep_collection_rank(rows) == ["000002"]
