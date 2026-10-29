import json
from scripts.us_order_latency_evidence_report import analyze

def test_stage4_insufficient_evidence_is_not_proven(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_text('{"stage4_ready_interval": 3.0}\n')
    report, ok = analyze(str(path), require_stage4=True)
    assert not ok
    assert report["stage4_evidence_status"] == "NOT_PROVEN"

def test_stage4_ready_intervals_and_bad_json(tmp_path):
    path = tmp_path / "b.jsonl"
    path.write_text("".join(json.dumps({"stage4_ready_interval": 3.0}) + "\n"
                            for _ in range(20)))
    report, ok = analyze(str(path), require_stage4=True)
    assert ok
    path.write_text(path.read_text() + "{bad-json\n")
    report, ok = analyze(str(path), require_stage4=True)
    assert not ok
    assert report["malformed_lines"] == 1

def test_stage4_slow_interval_is_not_proven(tmp_path):
    path = tmp_path / "c.jsonl"
    path.write_text("".join(json.dumps({"stage4_ready_interval": 35.0 if i == 19 else 3.0}) + "\n"
                            for i in range(20)))
    report, ok = analyze(str(path), require_stage4=True)
    assert not ok
    assert report["phases"]["stage4_ready_interval"]["stale_risk_count"] == 1
