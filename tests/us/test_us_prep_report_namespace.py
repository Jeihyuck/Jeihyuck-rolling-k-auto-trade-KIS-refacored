import json
from trader.us import prep_contract


def test_post_close_summary_is_namespaced_and_labeled(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(prep_contract, "us_prep_summary_json_path", lambda td: tmp_path / "dated.json")
    monkeypatch.setattr(prep_contract, "us_prep_latest_summary_json_path", lambda: tmp_path / "latest.json")
    monkeypatch.setattr(prep_contract, "us_prep_summary_md_path", lambda td: tmp_path / "dated.md")
    monkeypatch.setattr(prep_contract, "us_prep_latest_summary_md_path", lambda: tmp_path / "latest.md")
    prep_contract.save_us_prep_summary({"trade_date": "2026-07-20", "status": "OK", "report_purpose": "post_close_refresh"})
    latest = json.loads((tmp_path / "latest.json").read_text())
    assert latest["report_purpose"] == "post_close_refresh"
    assert latest["actual_am_contract"] is False
    assert (tmp_path / "reports/us_prep/by_trade_date/2026-07-20/post_close_refresh.json").exists()
