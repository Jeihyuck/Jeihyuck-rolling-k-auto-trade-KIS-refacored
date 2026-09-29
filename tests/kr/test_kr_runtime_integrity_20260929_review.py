from __future__ import annotations

from types import SimpleNamespace

from trader.kr.runtime_integrity_20260929_review import _build_daily_ccld_authority_fence


def test_continuation_final_page_cannot_replace_full_daily_snapshot():
    day = "20260929"

    def unsafe_inner(self, **_kwargs):
        # Simulate the inner Sep-29 capture seeing a terminal continuation page
        # and incorrectly caching only that page's codes.
        self._kr_20260929_daily_ccld_snapshot = {
            "captured_mono": 123.0,
            "start_date": day,
            "end_date": day,
            "codes": ["047050"],
            "authoritative_complete": True,
        }
        return {
            "rt_cd": "0",
            "output1": [{"pdno": "047050"}],
            "_response_meta": {"tr_cont": "E"},
        }

    kis = SimpleNamespace()
    wrapped = _build_daily_ccld_authority_fence(unsafe_inner)
    wrapped(
        kis,
        start_date=day,
        end_date=day,
        ctx_area_fk100="CONT-FK",
        ctx_area_nk100="CONT-NK",
    )
    assert not hasattr(kis, "_kr_20260929_daily_ccld_snapshot")


def test_root_complete_response_may_replace_snapshot():
    day = "20260929"

    def inner(self, **_kwargs):
        self._kr_20260929_daily_ccld_snapshot = {
            "captured_mono": 456.0,
            "start_date": day,
            "end_date": day,
            "codes": ["039030"],
            "authoritative_complete": True,
        }
        return {"rt_cd": "0", "output1": [{"pdno": "039030"}]}

    kis = SimpleNamespace()
    wrapped = _build_daily_ccld_authority_fence(inner)
    wrapped(kis, start_date=day, end_date=day, ctx_area_fk100="", ctx_area_nk100="")
    assert kis._kr_20260929_daily_ccld_snapshot["codes"] == ["039030"]
