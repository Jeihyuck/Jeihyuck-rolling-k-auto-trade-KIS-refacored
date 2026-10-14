"""KIS official daily master-file regression for KR PREP 150+150.

No KRX HTTP, brokerage orders or production DB writes. Generated synthetic
CP949 records follow the official KIS 228/222-character tail layout.
"""
from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from zipfile import ZipFile

import pytest
from zoneinfo import ZoneInfo

from trader.universe.providers import kis_master_marketcap as master

KST = ZoneInfo("Asia/Seoul")


def sample_mst(market="KOSPI", *, count=180, duplicate=False):
    file_name, tail_len = master.LAYOUT[market]
    records = []
    for i in range(count):
        code = f"{100000 + i:06d}"
        if duplicate and i == count-1:
            code = f"{100000:06d}"
        prefix = (f"A{code}".ljust(9) + f"KR{i:010d}"[:12].ljust(12)).encode("ascii")
        name = f"가상기업{i}".encode("cp949")
        tail = list(" " * tail_len)
        # A non-ASCII tail character makes byte offsets differ from KIS's
        # reference *decoded character* widths. Reproduces live row 154.
        if i == 153:
            tail[40] = "가"
        cap = 100000000 - i * 100
        tail[-15:-6] = list(f"{cap:09d}")
        records.append(prefix + name + "".join(tail).encode("cp949") + b"\n")
    return file_name, b"".join(records)


def sample_zip(market):
    file_name, content = sample_mst(market)
    buffer = BytesIO()
    with ZipFile(buffer, "w") as z:
        z.writestr(file_name, content)
    return buffer.getvalue()


class FakeResponse:
    headers = {"Last-Modified": "Wed, 14 Oct 2026 00:01:00 GMT"}
    def __init__(self, content):
        self.content = content
    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, data):
        self.data = data
    def get(self, url, **kwargs):
        market = "KOSPI" if "kospi_code" in url else "KOSDAQ"
        assert kwargs["timeout"] == (5, 25)
        return FakeResponse(self.data[market])


def test_official_byte_layout_parses_cp949_and_orders_capitalisation():
    for venue in master.LAYOUT:
        _name, raw = sample_mst(venue)
        rows = master.parse_master(raw, venue)
        assert len(rows) == 180
        assert rows[0]["code"] == "100000"
        assert rows[0]["name"] == "가상기업0"
        assert rows[0]["market_cap"] == 100000000
        assert rows[-1]["market_cap"] == 100000000 - 179 * 100
        assert rows[0]["market_cap"] > rows[-1]["market_cap"]
        assert rows[-1]["code"] == "100179"


def test_official_kis_tail_width_excludes_newline():
    # KIS sample slices row[-228:] (KOSPI) / row[-222:] (KOSDAQ)
    # including LF; its official fixed-width field specs total 227/221.
    assert master.LAYOUT["KOSPI"] == ("kospi_code.mst", 227)
    assert master.LAYOUT["KOSDAQ"] == ("kosdaq_code.mst", 221)


def test_live_shape_readonly_150_each_and_provenance():
    data = {m: sample_zip(m) for m in master.LAYOUT}
    provider = master.KISMasterMarketcapProvider(session=FakeSession(data))
    for market in master.LAYOUT:
        rows = provider.get_marketcap_top_with_meta(market, "2026-10-13", 150,
                                                    readonly_probe=True)
        assert len(rows) == 150
        assert all(x["market"] == market for x in rows)
        assert all(x["actual_as_of"] is None for x in rows)
        assert all(x["source_published_on"] == "2026-10-14" for x in rows)
        assert len({r["code"] for r in rows}) == 150
        assert len(rows[0]["source_sha256"]) == 64


def test_duplicate_stock_code_fails_closed():
    _name, raw = sample_mst(duplicate=True)
    with pytest.raises(RuntimeError, match="duplicate_code"):
        master.parse_master(raw, "KOSPI")


def test_empty_and_short_or_malformed_master_fail_closed():
    with pytest.raises(RuntimeError, match="invalid_uncompressed_size"):
        master.parse_master(b"", "KOSPI")
    with pytest.raises(RuntimeError, match="truncated"):
        master.parse_master(b"123", "KOSDAQ")
    _name, raw = sample_mst(count=30)
    with pytest.raises(RuntimeError, match="insufficient_valid"):
        master.parse_master(raw, "KOSDAQ")


def test_prep_morning_requires_exact_previous_session_and_fresh_master(monkeypatch):
    data = {m: sample_zip(m) for m in master.LAYOUT}
    p = master.KISMasterMarketcapProvider(session=FakeSession(data))
    monkeypatch.setattr(master, "now_kst", lambda: datetime(2026,10,14,6,30,tzinfo=KST))
    monkeypatch.setattr(master, "resolve_prev_krx_trading_day", lambda day: date(2026,10,13))
    assert len(p.get_marketcap_top_with_meta("KOSPI","2026-10-13",150)) == 150
    with pytest.raises(RuntimeError, match="asof_mismatch"):
        p.get_marketcap_top_with_meta("KOSPI","2026-10-12",150)
    monkeypatch.setattr(master, "now_kst", lambda: datetime(2026,10,14,15,30,tzinfo=KST))
    with pytest.raises(RuntimeError, match="morning_window"):
        p.get_marketcap_top_with_meta("KOSPI","2026-10-13",150)


def test_stale_last_modified_fails_closed(monkeypatch):
    class StaleSession(FakeSession):
        def get(self, url, **kwargs):
            out = super().get(url, **kwargs)
            out.headers = {"Last-Modified": "Mon, 12 Oct 2026 00:01:00 GMT"}
            return out
    p = master.KISMasterMarketcapProvider(session=StaleSession({
        m: sample_zip(m) for m in master.LAYOUT}))
    monkeypatch.setattr(master, "now_kst", lambda: datetime(2026,10,14,6,30,tzinfo=KST))
    monkeypatch.setattr(master, "resolve_prev_krx_trading_day", lambda d: date(2026,10,13))
    with pytest.raises(RuntimeError, match="stale_publication"):
        p.get_marketcap_top_with_meta("KOSDAQ","2026-10-13",150)


def test_missing_last_modified_and_zip_entry_fail_closed():
    class MissingHeaderSession(FakeSession):
        def get(self, url, **kwargs):
            out = super().get(url, **kwargs)
            out.headers = {}
            return out
    data = {m: sample_zip(m) for m in master.LAYOUT}
    p = master.KISMasterMarketcapProvider(session=MissingHeaderSession(data))
    with pytest.raises(RuntimeError, match="last_modified_missing"):
        p.get_marketcap_top_with_meta("KOSPI","2026-10-13",150,readonly_probe=True)
